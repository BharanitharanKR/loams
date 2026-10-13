//! `loams-connector-postgres`: the Postgres connector family (CN2 Task 10,
//! OR-6, OR-9).
//!
//! One [`PostgresFactory`] per manifest, each a
//! [`loams_connector_sdk::ConnectorFactory`]:
//!
//! | Manifest | Target profile | Source | Sink |
//! |---|---|---|---|
//! | `postgresql` | — | batch ([`batch`]) or pgoutput CDC ([`cdc`]) | upsert, exactly-once ([`sink`]) |
//! | `aurora` | `aurora-postgres` | as Postgres; IAM tokens ([`rds_iam`]) | as Postgres |
//! | `pgvector` | `pgvector` | as Postgres; `vector` ↔ `FixedSizeList<Float32>` | as Postgres |
//! | `cockroachdb` | `cockroachdb` | `SELECT` batches, sinkless changefeeds ([`cockroach`]) | as Postgres |
//!
//! The instance's `mode` (`batch`, the default, or `cdc`) picks the source;
//! a `table` in the config makes the instance a sink. The connection goes
//! through `sdk::net::connect` (egress), TLS is rustls with ring, and every
//! secret is resolved by name through the instance's `SecretResolver`.

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};

pub mod batch;
pub mod cdc;
pub mod cockroach;
pub mod config;
pub mod conn;
pub mod decode;
#[cfg(feature = "conformance")]
pub mod harness;
pub mod rds_iam;
pub mod replication;
pub mod rows;
pub mod sink;
pub mod slot;
pub mod table;
pub mod types;

pub use config::{Mode, PgConfig, Target};

/// The factory of one manifest of the family.
#[derive(Debug, Clone, Copy)]
pub struct PostgresFactory {
    target: Target,
}

impl PostgresFactory {
    pub const fn new(target: Target) -> Self {
        Self { target }
    }

    pub fn target(&self) -> Target {
        self.target
    }

    /// The instance's config: the profile's `defaults` under the user's.
    pub fn config(&self, ctx: &ConnectorContext) -> Result<PgConfig, ConnectorError> {
        let merged = config::merge_defaults(&ctx.config, ctx.profile.as_ref());
        PgConfig::parse(self.target, &ctx.instance, &merged)
    }

    /// `DeleteInstance`: drop the CDC slot and the Loams-owned publication
    /// (Postgres targets; a CockroachDB sinkless changefeed holds nothing
    /// server-side once its connection closes). The runtime calls this on
    /// delete, never on stop.
    pub async fn delete_instance(&self, ctx: &ConnectorContext) -> Result<(), ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.mode == Mode::Cdc && self.target != Target::Cockroach {
            cdc::delete_instance(&cfg, ctx).await?;
        }
        Ok(())
    }
}

impl ConnectorFactory for PostgresFactory {
    fn id(&self) -> &'static str {
        self.target.manifest_id()
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.table.is_some() && cfg.tables.is_empty() {
            return Ok(None);
        }
        let id = self.id();
        Ok(Some(match (cfg.mode, self.target) {
            (Mode::Batch, _) => (
                Box::new(batch::BatchEnumerator::new(&cfg)),
                Box::new(batch::BatchFactory::new(cfg, ctx.clone(), id)),
            ),
            (Mode::Cdc, Target::Cockroach) => (
                Box::new(cockroach::FeedEnumerator),
                Box::new(cockroach::FeedFactory::new(cfg, ctx.clone(), id)),
            ),
            (Mode::Cdc, _) => (
                Box::new(cdc::PgCdcEnumerator::new(&cfg)),
                Box::new(cdc::PgCdcFactory::new(cfg, ctx.clone(), id)),
            ),
        }))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.table.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(sink::PgSinkFactory::new(cfg, ctx.clone())?)))
    }
}

/// Every factory of the family, for the runtime's registry (Task 8 links
/// them into `loams-flow` behind `conn-postgres`).
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    [
        Target::Postgres,
        Target::Aurora,
        Target::Cockroach,
        Target::Pgvector,
    ]
    .into_iter()
    .map(|t| Box::new(PostgresFactory::new(t)) as Box<dyn ConnectorFactory>)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::{InstanceKey, Scrubber, StaticSecrets};
    use serde_json::json;
    use std::sync::Arc;

    fn ctx(config: serde_json::Value) -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "i"),
            config,
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        )
    }

    #[test]
    fn one_factory_per_manifest() {
        let ids: Vec<&str> = factories().iter().map(|f| f.id()).collect();
        assert_eq!(ids, ["postgresql", "aurora", "cockroachdb", "pgvector"]);
    }

    #[test]
    fn mode_and_table_pick_the_directions() {
        let f = PostgresFactory::new(Target::Postgres);
        let batch = ctx(json!({"host": "h", "user": "u", "tables": ["t"]}));
        assert!(f.source(&batch).expect("source").is_some());
        assert!(f.sink(&batch).expect("sink").is_none());
        let sink = ctx(json!({"host": "h", "user": "u", "table": "t"}));
        assert!(f.source(&sink).expect("source").is_none());
        assert!(f.sink(&sink).expect("sink").is_some());
        let cdc = ctx(json!({"host": "h", "user": "u", "mode": "cdc", "tables": ["t"]}));
        assert!(f.source(&cdc).expect("cdc").is_some());
        assert!(
            PostgresFactory::new(Target::Cockroach)
                .source(&cdc)
                .expect("feed")
                .is_some()
        );
        assert!(f.source(&ctx(json!({"user": "u"}))).is_err());
    }
}
