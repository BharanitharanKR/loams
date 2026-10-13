//! `loams-connector-sqlserver`: the SQL Server connector family (CN2 Task
//! 12, OR-6, OR-9).
//!
//! One [`SqlServerFactory`] per manifest, each a
//! [`loams_connector_sdk::ConnectorFactory`]:
//!
//! | Manifest | Target profile | Source | Sink |
//! |---|---|---|---|
//! | `sql-server` | — | key-range batch ([`batch`]) or change-table CDC ([`cdc`]) | bulk load + `MERGE`, exactly-once ([`sink`]) |
//! | `azure-sql` | `azure-sql` | as SQL Server; Entra ID tokens ([`entra`]) | as SQL Server |
//!
//! The instance's `mode` (`batch`, the default, or `cdc`) picks the source;
//! a `table` in the config makes the instance a sink. Every connection is
//! opened by `sdk::net::connect` (egress), TLS is TDS 8.0 `strict` over the
//! SDK's rustls with ring, and every secret is resolved by name through the
//! instance's `SecretResolver`.

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};

pub mod batch;
pub mod cdc;
pub mod config;
pub mod conn;
pub mod entra;
#[cfg(feature = "conformance")]
pub mod harness;
pub mod sink;
pub mod table;
pub mod types;

pub use config::{Mode, SqlConfig, Target};

/// The factory of one manifest of the family.
#[derive(Debug, Clone, Copy)]
pub struct SqlServerFactory {
    target: Target,
}

impl SqlServerFactory {
    pub const fn new(target: Target) -> Self {
        Self { target }
    }

    pub fn target(&self) -> Target {
        self.target
    }

    /// The instance's config: the profile's `defaults` under the user's.
    pub fn config(&self, ctx: &ConnectorContext) -> Result<SqlConfig, ConnectorError> {
        let merged = config::merge_defaults(&ctx.config, ctx.profile.as_ref());
        SqlConfig::parse(self.target, &ctx.instance, &merged)
    }
}

impl ConnectorFactory for SqlServerFactory {
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
        Ok(Some(match cfg.mode {
            Mode::Batch => (
                Box::new(batch::BatchEnumerator::new(&cfg, ctx)),
                Box::new(batch::BatchFactory::new(cfg, ctx.clone(), id)),
            ),
            Mode::Cdc => (
                Box::new(cdc::CdcEnumerator::new(&cfg)),
                Box::new(cdc::CdcFactory::new(cfg, ctx.clone(), id)),
            ),
        }))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.table.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(sink::MergeSinkFactory::new(
            cfg,
            ctx.clone(),
        )?)))
    }
}

/// Every factory of the family, for the runtime's registry (Task 8 links
/// them into `loams-flow` behind `conn-sqlserver`).
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    [Target::SqlServer, Target::AzureSql]
        .into_iter()
        .map(|t| Box::new(SqlServerFactory::new(t)) as Box<dyn ConnectorFactory>)
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
        assert_eq!(ids, ["sql-server", "azure-sql"]);
    }

    #[test]
    fn mode_and_table_pick_the_directions() {
        let f = SqlServerFactory::new(Target::SqlServer);
        let base = json!({"host": "h", "database": "d", "user": "u"});
        let with = |extra: serde_json::Value| {
            let mut c = base.clone();
            for (k, v) in extra.as_object().expect("object") {
                c[k] = v.clone();
            }
            ctx(c)
        };
        let batch = with(json!({"tables": ["t"]}));
        assert!(f.source(&batch).expect("source").is_some());
        assert!(f.sink(&batch).expect("sink").is_none());
        let sink = with(json!({"table": "t"}));
        assert!(f.source(&sink).expect("source").is_none());
        assert!(f.sink(&sink).expect("sink").is_some());
        assert!(
            f.source(&with(json!({"mode": "cdc", "tables": ["t"]})))
                .expect("cdc")
                .is_some()
        );
        assert!(f.source(&ctx(json!({"user": "u"}))).is_err());
    }
}
