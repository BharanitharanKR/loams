//! `loams-connector-mysql`: the MySQL connector family (CN2 Task 11, OR-6,
//! OR-9).
//!
//! One [`MysqlFactory`] per manifest, each a
//! [`loams_connector_sdk::ConnectorFactory`]:
//!
//! | Manifest | Target profile | Source | Sink |
//! |---|---|---|---|
//! | `mysql` | — | batch ([`batch`]) or row-binlog CDC ([`cdc`]) | upsert, exactly-once ([`sink`]) |
//! | `mariadb` | `mariadb` | as MySQL, with MariaDB's GTIDs ([`gtid`]) | as MySQL |
//! | `aurora` (`engine: mysql`) | `aurora-mysql` | as MySQL; IAM tokens ([`rds_iam`]) | as MySQL |
//!
//! The instance's `mode` (`batch`, the default, or `cdc`) picks the source;
//! a `table` in the config makes the instance a sink. Every connection goes
//! through `sdk::net::connect` (egress after DNS) and this crate's own
//! protocol client ([`conn`], over `mysql_common`), TLS is rustls with ring,
//! and every secret is resolved by name through the instance's
//! `SecretResolver`.

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};

pub mod batch;
pub mod binlog;
pub mod cdc;
pub mod config;
pub mod conn;
pub mod decode;
pub mod gtid;
#[cfg(feature = "conformance")]
pub mod harness;
pub mod history;
pub mod rds_iam;
pub mod rows;
pub mod sink;
pub mod snapshot;
#[cfg(test)]
mod synth;
pub mod table;
pub mod types;

pub use config::{Mode, MyConfig, Target};

/// The factory of one manifest of the family.
#[derive(Debug, Clone, Copy)]
pub struct MysqlFactory {
    target: Target,
}

impl MysqlFactory {
    pub const fn new(target: Target) -> Self {
        Self { target }
    }

    pub fn target(&self) -> Target {
        self.target
    }

    /// The instance's config: the profile's `defaults` under the user's.
    pub fn config(&self, ctx: &ConnectorContext) -> Result<MyConfig, ConnectorError> {
        let merged = config::merge_defaults(&ctx.config, ctx.profile.as_ref());
        MyConfig::parse(self.target, &ctx.instance, &merged)
    }

    /// `DeleteInstance`: remove the instance's watermark row. A binlog
    /// replica holds nothing on the server once its connection closes (no
    /// slot to drop), so this is tidiness, not a leak (Review Focus 6).
    pub async fn delete_instance(&self, ctx: &ConnectorContext) -> Result<(), ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.mode != Mode::Cdc {
            return Ok(());
        }
        let mut conn = conn::connect(&cfg, ctx).await?;
        if table::describe_def(&mut conn, &cfg.watermark_table)
            .await?
            .is_some()
        {
            conn.exec(&format!(
                "DELETE FROM {} WHERE instance = {}",
                cfg.watermark_table.quoted(),
                types::quote_str(&ctx.instance.to_string())
            ))
            .await
            .map_err(|e| e.to_connector("removing the watermark row"))?;
        }
        conn.quit().await;
        Ok(())
    }
}

impl ConnectorFactory for MysqlFactory {
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
            Mode::Batch => {
                if cfg.tables.is_empty() {
                    return Err(ConnectorError::fatal(
                        "config: a batch source needs `tables`",
                    ));
                }
                (
                    Box::new(batch::BatchEnumerator::new(&cfg)),
                    Box::new(batch::BatchFactory::new(cfg, ctx.clone(), id)),
                )
            }
            Mode::Cdc => (
                Box::new(cdc::MyCdcEnumerator),
                Box::new(cdc::MyCdcFactory::new(cfg, ctx.clone(), id)),
            ),
        }))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.table.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(sink::MySinkFactory::new(cfg, ctx.clone())?)))
    }
}

/// Every factory of the family, for the runtime's registry (Task 8 links
/// them into `loams-flow` behind `conn-mysql`). The `aurora` manifest has
/// two engines; its runtime dispatches on the config's `engine` between this
/// factory and the Postgres family's (ruling R11.10).
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    [Target::Mysql, Target::Mariadb, Target::Aurora]
        .into_iter()
        .map(|t| Box::new(MysqlFactory::new(t)) as Box<dyn ConnectorFactory>)
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
        assert_eq!(ids, ["mysql", "mariadb", "aurora"]);
    }

    #[test]
    fn mode_and_table_pick_the_directions() {
        let f = MysqlFactory::new(Target::Mysql);
        let base = json!({"host": "h", "user": "u", "database": "d"});
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
            f.source(&with(json!({"mode": "cdc"})))
                .expect("cdc")
                .is_some()
        );
        assert!(f.source(&with(json!({}))).is_err(), "batch needs tables");
        assert!(f.source(&ctx(json!({"user": "u"}))).is_err());
    }
}
