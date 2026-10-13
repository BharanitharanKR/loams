//! The `mongodb` manifest's factory (`runtime.ref:
//! loams_connector_mongodb::mongodb`).

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, Delivery, SinkFactory, SourceFactory,
    SplitEnumerator,
};

use crate::config::{Mode, MongoConfig, merge_defaults};
use crate::{batch, cdc, sink};

/// The manifest id.
pub const ID: &str = "mongodb";

/// One factory: `mode` (`batch`, the default, or `cdc`) picks the source; a
/// `collection` in the config makes the instance a sink.
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoFactory;

impl MongoFactory {
    /// The instance's config: a target profile's `defaults` under the user's.
    pub fn config(&self, ctx: &ConnectorContext) -> Result<MongoConfig, ConnectorError> {
        MongoConfig::parse(&merge_defaults(&ctx.config, ctx.profile.as_ref()))
    }

    /// The delivery the instance's sink gets on its deployment: what a
    /// manifest or an instance may declare for it (exactly once needs
    /// transactions; a standalone server narrows `upsert` to effectively
    /// once and `insert` to at least once).
    pub async fn sink_delivery(&self, ctx: &ConnectorContext) -> Result<Delivery, ConnectorError> {
        sink::sink_delivery(&self.config(ctx)?, ctx).await
    }

    /// `DeleteInstance`: a change stream holds nothing on the server (no
    /// slot to drop, unlike Postgres), and the checkpoint collection is the
    /// target's data. Nothing to do; kept so the runtime calls every family
    /// alike.
    pub async fn delete_instance(&self, ctx: &ConnectorContext) -> Result<(), ConnectorError> {
        self.config(ctx).map(|_| ())
    }
}

impl ConnectorFactory for MongoFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.sink.is_some() {
            return Ok(None);
        }
        Ok(Some(match cfg.mode {
            Mode::Batch => (
                Box::new(batch::BatchEnumerator::new(cfg.clone(), ctx.clone())),
                Box::new(batch::BatchFactory::new(cfg, ctx.clone(), ID)),
            ),
            Mode::Cdc => (
                Box::new(cdc::CdcEnumerator::new(&cfg)),
                Box::new(cdc::CdcFactory::new(cfg, ctx.clone(), ID)),
            ),
        }))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = self.config(ctx)?;
        if cfg.sink.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(sink::MongoSinkFactory::new(
            cfg,
            ctx.clone(),
        )?)))
    }
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
    fn mode_and_collection_pick_the_directions() {
        let f = MongoFactory;
        assert_eq!(f.id(), "mongodb");
        let batch = ctx(json!({"hosts": ["h"], "database": "d"}));
        assert!(f.source(&batch).expect("source").is_some());
        assert!(f.sink(&batch).expect("sink").is_none());
        let cdc = ctx(json!({"hosts": ["h"], "database": "d", "mode": "cdc"}));
        assert!(f.source(&cdc).expect("cdc").is_some());
        let sink = ctx(json!({"hosts": ["h"], "database": "d", "collection": "out"}));
        assert!(f.source(&sink).expect("source").is_none());
        assert!(f.sink(&sink).expect("sink").is_some());
        assert!(f.source(&ctx(json!({"database": "d"}))).is_err());
    }
}
