//! The native Iceberg raw-topic sink (OR-8): **not built yet, by the owner's
//! decision** (CN2 R0.15, R17.1).
//!
//! The plan's sink appends through iceberg-rust with the Fabric checkpoint in
//! the snapshot summary property `loams.checkpoint` (`exactly_once`), against
//! a Lakekeeper catalog. iceberg-rust 0.10.1, the newest release (checked on
//! 2026-10-13), requires arrow and parquet `^58`, while the workspace is on
//! 59 (CN1 Ruling 4). R0.15 gave three ways forward: (a) wait for an
//! iceberg-rust release on arrow 59, (b) link arrow 58 beside 59 in this
//! crate, (c) move the workspace's Arrow.
//!
//! **Owner, 2026-10-13: (a) wait; revisit when an iceberg release on arrow 59
//! is at least 14 days old.** iceberg-rust's `main` moved to arrow 59.2 on
//! 2026-09-03; no release carries it yet. Until one does (and is 14 days
//! old), the factory refuses to start an Iceberg sink with an error naming
//! R0.15 and R17.1. The ★ Iceberg manifest's declared fallback (OR-4) is
//! unchanged.

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};

/// The manifest id (`connectors/registry/iceberg.yaml`).
pub const CONNECTOR: &str = "iceberg";

/// Why the sink does not start.
pub const REFUSAL: &str = "the native Iceberg sink is not available: iceberg-rust 0.10.1 needs \
     arrow 58 and the workspace is on arrow 59 (CN2 R0.15). The owner chose to wait for an \
     iceberg-rust release on arrow 59 and revisit once it is at least 14 days old (CN2 R17.1); \
     until then an instance can opt into the manifest's declared fallback (runtime: fallback)";

/// The Iceberg raw-topic sink's factory. The Iceberg ★ source stays CN1's
/// (`loams_flow::connectors::iceberg`), so this factory has no source.
#[derive(Debug, Default, Clone, Copy)]
pub struct IcebergSink;

impl ConnectorFactory for IcebergSink {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        Ok(None)
    }

    fn sink(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Err(ConnectorError::fatal(REFUSAL))
    }
}
