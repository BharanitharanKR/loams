//! The native Iceberg raw-topic sink (OR-8): **not built, by an owner
//! decision that is pending** (CN2 R0.15, R17.1).
//!
//! The plan's sink appends through iceberg-rust with the Fabric checkpoint in
//! the snapshot summary property `loams.checkpoint` (`exactly_once`), against
//! a Lakekeeper catalog. iceberg-rust 0.10.1, the newest release (checked on
//! 2026-10-13), requires arrow and parquet `^58`, while the workspace is on
//! 59 (CN1 Ruling 4). R0.15 gives three ways forward, and choosing between
//! them is the owner's:
//!
//! - (a) wait for an iceberg-rust release on arrow 59 (its `main` moved to
//!   arrow 59.2 on 2026-09-03; no release carries it yet);
//! - (b) link iceberg 0.10.1 in this crate only, with arrow 58 beside 59 and
//!   batches crossing by Arrow IPC bytes;
//! - (c) move the workspace's Arrow.
//!
//! Until then the factory refuses to start an Iceberg sink with an error
//! naming R0.15, and the ★ Iceberg manifest keeps its Iggy `iceberg_sink`
//! fallback (OR-4), which an instance opts into with `runtime: fallback`.

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};

/// The manifest id (`connectors/registry/iceberg.yaml`).
pub const CONNECTOR: &str = "iceberg";

/// Why the sink does not start.
pub const REFUSAL: &str = "the native Iceberg sink is not available: iceberg-rust 0.10.1 needs \
     arrow 58 and the workspace is on arrow 59 (CN2 R0.15). The owner's decision between \
     (a) waiting for an iceberg-rust release on arrow 59, (b) linking arrow 58 in \
     loams-connector-lakehouse, or (c) moving the workspace's Arrow is pending (CN2 R17.1); \
     until then use the Iggy iceberg_sink fallback (runtime: fallback)";

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
