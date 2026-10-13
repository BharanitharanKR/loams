//! `loams-connector-lakehouse`: the lakehouse connector family (CN2 Task 17,
//! OR-6) on `loams-connector-sdk`.
//!
//! - [`delta`]: Delta Lake in both directions (OR-18). The sink is
//!   exactly-once through Delta's `txn` action; the source reads table
//!   versions and, with `source.mode: cdf`, the change data feed.
//! - [`iceberg_sink`]: the native Iceberg raw-topic sink (OR-8), which
//!   refuses to start until the owner rules on R0.15 (R17.1).
//!
//! Heavy dependencies (`deltalake`, `object_store`) are compiled by this
//! crate only (OR-6). `loams-flow` links it behind `conn-lakehouse` through
//! [`factories`].

pub mod delta;
pub mod iceberg_sink;
pub mod rows;
pub mod store;

use loams_connector_sdk::ConnectorFactory;

/// Every connector of the family, for `loams-flow`'s registry.
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    vec![
        Box::new(delta::DeltaLake),
        Box::new(iceberg_sink::IcebergSink),
    ]
}
