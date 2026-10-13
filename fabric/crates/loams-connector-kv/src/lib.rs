//! `loams-connector-kv`: the key-value and wide-column connector family
//! (CN2 OR-6, plan Task 14).
//!
//! * [`redis`] — Redis ★ (built here directly on the SDK, R0.2), Valkey (the
//!   `valkey` target profile, OR-2) and Redis Streams: keyspace reads by
//!   `SCAN`, Streams through a consumer group with `XACK` after the Fabric
//!   acknowledges, and a pipelined `XADD`/`SET`/`HSET`/`PUBLISH` sink.
//! * [`dynamodb`] — parallel `Scan` segments as splits, and a
//!   `BatchWriteItem` sink that retries `UnprocessedItems`, with conditional
//!   writes on an optional version attribute.
//! * [`ddbstreams`] — DynamoDB Streams shards as splits, parents before
//!   children, `loamsop` from `eventName`, a trimmed shard `PositionLost`.
//! * [`cassandra`] — token-range splits read with paging state as the
//!   position, and an upsert sink idempotent by primary key.
//!
//! Every connection goes through the SDK's egress-checked dialler (`sdk::net`
//! for RESP, `sdk::http::client` for the AWS APIs) or, for CQL, through
//! contact points resolved and checked by the SDK's hook plus a host filter
//! that applies the same check to every peer the driver discovers.
//!
//! With the `conformance` feature, [`harness`] holds the conformance-kit
//! harnesses each manifest's suites run through, against the real systems of
//! `conformance/connectors/systems.toml`.

pub mod aws;
pub mod cassandra;
pub mod config;
pub mod ddbstreams;
pub mod dynamodb;
#[cfg(feature = "conformance")]
pub mod harness;
pub mod redis;
pub mod util;

use std::sync::Arc;

use loams_connector_sdk::ConnectorFactory;

/// Every factory this family registers, one per manifest id.
pub fn factories() -> Vec<Arc<dyn ConnectorFactory>> {
    let mut out = redis::factories();
    out.push(Arc::new(dynamodb::DynamoDbFactory::new()));
    out.push(Arc::new(ddbstreams::DynamoDbStreamsFactory::new()));
    out.push(Arc::new(cassandra::CassandraFactory));
    out
}
