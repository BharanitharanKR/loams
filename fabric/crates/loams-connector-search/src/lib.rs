//! `loams-connector-search`: the search and vector connector family (CN2
//! Task 15, OR-6), on `loams-connector-sdk`.
//!
//! One Elasticsearch implementation serves two manifests (OR-2):
//! Elasticsearch ★ (native since OR-8; Iggy's `elasticsearch_*` plugins are
//! its fallback) and, as a target profile, OpenSearch
//! (`connectors/profiles/opensearch.yaml`). Each target's suite runs against
//! that target itself. Qdrant is its own connector.
//!
//! * [`elasticsearch::Elasticsearch`] — the
//!   [`loams_connector_sdk::ConnectorFactory`]
//!   (`runtime.ref: loams_connector_search::elasticsearch`).
//! * [`bulk`] — the `_bulk` sink: `_id` = the document key or `ce_id`
//!   (`effectively_once`), deletes on `loamsop = d`, retrying only the failed
//!   items of a partial failure, shrinking its batch on 429.
//! * [`pit`] — the point-in-time source: `search_after` with a tiebreaker,
//!   incremental by a cursor field; the position is the cursor plus the last
//!   sort values.
//! * [`qdrant::Qdrant`] (`runtime.ref: loams_connector_search::qdrant`) — an
//!   upsert sink with UUID v5 point ids and a scroll source.
//! * [`client`] — the REST client over the SDK's egress-checked HTTP client
//!   (Basic, API key, SigV4).
//! * `conformance` (feature `conformance`) — the harness the conformance
//!   kit drives against the real systems.

pub mod bulk;
pub mod client;
pub mod config;
#[cfg(feature = "conformance")]
pub mod conformance;
pub mod elasticsearch;
pub mod pit;
pub mod qdrant;

pub use bulk::{BulkSink, BulkSinkFactory};
pub use config::{Api, EsConfig, QdrantConfig};
pub use elasticsearch::Elasticsearch;
pub use pit::{IndexEnumerator, PitPosition, PitSource, PitSourceFactory};
pub use qdrant::{Qdrant, point_id};
