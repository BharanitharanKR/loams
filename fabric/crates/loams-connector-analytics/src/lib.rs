//! `loams-connector-analytics`: the analytics connector family (CN2 Task
//! 17, OR-6) on `loams-connector-sdk`.
//!
//! - [`clickhouse_sink`]: the ClickHouse ★'s native sink (OR-8):
//!   `INSERT … FORMAT ArrowStream` with a dedup token per batch.
//! - [`redshift`]: Redshift through the Data API; `UNLOAD` to Parquet for
//!   the bulk path; `COPY` and `MERGE` for the sink.
//! - [`databricks`]: the SQL Statement Execution API with Arrow external
//!   links; volume files and `COPY INTO`/`MERGE` for the sink.
//! - [`trino`]: Trino's client protocol, a source only.
//! - [`adbc`]: ADBC targets such as DuckDB, with drivers loaded by path from
//!   an allowlist with a SHA-256 check (CN1 Ruling 9).
//!
//! The batch sources share [`query`]'s paged reader. `loams-flow` links the
//! crate behind `conn-analytics` through [`factories`].

pub mod adbc;
pub mod aws;
pub mod clickhouse_sink;
pub mod common;
pub mod databricks;
pub mod query;
pub mod redshift;
pub mod rows;
pub mod s3;
pub mod trino;

use loams_connector_sdk::ConnectorFactory;

/// Every connector of the family, for `loams-flow`'s registry. The DuckDB
/// target reads the runtime's ADBC allowlist from `LOAMS_ADBC_ALLOWLIST`.
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    vec![
        Box::new(clickhouse_sink::ClickHouse),
        Box::new(redshift::Redshift),
        Box::new(databricks::Databricks),
        Box::new(trino::Trino),
        Box::new(adbc::Adbc::duckdb()),
    ]
}
