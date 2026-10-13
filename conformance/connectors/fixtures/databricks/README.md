# Databricks fixtures

Recorded only from a real Databricks workspace (OR-2), by the
`connectors-nightly` job's `databricks_arrow_external_links_fixture`
(`fabric/crates/loams-connector-analytics/tests/databricks.rs`) through CN2 Task
6's `FixtureProxy`, with the workspace token and the presigned links' query
strings scrubbed.

Nothing is recorded yet. The Statement Execution API's replies are JSON, but the
result chunks behind the external links are Arrow IPC streams and the sink
uploads Parquet, and `FixtureProxy`'s `Body` has no binary variant (`empty`,
`json`, `text`). Recording waits for Task 6 to add one (CN2 R17.10). Until a
recording lands here the replay half skips, the local request-shape half runs in
every build (never counted toward a suite), and `databricks` stays `planned`.
