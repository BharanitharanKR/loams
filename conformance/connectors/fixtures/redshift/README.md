# Amazon Redshift fixtures

Recorded only from real Redshift (OR-2), by the `connectors-nightly` job's
`redshift_unload_bulk_arrow_fixture` and `redshift_copy_merge_sink_fixture`
(`fabric/crates/loams-connector-analytics/tests/redshift.rs`) through CN2 Task
6's `FixtureProxy`, with credentials scrubbed by its header and JSON-pointer
rules (the SigV4 `Authorization` header and `X-Amz-Security-Token`).

Nothing is recorded yet. The Data API exchanges are JSON and fit the proxy's
format, but the bulk path and the sink also move Parquet through S3 (the
`UNLOAD` output and the staged `COPY` files), and `FixtureProxy`'s `Body` has no
binary variant (`empty`, `json`, `text`). Recording waits for Task 6 to add one
(CN2 R17.10). Until a recording lands here the replay halves skip, the local
request-shape halves run in every build (never counted toward a suite), and
`redshift` stays `planned`.
