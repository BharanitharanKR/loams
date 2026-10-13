# Delta Lake

Native in both directions (OR-18), `loams_connector_lakehouse::delta` (CN2 Task
17), on `deltalake` 1.1.0 without its `datafusion` feature (R0.14, R17.6).
Storage is S3 and its targets (RustFS, MinIO, R2) through `object_store` 0.13.2
with the egress-checked client (handed to deltalake as the table's storage
backend), or a local directory.

## Sink: exactly-once through Delta's `txn` action

A `TransactionalSink` (OR-7). Per Fabric batch:

1. `write` stages the rows (an Arrow batch as is; events as one JSON object per
   `data`, decoded against the table's schema).
2. `prepare` writes them as Parquet data files. No log entry names them yet, so
   they are invisible.
3. `commit` makes **one** Delta commit holding the files' `add` actions, the
   `txn` action (`appId` = `loams/<namespace>/<instance>`, `version` = the sink's
   commit sequence) and, in `commitInfo`, the Fabric checkpoint
   (`loams.checkpoint`). Data and checkpoint land atomically.
4. After a crash, `recover` reads `transaction_version(appId)` and the
   checkpoint of that commit; the Fabric reader seeks there. A commit that
   already landed is not repeated (`commit` is idempotent); files a crash left
   after `prepare` are unreferenced and `VACUUM` removes them.

| | |
|---|---|
| Modes | `append` (`merge` needs deltalake's `datafusion` feature, which is not built: R17.6) |
| Creating the table | from the first batch's schema when `sink.create` (default `true`), partitioned by `sink.partition_by` |
| Schema | a column the table lacks pauses the instance (`SchemaIncompatible`); the sink does not evolve the table yet (R17.6) |
| Delivery | `exactly_once` |

## Source: versions and the change data feed

One split per table; the position is a table version plus the file and row
within it, so a restart resumes mid-file. Without a position it reads the latest
snapshot and then each later version (`source.start: earliest` reads from
version 0, failing with `PositionLost` once the log has been cleaned up).

- `source.mode: versions` (default): the rows each version adds, as `rows` events
  (Arrow).
- `source.mode: cdf`: the change data feed by the Delta protocol's rules: a
  version's `cdc` files when it has any, else its `add` files as `insert` and its
  data-changing `remove` files as `delete`; each row carries `_change_type`,
  `_commit_version` and `_commit_timestamp`. Emitted as `change` events (Arrow).

The source reads the JSON log itself (R17.6), so CDF needs no datafusion. Tables
with deletion vectors are refused rather than read wrong, and a data file larger
than `source.max_file_bytes` (512 MiB) stops the instance (it is read whole).

| | |
|---|---|
| Tested against | real Delta tables on a local directory in every build (exactly-once under crashes, CDF ops, resume); RustFS when a job provides `LOAMS_RUSTFS_*`; a real S3 prefix nightly (`LOAMS_DELTA_TABLE_URI`) |

```yaml
table_uri: s3://lake/sales/orders
storage:
  endpoint: https://rustfs.internal:9000
  region: us-east-1
  auth: { access_key_id: s3-key, secret_access_key: s3-secret }
source: { mode: cdf, start: snapshot }
sink: { partition_by: [day] }
```

On S3 the default Delta log store commits with `PutMode::Create`, which
`object_store` sends as `If-None-Match: *` (R0.13): the store must support
conditional writes (AWS S3 and RustFS do).
