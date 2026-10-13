# SQL Server

The native SQL Server connector reads tables in batches or as a change stream, and writes rows with an exactly-once `MERGE`. Built in CN2 Task 12 as `loams_connector_sqlserver::SqlServerFactory` (crate `fabric/crates/loams-connector-sqlserver`), on the TDS client `tiberius` 0.13.0. Azure SQL Database runs the same code through the `azure-sql` target profile ([azure-sql.md](azure-sql.md)).

**Tested against:** SQL Server 2022 Developer (`mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04`, pinned by digest in `deploy/fabric/compose.yaml`). The image is under Microsoft's EULA, so it is a `ci-service`: it runs in the `connectors-ci-only` CI job only and never ships in an image Loams builds (OR-3).

**Fallback:** Debezium Server's SQL Server connector (`debezium-sqlserver`, OR-13) stays available for CDC with `runtime: fallback`; the native source is the default.

## Connection

| Key | Meaning |
|---|---|
| `host`, `port` (1433), `database` | Where to connect. Every connection, including an Azure gateway redirect, is opened by `sdk::net::connect`, so the instance's egress policy is checked after DNS |
| `auth` | `sql` (the default for `sql-server`): `user` and `password` (a secret name). `entra`: an Entra ID service principal, `entra.tenant_id`, `entra.client_id`, `entra.client_secret` (a secret name) and `entra.authority`. `entra-token`: `token` names a secret holding an access token the runtime keeps fresh |
| `encrypt` | `strict` (the default): TDS 8.0, TLS before the first TDS byte with ALPN `tds/8.0`, so the login is encrypted. `disable`: no TLS at all, only for a server on a private network; refused with Entra ID |
| `ca_pem`, `trust_server_certificate`, `host_name_in_certificate` | Extra trusted roots; accept any certificate (a self-signed test server only); the name the certificate carries |

TDS 8.0 `strict` needs SQL Server 2022 or Azure SQL. Older servers (2019 and before) negotiate TLS inside the TDS pre-login, which this connector does not do. Connect to them with `encrypt: disable` over a private network, or use the Debezium fallback.

## Batch reads (`mode: batch`, the default)

`tables` lists the tables to read (`schema.table`, `dbo` when unqualified). Each table is read in key order (`key`, by default the primary key) with keyset paging, `SELECT TOP (n) … WHERE (key) > (last) ORDER BY key`, inside one `SNAPSHOT` transaction per run, as Arrow batches of `rows` events. Snapshot isolation needs `ALTER DATABASE … SET ALLOW_SNAPSHOT_ISOLATION ON`; without it, set `isolation: read_committed`.

- `partitions: n` splits a table whose key is one integer column into `n` key ranges over `[MIN, MAX]`, each with its own split and checkpoint. The first and last ranges are open-ended.
- `cursor_column: c` reads incrementally, ordered by `c` and then the key. With `poll_interval_ms`, the read starts again from its position that long after it reaches the end; without it, the read ends. Rows whose cursor is NULL are not read.

A restart resumes after the last key delivered. Event ids derive from the run and the row numbers, so a re-read of the same run produces the same ids.

## Change data capture (`mode: cdc`)

CDC must be enabled on the database and on each table (see the [runbook](../../runbooks/connectors/sqlserver-cdc.md)). The source polls `cdc.fn_cdc_get_all_changes_<capture_instance>(…, N'all update old')` every `poll_interval_ms` (1 s) up to `sys.fn_cdc_get_max_lsn()`.

- **Snapshot.** With `snapshot: initial` (the default), the first run records the maximum LSN, reads the table in key order under snapshot isolation (Arrow batches of `change` events with `loamsop = r`), then streams the changes after that LSN. A change that is committed but not yet harvested by the capture job when the snapshot starts can arrive twice (once in the snapshot, once streamed); none is lost. `snapshot: never` starts at the current maximum LSN.
- **Events.** One `change` event per row change, with `loamsop` `c`, `u` or `d`, `loamslsn` `<start_lsn>/<seqval>` (hex), and `data` `{table, op, key, before, after}`. An update's before and after images form one event. Ids are `<capture instance>/<start_lsn>/<seqval>/<operation>`, so a re-read produces the same id and `ingest` drops the duplicate.
- **Position.** The `(start_lsn, seqval, operation)` triple of the last change delivered. A restart resumes exactly after it, even inside a large transaction.
- **DDL.** After an `ALTER TABLE`, create a second capture instance (see the runbook). The source reads the old one up to the new one's start, emits a `schema-change` event, and follows the new one. `evolution` (`additive`, `widen` or `pause`) decides whether the change is accepted or pauses the instance.
- **Retention.** The cleanup job deletes changes older than its retention (three days by default). A position older than that is `PositionLost`: the instance stops with a re-snapshot hint. The position's age against the retention is the `position_risk` metric (`retention_minutes` overrides the value read from `msdb.dbo.cdc_jobs`), and the lag behind the maximum LSN is `lag`.

## Sink

`table` makes the instance a sink. Each batch's rows (JSON objects, `change` events with `loamsop = d` as deletes, or Arrow batches) are bulk-loaded as text into the session temp table `#loams_stage`, then applied with one `MERGE … WITH (HOLDLOCK)` by `key` (by default the primary key). Values are cast to each column's declared type on the server. Computed and `rowversion` columns are not written; an `IDENTITY` column is written under `IDENTITY_INSERT`.

With `transactional: true` (the default), the sink is **exactly-once**: each batch and its checkpoint commit in one transaction, fenced by the lease epoch. The checkpoint goes to `<checkpoint_schema>._loams_checkpoints` (`dbo` by default, created on first use). A row the target refuses (a constraint, a conversion) is rolled back alone under a savepoint and goes to the DLQ. `transactional: false` is at-least-once and idempotent by key.

## Types

| SQL Server | Arrow |
|---|---|
| `bit` | `Boolean` |
| `tinyint` | `UInt8` |
| `smallint`, `int`, `bigint` | `Int16`, `Int32`, `Int64` |
| `real`, `float` | `Float32`, `Float64` |
| `decimal(p,s)`, `numeric(p,s)`, `money`, `smallmoney` | `Decimal128` |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml` | `Utf8` |
| `binary`, `varbinary`, `image`, `rowversion` | `Binary` |
| `uniqueidentifier` | `FixedSizeBinary(16)` (`arrow.uuid`) |
| `date` | `Date32` |
| `time(n)` | `Time64(µs)`; `ns` for `n = 7` |
| `datetimeoffset(n)` | `Timestamp(µs, UTC)`; `ns` for `n = 7` |
| `datetime`, `smalldatetime`, `datetime2(n)` | As `datetimeoffset`, but **lossy**: these types have no timezone, and they are read as UTC only when the column is listed in `lossy` |
| `sql_variant`, `geography`, `geometry`, `hierarchyid` | Not supported (the instance pauses) |

A scale-7 value outside 1677–2262 does not fit in nanoseconds; such a row goes to the DLQ.
