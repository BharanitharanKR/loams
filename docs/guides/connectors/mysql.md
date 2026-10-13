# MySQL connector

The native MySQL family (`fabric/crates/loams-connector-mysql`, CN2 Task 11) serves three manifests: `mysql` (this guide), [`mariadb`](mariadb.md) and the MySQL engine of `aurora` ([below](#aurora-mysql)). Everything is first-party Rust on the connector SDK. The wire protocol, the binlog events and the row images come from `mysql_common` 0.37.3. The connection layer is the crate's own, so that every connection passes the SDK's egress check after DNS and MariaDB's GTIDs can be resumed (plan rulings R0.7, R0.8, R11.1).

**Tested against:** MySQL 8.4 (`mysql:8.4.10`, compose service `mysql`, profile `connectors-p2`), with `--gtid-mode=ON --enforce-gtid-consistency=ON --binlog-row-metadata=FULL`. The suites run in PR CI. Until they are green there, the manifests stay at their current status (OR-23).

## Directions

| Mode | What it does | Events |
|---|---|---|
| `mode: batch` (default) | Reads each table in `tables` in primary-key order, `SELECT … WHERE (key) > (last) ORDER BY key LIMIT n`, inside one consistent snapshot, as Arrow batches | `rows`, Arrow (`application/vnd.apache.arrow.stream`) |
| `mode: cdc` | Reads the row binlog as a replica: an initial snapshot, then every change | `change` (JSON) and `schema-change`; the snapshot as `change` Arrow batches with `loamsop = r` |
| `table: …` | The upsert sink, exactly-once by default | — |

## Configuration

CN1's keys (`connectors/schemas/mysql.config.json`) keep their meaning. Task 2 rewrites the schema to this table (R11.9).

| Key | Meaning | Default |
|---|---|---|
| `host`, `port`, `database`, `user` | Where to connect; `database` qualifies unqualified table names | port 3306 |
| `password` | The **name** of the secret holding the password | none |
| `auth` | `basic`, or `iam` (Aurora; also implied by CN1's `iam: {region}` block) | `basic` |
| `aws.region`, `aws.access_key_id`, `aws.secret_access_key`, `aws.session_token` | IAM: the region and the **names** of the AWS credential secrets | — |
| `tls.enabled`, `tls.verify_full`, `tls.ca_pem` | TLS (rustls, ring); `verify_full: false` encrypts without verifying | off (on, verifying, with IAM) |
| `tables` | Sources: tables to read or capture, `db.table` or `table` | CDC: every base table of `database` |
| `key` | Batch reads: the columns to page by; sink: the upsert key (a list, or CN1's comma-separated string) | the primary key |
| `chunk_size` | Rows per chunked `SELECT` (batch reads and the CDC snapshot), capped by the pipeline's batch rows | 8192 |
| `server_id` | CDC: the replica's server id | derived from the instance, in `[2^24, 2^32)` |
| `snapshot` | CDC: `initial` or `never`, or `{mode, locking}` | `initial` |
| `snapshot.locking` | CDC: `watermark` (DBLog, no lock) or `global-read-lock` | `watermark` |
| `watermark_table` | CDC: the DBLog watermark table | `<database>._loams_watermarks` |
| `heartbeat_ms` | CDC: the server's heartbeat on an idle binlog | 10000 |
| `table` | Sink: the target table | — |
| `transactional` | Sink: exactly-once with `_loams_checkpoints` | `true` |
| `checkpoint_database` | Sink: where `_loams_checkpoints` lives | `database` |
| `apply_deletes` | Sink: apply `loamsop = d` (CN1's key) | `true` |
| `engine` | The `aurora` manifest's engine; anything but `mysql` is refused here | `mysql` |
| `evolution`, `lossy` | Schema policy (see [schema mapping](schema-mapping.md)) | `additive`, none |

Every connection is made through the SDK's egress check, after DNS. Authentication speaks `mysql_native_password`, `caching_sha2_password` and `mysql_clear_password`. For `caching_sha2_password`, full authentication goes over TLS, or with the server's RSA key on a plain connection. `mysql_clear_password` is allowed only over TLS. Other plugins (`client_ed25519`, `parsec`) are refused by name.

## CDC

Requirements on the server, all in the [runbook](../../runbooks/connectors/mysql-cdc.md): `log_bin` on, `binlog_format = ROW`, `binlog_row_image = FULL`, an account with `REPLICATION SLAVE`, `REPLICATION CLIENT` and `SELECT` on the captured tables, and, for the default snapshot, the watermark table with `INSERT, UPDATE` on it.

- **Position.** With GTIDs on (`gtid_mode = ON`), the position is the GTID set of every transaction the Fabric has, and a restart asks the server for everything outside it (`COM_BINLOG_DUMP_GTID`). With GTIDs off, the position is the binlog file and offset after the last whole transaction. A transaction cut by the batch budget is re-read on restart, and its delivered changes are skipped. The position also holds the schema history (below).
- **Snapshot without locks (DBLog).** For each chunk of a table, the source writes a low watermark row, `SELECT`s the chunk by primary key, and writes a high watermark row. In the binlog, a change to a chunk row committed between the two watermarks removes that row from the chunk, because the change carries the newer state. At the high watermark, the rest of the chunk is emitted. Changes are delivered in binlog order throughout, so the snapshot and the stream interleave without loss. A crash re-reads the current chunk (at least once). Each table is announced by a new-table `schema-change` event before its rows.
- **Snapshot under a global read lock** (`snapshot: {locking: global-read-lock}`), for an account that cannot write the watermark table: `FLUSH TABLES WITH READ LOCK` (needs `RELOAD`) only while the binlog position is read and a consistent snapshot begins. The tables are read in that snapshot, and the stream starts at the recorded position. A crash restarts this snapshot.
- **Ids and order.** A change's id is `<transaction>/<ordinal>`. The transaction is its GTID (`uuid:gno`, or `uuid:tag:gno` on MySQL 8.4 tagged GTIDs), or `<file>:<offset>` of its first event with GTIDs off. Re-reading a transaction yields the same ids, which `ingest` deduplicates. `loamslsn` is `<file>/<end offset, 20 digits>/<ordinal, 10 digits>`, which sorts as text within a server's binlog.
- **Data.** `{"table", "op", "key", "before", "after"}`, in the canonical value form of the [types table](#types). The same row reads the same from the snapshot and from the binlog.
- **Schema history.** Without `binlog_row_metadata = FULL` (the server default is MINIMAL), the binlog does not name columns. The connector keeps each captured table's columns as of its position. They are seeded from `information_schema` and moved by the DDL in the binlog, which is parsed with `sqlparser`'s MySQL dialect. They are stored with the checkpoint, so a restart maps old events with the columns they were written with, even if the table changed since. With FULL metadata the names come from the binlog itself. Either way, each event's column types are checked against the history. Two cases re-read the table from `information_schema`: a mismatch, and DDL the history does not model (for example `CREATE TABLE … AS SELECT`). If they still disagree, the instance pauses (R11.5).
- **Schema changes.** `ALTER TABLE … ADD COLUMN` produces a `schema-change` event, and later rows carry the column. Anything the evolution policy refuses pauses the instance until it restarts under another policy.
- **TRUNCATE** is a `change` with `loamsop = t`. The sink does not apply it (it goes to the DLQ).
- **Purged binlogs.** If the server has purged binlogs the instance has not read (a GTID in `gtid_purged` outside its set, a missing file, or the server's error 1236), the instance stops with `PositionLost` and a hint to delete and recreate it, which takes a new snapshot.
- **Not supported, refused by name:** partial row images (`binlog_row_image` other than FULL), partial JSON updates (`binlog_row_value_options = PARTIAL_JSON`) and MariaDB's compressed events (`log_bin_compress`). A text value that is not UTF-8 is `Poison` for its row (a DLQ record), never coerced.
- **Stop and delete.** A binlog replica holds nothing on the server once its connection closes, so stopping an instance leaks nothing. The server keeps binlogs by `binlog_expire_logs_seconds` regardless of the instance, and that retention bounds how long an instance can stay stopped. Deleting an instance removes its watermark row.

## Sink

Rows are upserted with `INSERT … ON DUPLICATE KEY UPDATE col = VALUES(col)`. `VALUES()` works on MySQL 8.4 and MariaDB alike. Values are literals that need no escaping: strings as `CONVERT(X'…' USING utf8mb4)`, bytes as `X'…'`, numbers as numbers. The server casts each value to its column's type. The session runs in UTC, and `timestamp` values are canonical UTC. An event may be a plain JSON row, a CDC change (`loamsop = d` deletes by key) or an Arrow batch. Within a batch the last operation per key wins. A column the table does not have pauses the instance rather than being dropped. A value the server refuses (out of range, truncated, not JSON, a foreign key) is `Poison` for that event.

Exactly-once: each batch is one transaction that also upserts the instance's row in `_loams_checkpoints (instance, split, position, display, epoch, txn)` after locking it (`SELECT … FOR UPDATE`). A row with a newer lease epoch fences the sink. After a crash the sink resumes from the checkpoint the database committed. A connection lost at `COMMIT` is resolved by looking for the transaction's id in that table. A deadlock or a lock timeout drops the connection, so the server rolls the transaction back, and the batch is replayed. No XA is used.

## Types

| MySQL | Arrow | JSON in `change` events |
|---|---|---|
| `tinyint`…`bigint` (`unsigned`) | `Int8`…`Int64` (`UInt8`…`UInt64`); `mediumint` → 32 bits | number |
| `float`, `double` | `Float32`, `Float64` | number |
| `decimal(p,s)` | `Decimal128(p,s)`; p > 38 refused unless listed in `lossy` | string (exact) |
| `date` | `Date32` | `"YYYY-MM-DD"` |
| `datetime(n)` | `Timestamp(µs, "UTC")`; refused unless listed in `lossy` (it has no zone and is read as UTC) | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` |
| `timestamp(n)` | `Timestamp(µs, "UTC")` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` |
| `time(n)` | `Duration(µs)` (MySQL's range is ±838 h) | `"[-]HH:MM:SS[.ffffff]"` |
| `year` | `Int16` | number |
| `char(n)`, `varchar(n)`, `*text` | `Utf8` (+ `loams.max_length`) | string |
| `binary(n)`, `varbinary(n)`, `*blob` | `Binary` | base64 string |
| `bit(n)` | `UInt64` | number |
| `enum(…)`, `set(…)` | `Utf8` | string (`set`: comma-separated) |
| `json` | `Utf8` + `arrow.json` | the JSON value |
| spatial types, `vector` | refused (`Unsupported`, never guessed) | — |

Note that `datetime` columns are refused unless their path (`db.table.column`) is in `lossy`. This follows the SDK's rule for timestamps without a zone (R5.2).

## Aurora MySQL

The `aurora` manifest with `engine: mysql` runs on this family through the [`aurora-mysql`](../../../connectors/profiles/aurora-mysql.yaml) profile. Everything above applies, given `binlog_format = ROW` in the cluster parameter group, plus:

- **IAM authentication** (`auth: iam`): the connector mints a 15-minute RDS token per connection from the AWS credentials it resolves by secret name. It sends the token through `mysql_clear_password`, so TLS is required (the profile turns it on). The RDS CA is not a webpki root: set `tls.ca_pem` to the RDS bundle, or `tls.verify_full: false`.
- **Tested against:** a real Aurora MySQL cluster in the nightly job (`LOAMS_AURORA_MYSQL_URL`, `LOAMS_AWS_*`; `LOAMS_AURORA_MYSQL_IAM_USER` names an IAM-enabled account when it is not the URL's). MySQL in compose is an extra target, never the system under test.

## Fallbacks

The Debezium MySQL connector (`debezium-mysql`) remains an opt-in fallback for CDC (OR-4, OR-8). Task 2 records it in the manifests.
