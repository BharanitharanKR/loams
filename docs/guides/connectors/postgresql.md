# PostgreSQL connector

The native Postgres family (`fabric/crates/loams-connector-postgres`, CN2 Task 10) serves four manifests: `postgresql` (this guide), [`aurora`](aurora.md), [`cockroachdb`](cockroachdb.md) and [`pgvector`](pgvector.md). Everything is first-party Rust on the connector SDK: `tokio-postgres` for SQL, a CopyBoth client of its own for logical replication, and `pg_walstream` 0.9.0 to decode pgoutput (plan rulings R0.6, R10.1).

**Tested against:** PostgreSQL 17 (`pgvector/pgvector:0.8.6-pg17`, compose service `pgvector`, profile `connectors-p2`), with `wal_level=logical`. The suites run in PR CI; until they are green there the manifests stay at their current status (OR-23).

## Directions

| Mode | What it does | Events |
|---|---|---|
| `mode: batch` (default) | Reads each table in `tables` in primary-key order with binary `COPY`, in Arrow batches | `rows`, Arrow (`application/vnd.apache.arrow.stream`) |
| `mode: cdc` | A pgoutput slot: an initial snapshot, then every change | `change` (JSON) and `schema-change`; the snapshot as `change` Arrow batches with `loamsop = r` |
| `table: …` | The upsert sink, exactly-once by default | — |

## Configuration

| Key | Meaning | Default |
|---|---|---|
| `host`, `port`, `database`, `user` | Where to connect | port 5432, database `postgres` |
| `password` | The **name** of the secret holding the password | none |
| `auth` | `basic`, or `iam` (Aurora; see its guide) | `basic` |
| `sslmode` | `disable`, `require` (encrypt, no verification) or `verify-full` | `disable` (`verify-full` with IAM) |
| `ca_pem` | Extra trusted roots (PEM) for `verify-full` | webpki roots only |
| `tables` | Tables to read, `schema.table` or `table` (`public`) | — |
| `key` | Batch reads: the columns to page by; sink: the upsert key | the primary key |
| `publication` | CDC: a publication you manage | `loams_<instance>`, created and owned by Loams |
| `slot` | CDC: the slot name | `loams_<namespace>_<instance>` |
| `snapshot` | CDC: `initial` or `never` | `initial` |
| `streaming` | CDC: receive in-progress transactions (protocol 2) | `false` |
| `stream_buffer_bytes` | CDC: the most a streamed transaction may buffer | 64 MiB |
| `heartbeat_ms` | CDC: the idle heartbeat; `0` disables | 10000 |
| `status_interval_ms` | CDC: Standby Status Update interval | 10000 |
| `table`, `schema_name` | Sink: the target table | — |
| `transactional` | Sink: exactly-once with `_loams_checkpoints` | `true` |
| `checkpoint_schema` | Sink: where `_loams_checkpoints` lives | `public` |
| `evolution`, `lossy` | Schema policy (see [schema mapping](schema-mapping.md)) | `additive`, none |

Every connection is made through the SDK's egress check, after DNS.

## CDC

Requirements on the server: `wal_level = logical`, a free replication slot and WAL sender, and a role with `REPLICATION` (or the cloud equivalent) that can `CREATE PUBLICATION` on the tables (or a publication you name).

- **Snapshot handoff.** The slot is created with an exported snapshot; every published table is copied inside that snapshot while the slot waits, and streaming starts at the slot's consistent point: no gap and no overlap. A snapshot cut short by a crash cannot resume (the exported snapshot dies with its connection), so the slot is re-created and the snapshot restarts under a new consistent point; rows of the first attempt that already reached the Fabric arrive again with new ids (at least once).
- **Ids and order.** A change's id is `<slot>/<commit LSN>/<ordinal>`; re-reading a transaction yields the same ids, which `ingest` deduplicates. `loamslsn` is `<commit LSN, 16 hex digits>/<ordinal, 10 digits>` and sorts as text.
- **Data.** `{"table", "op", "key", "before", "after", "unchanged"}`. Numbers and booleans are JSON values, `json`/`jsonb` are embedded, `vector` is an array, and everything else (numeric, timestamps, `bytea` as `\x…`) is Postgres's own text. With the default `REPLICA IDENTITY`, `before` holds only the key on deletes and is absent on updates; set `REPLICA IDENTITY FULL` on a table for full before-images. Unchanged TOASTed values are not in the stream: such columns are listed in `unchanged` and the sink leaves them as they are.
- **TRUNCATE** is a `change` with `loamsop = t` and no key. The sink does not apply it (it goes to the DLQ).
- **WAL release.** The slot's flush LSN moves only after the Fabric acknowledges the data. On a quiet database the heartbeat (`pg_logical_emit_message`) and the server's keepalives keep it moving.
- **Schema changes.** `ALTER TABLE … ADD COLUMN` produces a `schema-change` event and later rows carry the column. Anything the evolution policy refuses pauses the instance until it is restarted under another policy.
- **Stop and delete.** Stopping an instance keeps its slot, so a restart resumes. Deleting it drops the slot (ending a lingering session) and a Loams-owned publication. See the [runbook](../../runbooks/connectors/postgres-cdc.md) for slot lag and recovery.

## Sink

Rows are upserted (`INSERT … ON CONFLICT (key) DO UPDATE`) with each value sent as text and cast to the column's type, so any type the server parses (including `vector`) works. An event may be a plain JSON row, a CDC change (`loamsop = d` deletes by key) or an Arrow batch. Within a batch the last operation per key wins. A column the table does not have pauses the instance rather than being dropped.

Exactly-once: each batch is one transaction that also upserts the instance's row in `_loams_checkpoints (instance, split, position, display, epoch, txn)`. The upsert is fenced by the lease epoch, and after a crash the sink resumes from the checkpoint the database committed. It does not use `PREPARE TRANSACTION`, so `max_prepared_transactions` may stay 0.

## Types

See the table in `loams-connector-postgres/src/types.rs`; in short: integers, floats, `numeric(p,s)` (`Decimal128`; `numeric` without precision as exact text), text types, `bytea`, `date`, `time`, `timestamptz`, `uuid`, `json`/`jsonb` and `vector`. `timestamp without time zone` is refused unless listed in `lossy` (it is read as UTC). Arrays, `interval`, network types, ranges and enums are not mapped and pause the instance.

## Fallbacks

The Debezium Postgres connector (`debezium-postgres`) and Iggy's `postgres_source`/`postgres_sink` remain opt-in fallbacks (OR-4, OR-8). Task 2 records them in the manifests.
