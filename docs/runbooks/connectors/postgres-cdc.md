# Runbook: Postgres CDC (native pgoutput)

For instances of `postgresql`, `aurora` and `pgvector` with `mode: cdc` (CN2 Task 10). CockroachDB changefeeds hold no slot; see [its guide](../../guides/connectors/cockroachdb.md).

## What the connector holds on the server

- A logical replication slot, `loams_<namespace>_<instance>` (or the configured `slot`), using `pgoutput`.
- A publication, `loams_<instance>`, owned by Loams (or one you named, which Loams never alters or drops).

The slot keeps WAL until the connector confirms it. The connector confirms an LSN only after the Fabric has acknowledged everything before it. A stopped instance therefore makes the slot hold WAL for as long as it is stopped.

## Watching it

`PgCdcFactory::lag()` samples, every 30 s (Task 24 exports these as metrics):

- **lag bytes**: `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`;
- **lag seconds**: time since the commit of the last transaction delivered;
- **headroom**: `safe_wal_size`, the WAL that can still be written before `max_slot_wal_keep_size` invalidates the slot (absent when unlimited).

By hand:

```sql
SELECT slot_name, active, active_pid, wal_status, safe_wal_size,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots WHERE slot_name LIKE 'loams_%';
```

## Symptoms and actions

| Symptom | Cause | Action |
|---|---|---|
| Lag bytes grow while the instance runs | The Fabric or a downstream sink is slow (backpressure), or the instance is paused | Check the instance's status and `last_error`; a `SchemaIncompatible` pause needs a restart under another `evolution`/`lossy` policy |
| Lag grows on a quiet database | The heartbeat is off (`heartbeat_ms: 0`) | Set `heartbeat_ms` (default 10 s) |
| `wal_status` is `unreserved` | The slot is close to `max_slot_wal_keep_size` | Resume the instance, or raise the limit; if the slot is lost, see below |
| Instance fails with `position_lost`: slot no longer exists, or lost its WAL | The slot was dropped by hand, or invalidated | Delete and recreate the instance: it re-creates the slot and takes a new snapshot. Downstream, the new snapshot's rows are new events (a re-snapshot is at least once) |
| Start fails: replication slot is active (55006) | A previous session still holds the slot (a crashed process, before `wal_sender_timeout`) | A resume retries. The connector ends a lingering session itself only when it re-creates the slot (a fresh start, or a snapshot cut short) or deletes the instance |
| Disk filling on the source after deleting an instance outside Loams | A slot left behind | `SELECT pg_drop_replication_slot('<slot>')` once you are sure no instance uses it |

## Deleting an instance

`DeleteInstance` drops the slot (ending a session that holds it) and the Loams-owned publication. `StopInstance` keeps both. A stopped instance keeps WAL, so delete instances that will not come back.

## Large transactions

By default the server decodes a transaction completely before sending it, spilling to its own disk past `logical_decoding_work_mem`. With `streaming: true` (PG 14+), in-progress transactions are sent early and buffered by the connector up to `stream_buffer_bytes` (64 MiB). A larger one fails the instance with a hint to turn streaming off.
