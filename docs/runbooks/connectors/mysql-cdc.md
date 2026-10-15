# Runbook: MySQL and MariaDB CDC (native row binlog)

For instances of `mysql`, `mariadb` and `aurora` (`engine: mysql`) with `mode: cdc` (CN2 Task 11).

## Server settings

| Setting | Value | Why |
|---|---|---|
| `log_bin` | on (MySQL 8 default; MariaDB: `--log-bin`) | The binlog is the source |
| `binlog_format` | `ROW` (MariaDB's default is `MIXED`) | Row events carry the data |
| `binlog_row_image` | `FULL` | Before and after images of every column |
| `binlog_row_metadata` | `FULL` recommended | Column names in the binlog; without it the schema history names them |
| `binlog_row_value_options` | `''` (not `PARTIAL_JSON`) | Partial JSON updates have no full value |
| `gtid_mode`, `enforce_gtid_consistency` | `ON` recommended (MySQL) | A position that survives failover; MariaDB always has GTIDs |
| `log_bin_compress` (MariaDB) | `OFF` | Compressed events are refused |
| `binlog_expire_logs_seconds` (MySQL), `binlog_expire_logs_seconds`/`expire_logs_days` (MariaDB) | longer than the longest time an instance may stay stopped | A purged position needs a new snapshot |

The connector checks the first three at start and fails with the setting's name.

## Account

```sql
CREATE USER 'loams'@'%' IDENTIFIED BY '…';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'loams'@'%';
GRANT SELECT ON shop.* TO 'loams'@'%';
-- The default (DBLog) snapshot: the watermark table, written by the connector.
CREATE TABLE IF NOT EXISTS shop._loams_watermarks (
  instance VARCHAR(255) NOT NULL PRIMARY KEY,
  mark VARCHAR(64) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
GRANT INSERT, UPDATE, DELETE ON shop._loams_watermarks TO 'loams'@'%';
-- Only with snapshot.locking: global-read-lock (instead of the watermark table):
GRANT RELOAD ON *.* TO 'loams'@'%';
```

The watermark table must be InnoDB, in a database the binlog records (not excluded by `binlog-ignore-db`), and `watermark_table` names it if it is not `<database>._loams_watermarks`. If the connector's watermark rows never reach the binlog, it fails after two minutes and says so.

## What the connector holds on the server

Nothing that outlives its connection. A binlog replica registers with its `server_id` while it reads (`SHOW REPLICAS` / `SHOW SLAVE HOSTS`) and holds no slot. The server keeps binlogs by its own retention, whatever the connector does. The instance's watermark row is removed when the instance is deleted. Two instances must not share a `server_id`, because the server would disconnect one of them. The id is derived from the instance unless `server_id` is set.

## Watching it

The connector reports its lag (the age of the last binlog event it decoded, `0` when caught up) every 5 s through the observer (`loams_flow_connector_lag_seconds`, Task 24). By hand:

```sql
SHOW BINARY LOG STATUS;              -- MySQL 8.2+; SHOW MASTER STATUS on MariaDB and older MySQL
SELECT @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged;   -- MySQL
SELECT @@GLOBAL.gtid_binlog_pos;     -- MariaDB
SHOW BINARY LOGS;                    -- what is still on disk
```

The instance's checkpoint shows its position: `gtid <set or list>` (with `+n of <transaction>` mid-transaction) or `<file>:<offset>`, and the table the snapshot is in while it runs.

## Symptoms and actions

| Symptom | Cause | Action |
|---|---|---|
| Fails at start: `binlog_format is MIXED` / `binlog_row_image is MINIMAL` / binary log off | Server settings | Fix the setting (above) and resume |
| Fails at start: the watermark table does not exist | The default snapshot needs it | Create it (above), or set `snapshot: {locking: global-read-lock}` with `RELOAD` |
| Fails with `position_lost` ("purged binary logs", error 1236) | Binlogs were purged past the checkpoint (retention, `PURGE BINARY LOGS`, a failover to a server without them) | Delete and recreate the instance: it takes a new snapshot. Downstream, the new snapshot's rows are new events (a re-snapshot is at least once). Then raise the binlog retention |
| Paused with `schema_incompatible` | A change the evolution policy refuses, an unmapped type (`datetime` not in `lossy`, spatial types), or a table whose binlog columns do not match `information_schema` even after a re-read | Fix the policy (`evolution`, `lossy`) or the table, and resume |
| Records in the DLQ with "not UTF-8" | A text column in a non-UTF-8 character set | Convert the column to `utf8mb4` |
| Fails: "partial row images" or "partial JSON update" | `binlog_row_image` or `binlog_row_value_options` changed on a running server | Set them back (above) and resume |
| Another replica keeps disconnecting | Two replicas share a `server_id` | Give the instance its own `server_id` |

## Snapshots

The DBLog snapshot reads `chunk_size` rows at a time between two watermark writes, while the binlog keeps streaming. A crash re-reads the chunk in progress, and the snapshot continues from the last delivered chunk. The locked snapshot holds `FLUSH TABLES WITH READ LOCK` only while it reads the binlog position and opens a consistent snapshot. A crash starts it over.
