# MariaDB connector

MariaDB runs on the native MySQL family (`fabric/crates/loams-connector-mysql`, CN2 Task 11) through the [`mariadb`](../../../connectors/profiles/mariadb.yaml) target profile. Everything in the [MySQL guide](mysql.md) applies: batch reads, CDC from the row binlog with the DBLog snapshot, the exactly-once upsert sink, the configuration and the types. This page lists what differs.

**Tested against:** MariaDB 11.8 LTS (`mariadb:11.8.8`, compose service `mariadb`, profile `connectors-p2`), with `--log-bin --binlog-format=ROW --binlog-row-metadata=FULL --server-id=1`. The suite runs against MariaDB itself, never against MySQL (OR-2).

## GTIDs

MariaDB's GTID is `domain-server-sequence`, and GTIDs are always on. The position is MariaDB's GTID list, the last GTID of each domain (as `@@gtid_binlog_pos` writes it). A restart sets `@mariadb_slave_capability = 4` and `@slave_connect_state` to that list before the binlog dump, and the server sends everything after it. `mysql_common` has no MariaDB GTIDs, so the family reads `MARIADB_GTID_EVENT` (0xA2) and `MARIADB_GTID_LIST_EVENT` (0xA3) itself (R0.7). A change's id is `<domain>-<server>-<sequence>/<ordinal>`. A server with no GTID yet is read from its current binlog file and offset until its first transaction.

A list the server no longer has (purged binlogs) makes the dump fail with error 1236. The instance stops with `PositionLost` and a hint to delete and recreate it.

## Differences from MySQL

- **Row events** are MariaDB's v1 events. `binlog_row_metadata` defaults to MINIMAL on MariaDB, so without FULL the schema history names the columns (see the MySQL guide).
- **`json`** is an alias for `longtext` with a check constraint, so it maps as text.
- **Compressed binlog events** (`log_bin_compress = ON`) are refused: turn it off.
- **Binlog status:** the connector asks `SHOW BINARY LOG STATUS` (MySQL 8.2+'s spelling) and falls back to `SHOW MASTER STATUS` where the server does not know it.
- **Authentication:** `mysql_native_password` and `caching_sha2_password` work. `ed25519` and `parsec` are refused by name.

## Fallbacks

Debezium's MariaDB connector remains an opt-in fallback for CDC (OR-4). Task 2 records it in the manifest.
