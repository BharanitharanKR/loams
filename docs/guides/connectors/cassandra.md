# Cassandra

The Cassandra connector reads a table by token range and upserts rows by primary key. It is built in CN2 Task 14 in `loams-connector-kv` on the `scylla` driver 1.9.0 (MIT OR Apache-2.0), CQL native protocol v4.

**Suites run against:** Apache Cassandra 5.0.8 (`cassandra`, Apache-2.0), compose service `cassandra` in the `connectors-p2` profile. **Narrowings:** none. **Fallback:** none.

## Source

- **Splits.** The Murmur3 token ring `[-2^63, 2^63 - 1]` is cut into `splits` contiguous token ranges (default 8), `tokens-<i>-of-<n>`.
- **Reads.** A split reads a page at a time with `SELECT JSON * FROM ks.table WHERE token(<partition key>) >= ? AND token(<partition key>) <= ?`.
- **Position.** A split's position is the driver's paging state, and whether the range is finished. A restart resumes at the page after the last one the Fabric acknowledged (`cassandra_paging_state_resumes`).
- **Events.** Each row is an `io.loams.dev.flow.cassandra.document.v1` event:
  - Its `subject` is the row's primary key as JSON.
  - Its data is the row as `SELECT JSON` renders it.
  - Its id is a digest of the row, so an unchanged row read again is dropped by `ingest`.
- **Passes.** Without `poll_interval_ms` the table is read once.

## Sink

- **Upserts.** Each event is `INSERT INTO ks.table JSON ? DEFAULT UNSET`. This is an upsert by primary key: a replay leaves one row, and columns the event omits are left as they are.
- **Deletes.** A `loamsop = d` event is `DELETE … WHERE <key column> = fromJson(?) …`, over every primary-key column.
- **Execution.** Statements are prepared once and marked idempotent. Up to 32 run at a time.
- **Delivery.** The sink is `effectively_once`.
- **Poison.** An event that lacks a key column, or that the server refuses as invalid, is `Poison` and goes to the DLQ.
- **Key.** The primary key is read from `system_schema.columns`. A `key` setting must match it, or the sink refuses to start.

## Types

Rows travel as JSON documents, so no Arrow type is involved on this path. `CqlTypeMapper` maps CQL types to Arrow for the bulk path, which is not built yet (ruling R14.5):

| CQL | Arrow |
|---|---|
| `ascii`, `text`, `varchar`, `inet` | `Utf8` |
| `bigint`, `counter` | `Int64` |
| `int`, `smallint`, `tinyint` | `Int32`, `Int16`, `Int8` |
| `boolean` | `Boolean` |
| `float`, `double` | `Float32`, `Float64` |
| `blob` | `Binary` |
| `uuid`, `timeuuid` | `FixedSizeBinary(16)` |
| `timestamp` | `Timestamp(ms, "UTC")` |
| `date` | `Date32` |
| `time` | `Time64(ns)` |
| `list<T>`, `set<T>` | `List<T>` |
| `map<K, V>` | `Map<K, V>` |
| `vector<T, n>` | `FixedSizeList<T, n>` |
| `frozen<T>` | as `T` |
| `decimal`, `varint`, `duration`, tuples, user types | refused (`Unsupported`), never guessed |

## Settings and auth

| Setting | Meaning |
|---|---|
| `contact_points` | `host[:port]` of one or more nodes; the port defaults to 9042. |
| `keyspace`, `table` | The table to read or write. |
| `user`, `password` | The user, and the name of the secret holding its password. |
| `consistency` | `local_quorum` (the default), `quorum`, `one`, `local_one` or `all`. |
| `splits`, `page_size`, `poll_interval_ms` | How the source reads. |
| `key` | The primary key, checked against the table's. |

Contact points are resolved and egress-checked by the SDK's hook before the driver sees them. A host filter applies the same check to every peer the driver discovers.

In PR CI, Cassandra runs with `AllowAllAuthenticator`, so the `auth` suite runs `none`. `basic` (PasswordAuthenticator) is implemented but not yet exercised. TLS is not built (ruling R14.7).

## Registry status

`connectors/registry/cassandra.yaml` is the hand-written manifest this connector ships, with its config schema `connectors/schemas/cassandra.config.json` (ruling R14.1). It stays `planned` until the family's suites are green in CI (OR-23).
