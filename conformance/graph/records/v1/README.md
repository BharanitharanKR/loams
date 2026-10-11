# Graph records, version 1

The value of every log record in a Loams Graph's implicit stream is one **graph record**: a
`GraphChangeSet` (what one committed write transaction changed) or a `SchemaChange` (one schema
statement). Design §48 §6.4, GR1 Task 10. The codec is `loams_graph::changeset::codec`.

The format is engine-neutral (§48 §6.1 I6): a reader needs neither Grafeo nor Loams to understand
it. The `.bin` files here are frozen. `crates/loams-graph/tests/codec.rs` decodes each one to the
record it names and encodes that record back to the same bytes. A change that breaks either is a
new version, not an edit. Add a case with
`UPDATE_GOLDEN=1 cargo test -p loams-graph --test codec golden_vectors_decode`.

| File | Record |
|---|---|
| `changeset_empty.bin` | A change set with no key, no link and no operations |
| `changeset_ops.bin` | Every operation kind, an idempotency key and a link's progress |
| `changeset_values.bin` | One node whose properties hold every value kind |
| `schema_change.bin` | A `SchemaChange` |

## Primitives

| Name | Encoding |
|---|---|
| `u8` | One byte |
| `uvar` | Unsigned LEB128, at most 10 bytes, **minimal** (no trailing `0x00` group), at most 2^64 − 1 |
| `svar` | Zigzag (`(n << 1) ^ (n >> 63)`), then `uvar` |
| `i32` | An `svar` whose value fits 32 bits |
| `f64`, `f32` | IEEE 754 bits, little-endian (every bit kept: NaN payloads, the sign of zero) |
| `str` | `uvar` byte length, then UTF-8 |
| `bool`, `option` | One byte, `0` or `1`; `option<T>` is `0`, or `1` then `T` |
| `list<T>` | `uvar` count, then the items |
| `id` | 16 bytes: the graph's ULID, big-endian (the `gr_<ULID>` on the wire) |

Keys of every map (properties, map values, counters, a link's partitions) are **strictly
increasing** (strings by their UTF-8 bytes), so a record has exactly one encoding. A decoder
refuses anything else: a non-minimal varint, a byte other than 0 or 1 for a boolean or an option,
keys out of order, an unknown tag, or bytes after the record.

## Record

```
record      = version:u8 (= 1)  kind:u8  body
kind 1      = change_set: graph:id lease_epoch:uvar commit_epoch:uvar txn_seq:uvar
                          idempotency_key:option<str> link:option<link> ops:list<op>
kind 2      = schema:     graph:id lease_epoch:uvar commit_epoch:uvar txn_seq:uvar
                          idempotency_key:option<str> language:u8 statement:str
link        = link_id:uvar version:uvar applied_after:list<(partition:uvar (32 bits), offset:uvar)>
language    = 0 unspecified | 1 GQL | 2 Cypher | 3 SQL/PGQ | 4 Gremlin | 5 GraphQL | 6 SPARQL
```

- `commit_epoch` is Grafeo's epoch on the owner that committed, and is informational. Replay order
  and idempotency use the record's log offset (R9.5). `txn_seq` is Loams' own per-graph counter, one
  per record of either kind.
- `link` is present when a link target committed the change set (R0.17).
- There is no checksum: the log's batches carry a crc32c (R0.18).

## Operations

A change set's operations are the transaction's net effect, in the canonical order: node upserts,
edge upserts, property changes, edge deletes, node deletes (R9.2).

```
op   = 1 upsert_node: id:uvar labels:list<str> props
     | 2 delete_node: id:uvar
     | 3 upsert_edge: id:uvar type:str src:uvar dst:uvar props
     | 4 delete_edge: id:uvar
     | 5 set_props:   id:uvar kind:u8 (1 node, 2 edge) set:props removed:list<str>
props = list<(key:str, value)>      keys strictly increasing
```

## Values

The kinds of `loams.graph.v1.Value` (GR1 Task 2), in exact units. Grafeo holds a node or a
relationship as a map, so it is a map here too.

| Tag | Kind | Payload |
|---|---|---|
| 0 | null | none |
| 1 | boolean | `bool` |
| 2 | int64 | `svar` |
| 3 | float64 | `f64` |
| 4 | string | `str` |
| 5 | bytes | `uvar` length, then the bytes |
| 6 | date | `i32`: days since 1970-01-01 |
| 7 | local_time | `uvar`: nanoseconds since midnight, below 86 400 × 10^9 |
| 8 | zoned_time | `uvar` nanoseconds since midnight, then `i32` UTC offset in seconds, within ±64 800 |
| 9 | local_datetime | `svar`: microseconds since 1970-01-01T00:00:00 (Grafeo's TIMESTAMP) |
| 10 | zoned_datetime | `svar` microseconds since the epoch, **UTC**, then `i32` offset within ±64 800 |
| 11 | duration | `svar` months, `svar` days, `svar` nanoseconds |
| 12 | list | `list<value>` |
| 13 | map | `list<(key:str, value)>`, keys strictly increasing |
| 14 | vector | `uvar` count, then that many `f32` |
| 15 | path | `list<value>` nodes, then `list<value>` edges |
| 16 | grow-only counter | `list<(replica:str, count:uvar)>`, replicas strictly increasing |
| 17 | positive-negative counter | two counters: positive, then negative |

At most 128 lists, maps and paths nest inside one property value. An encoder refuses a deeper
value, and a decoder refuses deeper bytes.

## A worked example: `changeset_empty.bin`

```
01                                   version 1
01                                   kind: change set
01 99 cb 6a d4 00 01 23 45 67 89 ab cd ef 01 23   graph gr_<ULID>
01                                   lease_epoch 1
00                                   commit_epoch 0
00                                   txn_seq 0
00                                   no idempotency key
00                                   no link
00                                   no operations
```
