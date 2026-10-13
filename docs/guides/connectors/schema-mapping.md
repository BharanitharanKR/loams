# Schema mapping and evolution

How native Loams connectors map a system's types to Arrow, how a table's schema may change while an instance runs, and what an operator does when it may not. Built in CN2 Task 5 (`loams-connector-sdk`, modules `schema` and `codecs`); design §33 D356.

## Arrow is the canonical schema

Every native connector family maps its native types (Postgres OIDs, MySQL column types, BSON, CQL, ClickHouse types, Elasticsearch mappings, Delta types, …) to Arrow fields and back, through a `TypeMapper`. Each family's guide has the table of its types. Relational families start from the shared SQL mapper below and add their own types.

| Native | Arrow | Lossy |
|---|---|---|
| `boolean`, `bool` | `Boolean` | |
| `tinyint`, `smallint`, `integer`, `bigint` | `Int8` … `Int64` | |
| the same, `unsigned` | `UInt8` … `UInt64` | `UInt64` into a target without unsigned integers |
| `real`, `double precision` | `Float32`, `Float64` | |
| `decimal(p,s)`, `numeric(p,s)`, p ≤ 38 | `Decimal128(p,s)` | p > 38: truncated to `Decimal128(38,s)` |
| `char(n)`, `varchar(n)` | `Utf8`, length kept in field metadata `loams.max_length` | |
| `text` | `Utf8` | |
| `varbinary`, `bytea`, `blob` | `Binary` | |
| `date` | `Date32` | |
| `time` | `Time64(µs)` | |
| `timestamp with time zone` | `Timestamp(µs, "UTC")` | |
| `timestamp`, `datetime` (no zone) | `Timestamp(µs, "UTC")` | always: the zone is assumed to be UTC |
| `uuid` | `FixedSizeBinary(16)`, extension `arrow.uuid` | |
| `json`, `jsonb` | `Utf8`, extension `arrow.json` | |

A type with no row here is refused (`Unsupported`), never guessed.

## Lossy mappings are refused unless you list them

A mapping that loses information is refused, and the instance pauses with an error that names the column. The three lossy classes are:

- unsigned 64-bit into a signed-only target,
- timestamps without a timezone,
- decimals over 38 digits.

To accept a loss, list the column's full path in the instance config. The setting applies to that exact column and no other:

```yaml
config:
  lossy: [public.orders.placed_at, public.orders.total]
```

An accepted field carries the metadata `loams.lossy`, which says what it loses.

## Evolution

`evolution` in the instance config decides which changes a running source accepts:

| `evolution` | Accepts |
|---|---|
| `additive` (default) | new tables; new **nullable** columns, including new nullable fields inside a struct |
| `widen` | `additive`, plus lossless widenings: `int32 → int64` (and every other integer widening that keeps all values), `float32 → float64`, `varchar(n) → varchar(m > n)`, decimal precision up at the same or a larger scale, `utf8 → large_utf8`, `NOT NULL → nullable` |
| `pause` | nothing: any change, including a new table, pauses the instance |

Under every setting, these changes are incompatible: a dropped column, a narrowed or retyped column, a new `NOT NULL` column, or a column made `NOT NULL`. Column order does not matter.

An accepted change is emitted as an `io.loams.dev.flow.<connector>.schema-change.v1` event. Its `subject` is the table. Its `data` is `{table, at, before: {fields}, after: {fields}}`, where `at` is the source position the new schema applies from. Its id comes from that position, so a re-read emits the same id. Routes deliver these events to `<db>.schema_changes` (§33 §7).

An incompatible change **pauses** the instance (`SchemaIncompatible`). Nothing of the new shape reaches the Fabric, and `last_error` holds the table, the reason, and the before and after schemas:

```
public.orders: incompatible schema change: column id changed type from Int32 to Utf8, which is
not a lossless widening; before: {id: Int32, name: Utf8?}; after: {id: Utf8, name: Utf8?}
```

To recover, change the target (or the instance's `evolution` or `lossy`) and resume the instance.

## Schema registries at Kafka boundaries

Avro and Protobuf at Kafka boundaries use Confluent's framing. Each record starts with a magic byte `0` and the schema id as a big-endian `u32`. For Protobuf, the message-index path follows as zigzag varints, with `[0]` written as one `0` byte. The SDK frames and unframes records (`schema::registry::{frame, unframe, frame_protobuf, unframe_protobuf}`) and defines the `SchemaRegistry` client trait, using the topic-name subject strategy (`<topic>-value`, `<topic>-key`). Until Q344 settles the registry, the in-memory `MemorySchemaRegistry` is the stub.
