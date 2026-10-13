# Protobuf

The Protobuf codec decodes messages of one type into Arrow record batches and encodes batches back to messages. It works from a `FileDescriptorSet` that the user supplies, so it needs no generated code and no protoc at run time (OR-12). Built in CN2 Task 5 as `loams_connector_sdk::codecs::protobuf::ProtobufCodec`, on `prost-reflect` 0.16.5.

## Supplying the descriptor

Build a descriptor set with its imports and give the codec the full message name:

```sh
protoc --include_imports --descriptor_set_out=shop.binpb shop.proto
# or: buf build -o shop.binpb
```

```rust
let codec = ProtobufCodec::new(&std::fs::read("shop.binpb")?, "shop.Order")?;
```

## Framing

The codec reads and writes three framings:

- **One message per record** (Kafka values, Pub/Sub messages): `decode_messages` and `encode_messages`.
- **A length-delimited stream** (a varint length before each message, the usual file framing): the `Codec` methods `decode`, `encode` and `decode_bulk`.
- **Confluent framing at Kafka boundaries**: `schema::registry::unframe_protobuf` first, then `decode_messages` on the payload (see [Schema mapping](schema-mapping.md)).

## Types

| Protobuf | Arrow |
|---|---|
| `double`, `float` | `Float64`, `Float32` |
| `int32`, `sint32`, `sfixed32` | `Int32` |
| `int64`, `sint64`, `sfixed64` | `Int64` |
| `uint32`, `fixed32` | `UInt32` |
| `uint64`, `fixed64` | `UInt64` |
| `bool`, `string`, `bytes` | `Boolean`, `Utf8`, `Binary` |
| an enum | `Int32` (the number), with field metadata `loams.proto.enum` set to the enum's full name |
| a message | `Struct` |
| `repeated T` | `List<T>`, non-null items |
| `map<K, V>` | `Map<K, V>`, entries in key order |

How the mapping treats a few cases:

- **Nullability.** A field is nullable exactly when it has presence: a message field, an `optional` scalar (proto2 or proto3), or a oneof member. An absent value is null, and an explicit zero stays a zero. A proto3 scalar without presence is never null.
- **Enums.** An enum is kept as its number, so a number the descriptor does not know survives a round trip.
- **Not mapped.** Well-known types such as `google.protobuf.Timestamp` map as the plain messages they are (`Struct{seconds, nanos}`). Recursive message types have no Arrow schema and are refused.

Bytes that are not a message of the type are a decode error, which becomes `Poison` and sends the record to the DLQ.

## Registry status

The `protobuf` manifest is still the generated `planned` stub on Camel. CN2 Task 2's runtime assignment moves it to `native` (see the CN2 plan's ruling R5.6).
