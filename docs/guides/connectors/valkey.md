# Valkey

Valkey is served by the Redis implementation in `loams-connector-kv` under the `valkey` target profile (`connectors/profiles/valkey.yaml`, CN2 OR-2). The code is the same as the Redis ★ connector's. The suites run against real Valkey, never against Redis. Built in CN2 Task 14 on the `redis` crate 1.7.1 (BSD-3-Clause), RESP2.

**Suites run against:** Valkey 8.1.10 (`valkey/valkey`, BSD-3-Clause), compose service `valkey` in the `connectors-p2` profile. **Narrowings:** none. Valkey 8 serves every capability the manifest declares. **Fallback:** none.

## Source

The source has two modes. `source_mode: keys` is the default. `source_mode: stream` is used when you set it, or whenever `stream` is configured.

- **Keyspace (`keys`).** The source runs `SCAN cursor MATCH <scan_match> COUNT <scan_count>`, then reads each key by its type, pipelined:

  | Type | Read with | `value` in the event |
  |---|---|---|
  | string | `GET` | a string |
  | hash | `HGETALL` | an object, fields sorted |
  | list | `LRANGE 0 -1` | an array |
  | set | `SMEMBERS` | a sorted array |
  | zset | `ZRANGE 0 -1 WITHSCORES` | an array of `[member, score]` |

  Streams and module types are skipped, and so is a key deleted between `SCAN` and its read. Bytes that are not UTF-8 become `{"$base64": "…"}`.

  - Each key is one `io.loams.dev.flow.valkey.document.v1` event, with `subject` set to the key and data `{"key", "type", "value"}`.
  - Its id is `<db>/<key>@<digest of the value>`. A key read again unchanged keeps its id, and `ingest` drops the copy. A changed key gets a new id.
  - There is one split, `keyspace`. Its position is the `SCAN` cursor of the page being read, and a restart re-reads at most that page.
  - Without `poll_interval_ms` the keyspace is read once. With it, a new pass starts that long after the last one finished.

- **Streams (`stream`).** Streams mode works as in [Redis Streams](redis-streams.md): `XREADGROUP` in the group `loams-<instance>`, with `XACK` sent only after the Fabric has acknowledged the batch.

## Sink

`sink_mode` is `xadd` when `stream` is set, and `set` otherwise.

| Mode | Write | A `loamsop = d` event |
|---|---|---|
| `set` | `SET <key> <data> [EX ttl_seconds]` | `DEL <key>` |
| `hset` | `HSET <key> <field> <data>` with `field_template`, or the data's JSON object as the hash | `HDEL` the field, or `DEL` the key |
| `xadd` | `XADD <stream> [MAXLEN ~ max_len] *` with the event in CloudEvents binary mode | — |
| `publish` | `PUBLISH <channel>` with the event as structured JSON | — |

- The key is `key` (or CN1's `key_template`), a template with `{subject}`, `{ce_id}`, `{type}`, `{source}` or `{<extension>}`. The default is `{subject}`.
- Each batch is one pipeline. Every event is checked before anything is sent: an event that cannot be written, such as one with no subject for `{subject}`, fails as `Poison` and goes to the instance's DLQ.
- **Delivery.** The `set` and `hset` modes are idempotent: a replay leaves one value per key. The manifest declares the sink `at_least_once`, because `xadd` appends again on a replay. The source is `effectively_once`, because its ids are stable.

## Settings and auth

| Setting | Meaning |
|---|---|
| `url` | `redis://host[:port][/db]`, or `rediss://` for TLS with the webpki roots. User information in the URL is refused. |
| `user` | The ACL user. |
| `password` | The **name** of a secret in the namespace's store, never the password itself. |
| `database` | The logical database. |

Every connection is opened by `sdk::net`, which egress-checks the address after DNS. Client certificates (`mtls`) are not built yet (ruling R14.7). The `auth` suite runs `none` and `basic`: it creates an ACL user limited to the harness's key prefix and connects as that user.

## Registry status

`connectors/registry/valkey.yaml` is the hand-written manifest this connector ships, with its config schema `connectors/schemas/valkey.config.json` (ruling R14.1). It stays `planned` until the family's suites are green in CI (OR-23).
