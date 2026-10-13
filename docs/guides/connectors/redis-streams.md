# Redis Streams

The Redis Streams connector reads streams through a consumer group and writes events to a stream with `XADD`. It is served by `loams-connector-kv`'s Redis implementation (CN2 Task 14), on the `redis` crate 1.7.1, RESP2.

**Suites run against:**

- Valkey 8.1.10: compose service `valkey`, profile `connectors-p2`.
- Redis 8.10.1: compose service `redis`, profile `connectors-ci-only`. Redis 8's tri-licence makes it a `ci-service` (OR-3), so it runs in CI and never in an image Loams builds.

**Narrowings:** none. **Fallback:** none.

## Source

There is one split per stream (`stream`, plus any `streams`).

- **Reading.** The source reads with `XREADGROUP GROUP <consumer_group> <consumer_name> COUNT <count> BLOCK <block_ms> STREAMS <stream> >`.
  - The group defaults to `loams-<instance>` and is created with `MKSTREAM` from `start_id` (`0` by default).
  - `block_ms` is capped at 30 s.
- **Acknowledgement.** An entry is acknowledged with `XACK` only after the Fabric has acknowledged its batch (`Source::committed`). Until then it stays in the group's pending list. The test `redis_streams_group_ack_after_fabric` checks this on both Valkey and Redis.
- **Restart.** The source first re-reads its own pending entries (`XREADGROUP … 0`):
  - Entries at or before the checkpointed position are acknowledged and not sent again.
  - Later entries are sent again with the same ids, and `ingest` drops the copies.
- **Idle consumers.** With `pending_idle_ms`, `XAUTOCLAIM` takes over entries that another consumer left pending for that long.
- **`ack_mode: on-read`.** This mode uses `XREADGROUP … NOACK`, which makes the source at most once.

Each entry becomes an event:

- **Pass-through.** If the entry is a CloudEvent in binary mode (`ce_id`, `ce_source`, `ce_type`, `ce_specversion`, with the payload under `data`), it keeps its type, source and id (§33 §6).
- **Other entries.** Any other entry is an `io.loams.dev.flow.redis-streams.stream-entry.v1` event:
  - Its id is `<stream>/<entry-id>`.
  - `time` comes from the entry id.
  - Its data is a JSON object of the entry's fields.

The source reports lag as the age of the last entry read.

## Sink

The sink sends `XADD <stream> [MAXLEN ~ max_len] *` with the event in binary content mode: `ce_id`, `ce_source`, `ce_type`, `ce_specversion`, optional `ce_subject`, `ce_time` and `content-type`, `ce_<extension>` for each extension, and the payload in `data`. The Streams source then passes these entries through with their identity.

A replayed batch appends again, so the sink is `at_least_once`.

## Settings and auth

| Setting | Meaning |
|---|---|
| `url` | `redis://` or `rediss://`, with no credentials in the URL. |
| `user` | The ACL user. |
| `password` | The name of a secret. |
| `database` | The logical database. |
| `stream`, `streams` | The streams to read. The sink writes to `stream`. |
| `consumer_group`, `consumer_name` | The group and the consumer within it. |
| `start_id` | Where a new group starts. |
| `ack_mode` | `after-ack` (the default) or `on-read`. |
| `block_ms`, `count` | How long a read blocks, and how many entries it takes. |
| `pending_idle_ms` | When to claim an idle consumer's entries. |
| `max_len` | The sink's approximate trim length. |

The `auth` suite runs `none`, and `basic` as an ACL user.

## Registry status

`connectors/registry/redis-streams.yaml` is still the generated `planned` stub. The manifest this connector ships is `fabric/crates/loams-connector-kv/manifests/redis-streams.yaml`. CN2 Task 2 installs it (ruling R14.1).
