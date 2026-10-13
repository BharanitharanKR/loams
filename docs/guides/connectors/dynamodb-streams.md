# DynamoDB Streams

The DynamoDB Streams connector is a native CDC source (CN2 OR-9) built in CN2 Task 14 in `loams-connector-kv` on `aws-sdk-dynamodbstreams` 1.115.0 (Apache-2.0).

**Suites run against:**

- floci 2.1.0 (MIT): compose service `floci`, profile `connectors-p2` (R0.23). DynamoDB Local is not used: R0.23 keeps it only for the case where floci's shard lineage fails a test.
- Amazon DynamoDB Streams: the `connectors-nightly` job.

**Narrowings:** none. **Fallback:** none.

## How it reads

- **Splits are shards, in lineage order.** `DescribeStream` lists the shards with their parents. A shard is released only when one of these holds:
  - it has no parent;
  - its parent is no longer listed, because it was trimmed;
  - its parent was read to its end.

  So a key's changes stay in order across a shard split. A finished shard stays a split while the stream lists it, so its position, and the release of its children, survive a restart.
- **Positions are sequence numbers.** A shard is first read from `TRIM_HORIZON`. With `start: latest`, a root shard with no checkpoint starts from `LATEST` instead. After a restart a shard is read from `AFTER_SEQUENCE_NUMBER` at its checkpoint, so nothing the Fabric acknowledged is read again.
- **Iterators.** An iterator that expired (after 15 min) is opened again at the same place.
- **Trimmed checkpoints.** A checkpoint whose records were trimmed, past the 24-hour retention, is `PositionLost`. The instance stops with a re-snapshot hint: re-read the table with the [DynamoDB](dynamodb.md) source, then restart this one.

## Events

Each record is an `io.loams.dev.flow.dynamodb-streams.change.v1` event:

| Attribute | Value |
|---|---|
| `id` | `<shard>/<sequence-number>`, stable across re-reads |
| `loamsop` | `c` for `INSERT`, `u` for `MODIFY`, `d` for `REMOVE` |
| `loamslsn` | the sequence number |
| `subject` | the item's key as JSON |
| `time` | `ApproximateCreationDateTime` |
| data | `{"keys", "new", "old"}`, as the stream's view type provides them, in plain JSON (see the DynamoDB guide's type table) |

The source reports two metrics through its observer:

- **Lag:** the age of the newest record read.
- **Position risk:** the checkpoint's age against the 24-hour retention.

DynamoDB has no DDL. The `cdc` suite's DDL step is an item that gains a new attribute (ruling R14.6).

## Settings and auth

| Setting | Meaning |
|---|---|
| `stream_arn` or `table` | The stream to read. With `table`, its `LatestStreamArn` is used. |
| `region`, `endpoint_url` | Where the stream lives. |
| `access_key_id`, `secret_access_key`, `session_token` | Secret names. |
| `start` | `trim_horizon` or `latest`. |
| `poll_interval_ms` | The wait after an empty read (default 1 s). |

Requests are signed with SigV4 and sent through the SDK's egress-checked HTTP client.

## Registry status

`connectors/registry/dynamodb-streams.yaml` is the hand-written manifest this connector ships, with its config schema `connectors/schemas/dynamodb-streams.config.json` (ruling R14.1). It stays `planned` until the family's suites are green in CI (OR-23).
