# Amazon Kinesis Data Streams

The `kinesis` ★ connector reads from and writes to a Kinesis data stream. It has CN1 Task 13's semantics, built on the connector SDK in the cloud family as CN2 Task 2's table assigns: `loams_connector_cloud::kinesis`, over AWS JSON 1.1 with SigV4 (plan ruling R19.1).

## Source

- **Shards are splits.** `ListShards` is re-run on the discovery timer.
- **Lineage.** A shard is read only when every parent (`ParentShardId`, `AdjacentParentShardId`) is finished or has expired from the listing. After a split or a merge, all of the parent's records therefore come before the children's.
- **Positions.**
  - A shard's position is its last sequence number plus a "finished" flag. A restart resumes with `AFTER_SEQUENCE_NUMBER`.
  - An expired iterator is renewed from the position.
  - A shard that disappears before it was finished is `PositionLost`, with a re-snapshot hint: its records past the position were trimmed by retention.
- **Polling.** `poll_interval_ms` (default 1000) paces empty polls, keeping each shard under Kinesis' five `GetRecords` calls a second.
- **Event ids** are `<stream>/<shard id>/<sequence number>`, stable across re-reads, so the source is effectively once into `ingest`.
- **Event types.**
  - A record that is a structured CloudEvent (as the sink writes) passes through.
  - Any other record is `io.loams.dev.flow.kinesis.record.v1`. Its subject is the partition key and its time is `ApproximateArrivalTimestamp`.

## Sink

`PutRecords` sends up to 500 records or 5 MiB per call. Each record is the event in the structured CloudEvents form, with its subject (else its id) as partition key; a key over 256 characters is hashed. Kinesis answers per record: only the failed records are retried, and `ProvisionedThroughputExceededException` backs off.

## Config

| Key | |
|---|---|
| `stream` or `stream_arn` | required |
| `start` | `trim_horizon` (default) or `latest`, for shards without a checkpoint |
| `poll_interval_ms` | 0–60,000, default 1000 |
| `region`, `endpoint`, `auth.*` | as for [SQS](aws-sqs.md) |

## Tested against

- floci 2.1.0 (`connectors-p2`) and real Kinesis (`connectors-nightly`): a round trip, and a reshard with `UpdateShardCount` read across parents and children.
- `kinesis_resharding_followed`, against a fake with a split and a merge, an expired iterator and a restart from the checkpoint.
- `kinesis_put_records_partial_failure`.
