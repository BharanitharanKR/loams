# DynamoDB

The DynamoDB connector reads a table with a parallel `Scan` and writes with `BatchWriteItem`. It is built in CN2 Task 14 in `loams-connector-kv` on `aws-sdk-dynamodb` 1.128.0 (Apache-2.0). For change capture, see [DynamoDB Streams](dynamodb-streams.md).

**Suites run against:**

- floci 2.1.0 (MIT): compose service `floci`, profile `connectors-p2` (R0.23).
- Amazon DynamoDB: the `connectors-nightly` job, with `LOAMS_AWS_ACCESS_KEY_ID`, `LOAMS_AWS_SECRET_ACCESS_KEY` and `LOAMS_AWS_REGION`.

**Narrowings:** none. **Fallback:** none.

## Source

- **Splits.** `segments` parallel `Scan` segments (default 4) are the splits, `segment-<i>-of-<n>`, read by the pipeline in parallel.
- **Position.** A segment's position is its `LastEvaluatedKey`, stored as DynamoDB JSON, and whether the segment has finished. A restart resumes with `ExclusiveStartKey`.
- **Events.** Each item is an `io.loams.dev.flow.dynamodb.document.v1` event. Its id is `<table>/<digest of the item>`: an unchanged item read again is dropped by `ingest`, and a changed one is a new event.
- **Passes.** Without `poll_interval_ms` the table is scanned once. With it, a new scan starts that long after the last one finished.

## Sink

- **Writes.** Events are written with `BatchWriteItem`, at most 25 writes per call. A key appears at most once per call, so repeated writes to one key keep their order. A `loamsop = d` event becomes a `DeleteRequest`.
- **`UnprocessedItems`.** Writes the service leaves unprocessed are retried with backoff, starting at 50 ms, doubling, and capped at 5 s. If writes are still unprocessed after `max_unprocessed_retries` (default 8), the batch is `Throttled` and retried whole, which is safe because puts are idempotent.
- **`version_attribute`.** With this setting, every put is a conditional `PutItem` (`attribute_not_exists(#v) OR #v < :v`). A replay or an older event never overwrites a newer item.
- **Key.** The key attributes come from `DescribeTable`, or from `key`.
- **Delivery.** The sink is `effectively_once`.
- **Poison.** An event whose data is not a JSON object, or that lacks a key attribute, is `Poison` and goes to the DLQ.

## Types

`item_format: json` is the default. It renders items as plain JSON:

| DynamoDB | JSON |
|---|---|
| `S` | string |
| `N` | number, or a string when the number does not survive as a JSON number |
| `B` | base64 string |
| `BOOL` | boolean |
| `NULL` | `null` |
| `L` | array |
| `M` | object |
| `SS`, `NS`, `BS` | sorted array |

Sets come back as lists. `item_format: dynamodb-json` uses DynamoDB JSON (`{"id": {"S": "a"}}`) in both directions, which is lossless.

## Settings and auth

| Setting | Meaning |
|---|---|
| `table` | The table. |
| `region` | The AWS region. |
| `endpoint_url` | An emulator or a VPC endpoint. |
| `access_key_id`, `secret_access_key`, `session_token` | Secret **names**, never the keys themselves. |
| `segments`, `page_size`, `consistent_read`, `poll_interval_ms` | How the source scans. |
| `item_format` | `json` or `dynamodb-json`. |
| `version_attribute`, `key`, `max_unprocessed_retries` | How the sink writes. |

Requests are signed with SigV4 (`iam`) and sent through the SDK's egress-checked HTTP client, with rustls and ring (ruling R14.3). Instance roles and web identity are not built.

## Registry status

`connectors/registry/dynamodb.yaml` is the hand-written manifest this connector ships, with its config schema `connectors/schemas/dynamodb.config.json` (ruling R14.1). It stays `planned` until the family's suites are green in CI (OR-23).
