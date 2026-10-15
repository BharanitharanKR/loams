# Amazon SQS

The `aws-sqs` connector reads from and writes to an SQS queue, standard or FIFO. It is first-party Rust in `loams_connector_cloud::sqs` (CN2 Task 19). It speaks the SQS JSON protocol with SigV4 through the connector SDK's HTTP client, so the egress guard checks every connection after DNS. No AWS SDK is linked (plan ruling R19.1).

## Source

- **Long polling.** `ReceiveMessage` waits up to `wait_time_seconds` (default 20) for up to ten messages per receive loop. `receivers` (default 1) runs several loops on the same queue.
- **Ack after the Fabric.** A received message stays invisible for `visibility_timeout_seconds` (default 30). While its event waits for the Fabric, the source renews that timeout at half its length (`ChangeMessageVisibilityBatch`). It deletes the message (`DeleteMessageBatch`) only after the Fabric acknowledges the event.
- **On stop.** A stopping source hands back what it holds by setting the visibility timeout to 0.
- **Crashes.** After a crash, SQS redelivers the message after its timeout. The event id is the SQS message id, so `ingest` drops the second copy. The source is therefore effectively once.
- **Event types.**
  - A message whose `ce_*` message attributes form a CloudEvent passes through with its `type`, `source` and `id`.
  - So does a body in the structured CloudEvents form.
  - So does an SNS notification envelope (SNS → SQS without raw delivery), unwrapped.
  - Any other message becomes `io.loams.dev.flow.aws-sqs.message.v1`. Its data is the body, its time is `SentTimestamp`, its subject is the FIFO group id, and attributes whose names are valid CloudEvents extension names become extensions.
- **Oversized messages.** A message larger than a whole batch is reported to the DLQ and left unacknowledged. The queue's redrive policy then moves it after `maxReceiveCount`, so configure one.

## Sink

- **Batches.** `SendMessageBatch` sends up to ten messages or 256 KiB per call. Only the entries SQS failed transiently are retried. An entry SQS refuses goes to the DLQ.
- **Binary mode.** The data travels as the body, with `ce_id`, `ce_source`, `ce_type`, `ce_specversion`, `ce_subject`, `ce_time`, each extension and `content-type` as message attributes.
- **Structured mode.** An event that needs more than SQS's ten attributes, or whose data is not text, is sent in the structured JSON form with `content-type: application/cloudevents+json`. The source reads both modes.
- **FIFO queues** (`….fifo`):
  - `MessageDeduplicationId` is the event's `ce_id`, or its SHA-256 when SQS would refuse the id, so a retried batch is enqueued once within SQS's five-minute window. That makes the sink `effectively_once`.
  - `MessageGroupId` is `message_group_id`, else the subject, else `loams`.

## Config

| Key | |
|---|---|
| `queue_url` | required |
| `region` | required unless `endpoint` is set |
| `endpoint` | an emulator or VPC endpoint, such as floci's `http://floci:4566` |
| `auth.access_key_id`, `auth.secret_access_key`, `auth.session_token` | names of secrets, write-only. They are required without `endpoint`. With an emulator `endpoint` and no `auth`, requests are signed with a placeholder key (R19.3) |
| `wait_time_seconds` | 0–20, default 20 |
| `visibility_timeout_seconds` | 1–43,200, default 30 |
| `receivers` | 1–64, default 1 |
| `message_group_id` | FIFO sink only |

## Tested against

- floci 2.1.0 in `connectors-p2`.
- Real SQS in `connectors-nightly`.
- Every PR also runs a fake SQS in process, which never counts toward `preview`.

The tests are `sqs_delete_after_ack`, `sqs_visibility_extended_while_in_flight`, `sqs_fifo_dedup_id_is_ce_id` and `sqs_roundtrip_keeps_cloudevent_identity`. There is no fallback runtime (Task 2).
