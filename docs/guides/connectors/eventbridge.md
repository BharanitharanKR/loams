# Amazon EventBridge

The `eventbridge` connector puts events on an event bus, and reads a bus through a rule that targets an SQS queue. It is first-party Rust in `loams_connector_cloud::eventbridge` (CN2 Task 19), over AWS JSON 1.1 with SigV4 (plan ruling R19.1).

## Sink

`PutEvents` sends ten entries or 256 KiB per call to `event_bus` (a name or ARN, default `default`). Each entry carries:

| Entry field | Value |
|---|---|
| `Source` | the event's `source` |
| `DetailType` | the event's `type` |
| `Time` | the event's `time` |
| `Detail` | the whole event in the structured CloudEvents form |

Rules can therefore match on any CloudEvents attribute (`detail.id`, `detail.subject`, `detail.data.…`). EventBridge answers per entry: a throttled or failed entry is retried alone, and an entry EventBridge refuses (malformed detail, for example) goes to the DLQ with its error code.

## Source

EventBridge pushes to targets. The source is therefore a rule whose target is an SQS queue, which the instance reads with the SQS settings (see [SQS](aws-sqs.md)). The [runbook](../../runbooks/connectors/eventbridge-source.md) sets up the rule, the queue and the queue policy.

- An event whose `detail` is a CloudEvent, as the sink writes it, passes through with its `type`, `source` and `id`.
- Any other event becomes `io.loams.dev.flow.eventbridge.event.v1`. Its id is EventBridge's event id, its subject is `detail-type`, its time is `time`, and its data is the whole EventBridge event.
- Deletion after the Fabric's ack, visibility extension and redelivery are SQS's.

## Config

| Key | |
|---|---|
| `event_bus` | sink |
| `queue_url`, `wait_time_seconds`, `visibility_timeout_seconds`, `receivers` | source |
| `region`, `endpoint`, `auth.*` | as for [SQS](aws-sqs.md) |

## Tested against

- floci 2.1.0 (`connectors-p2`) and real EventBridge (`connectors-nightly`): a bus, a rule and an SQS target are created per run, events are put, and they are read back with their ids.
- `eventbridge_put_events_partial_failure` and `eventbridge_source_reads_rule_target_queue`, on fakes, in every PR.
