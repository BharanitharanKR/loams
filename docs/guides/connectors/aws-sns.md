# Amazon SNS

The `aws-sns` connector publishes to an SNS topic, and receives a topic's messages through an HTTPS subscription. It is first-party Rust in `loams_connector_cloud::sns` (CN2 Task 19), over SNS's Query protocol with SigV4 (plan ruling R19.1).

## Sink

- **Batches.** `PublishBatch` sends ten messages per call. Only the entries SNS failed on its side are retried. An entry SNS refuses goes to the DLQ.
- **Attributes.** The event's CloudEvents attributes travel as `ce_*` message attributes, with the data as the message, in the same binary or structured mode as [SQS](aws-sqs.md). SQS subscribers with raw delivery therefore see the same attributes, and Loams' SQS source passes the event through either way.
- **FIFO topics.** A FIFO topic's deduplication id is the event's `ce_id`, and its group id is `message_group_id`, else the subject.

## Source: an HTTPS subscription

SNS pushes to an HTTPS endpoint, the instance's route on the webhooks source. CN2 Task 8 wires the route, and `SnsFactory::webhook(instance)` hands out the handler. Every message is checked before anything else happens:

- **Topic.** `TopicArn` must be the instance's `topic_arn`. Any other topic is refused with `403`.
- **Certificate URL.** `SigningCertURL` must be HTTPS on SNS's own host, `sns.<region>.amazonaws.com` or `.amazonaws.com.cn`, and end in `.pem`. The certificate is fetched once through the egress-checked client and cached.
- **Signature.** The signature must verify over SNS's string to sign: SHA1withRSA for `SignatureVersion` 1 and SHA256withRSA for 2.

After the checks:

- A `SubscriptionConfirmation` is confirmed by visiting its `SubscribeURL`, which must also be SNS's.
- A `Notification` becomes one event. Its `ce_*` attributes or a structured body pass through; otherwise it becomes `io.loams.dev.flow.aws-sns.message.v1`, with SNS's message id, its `Subject` as subject and its `Timestamp` as time.
- The request is answered `200` only after the Fabric acknowledges, and `503` with `Retry-After` when the source is busy or stopping. SNS's delivery policy then retries.

Until Task 28 opens remote ingress (Q30), SNS cannot reach Loams. Until then the push source runs on requests signed with a test-only key in CI.

**Fan-out instead of push.** Subscribe an SQS queue to the topic and read the queue with the `aws-sqs` connector. That works today, with SQS's delivery guarantees, and is what the floci and nightly tests exercise.

## Config

| Key | |
|---|---|
| `topic_arn` | required |
| `region`, `endpoint`, `auth.*` | as for [SQS](aws-sqs.md) |
| `message_group_id` | FIFO sink only |

## Tested against

- floci 2.1.0 (`connectors-p2`) and real SNS (`connectors-nightly`), for publish → SQS fan-out → SQS source.
- The signature and confirmation handshake on locally signed messages: `sns_signature_and_subscription_confirmation`.
- `sns_publish_sink_maps_ce_attributes_and_retries_failed_entries` and `sns_to_sqs_envelope_passes_through`.

As of Task 2 the manifest declares the sink direction only. The source is declared when the webhooks route is wired (R19.6).
