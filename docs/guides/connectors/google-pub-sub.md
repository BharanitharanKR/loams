# Google Cloud Pub/Sub

The `google-pub-sub` connector reads a subscription and publishes to a topic. It is first-party Rust in `loams_connector_cloud::pubsub` (CN2 Task 19). It speaks the Pub/Sub REST API (`pubsub.googleapis.com/v1`) through the SDK's egress-checked client. Plan ruling R19.1 records why it uses REST and not the `google-cloud-pubsub` crate's gRPC `StreamingPull`.

## Source

- **Pull.** `pull` takes up to `max_messages` (default 100) per receive loop.
- **Ack after the Fabric.** Each message's ack deadline is extended (`modifyAckDeadline`) at half of `ack_deadline_seconds` (default 60) while its event waits for the Fabric. It is acknowledged only after the Fabric's ack.
- **On stop.** A stopping source hands its messages back with a deadline of 0.
- **Crashes.** Event ids are Pub/Sub message ids, so a redelivery after a crash is dropped by `ingest`.
- **Event types.**
  - A message in the CloudEvents Pub/Sub binding (`ce-id`, `ce-source`, `ce-type`, `ce-specversion` and the other `ce-*` attributes, with `content-type`) passes through with its identity and extensions.
  - So does a structured body.
  - Any other message is `io.loams.dev.flow.google-pub-sub.message.v1`, with `publishTime` as time, its ordering key as subject, and its attributes as extensions where their names are valid.
- **Exactly-once subscriptions.** An acknowledgement whose ack id expired is logged, not retried. The message comes back and is deduplicated by id.

## Sink

`publish` sends up to 1,000 messages per call in the binding's binary mode: `ce-*` attributes, with the data as the message data. An event with an attribute over Pub/Sub's 1,024-byte value limit is published in the structured mode. With `ordering_key: subject`, each message's ordering key is the event's subject. Publishing is all or nothing per call, so a refused call goes back to the pipeline, which isolates the event that caused it.

## Auth

- `auth.credentials_json` names the secret that holds a service account key file. The connector signs a JWT with the key (RS256) and exchanges it for an access token at the key's `token_uri`. It caches the token until a minute before it expires, and drops it after a `401`.
- Without `auth`, an `endpoint` is required: the emulator takes no credentials.

## Config

| Key | |
|---|---|
| `project` | required |
| `subscription` | source |
| `topic` | sink |
| `endpoint` | the emulator, `http://pubsub:8085`. Default `https://pubsub.googleapis.com` |
| `auth.credentials_json` | the name of a secret, write-only |
| `ack_deadline_seconds` | 1–600, default 60 |
| `max_messages` | 1–1,000, default 100 |
| `receivers` | 1–64, default 1 |
| `ordering_key` | `subject` or `none` (default) |

## Tested against

- **The Pub/Sub emulator** in `connectors-ci-only`. It is treated as a `ci-service` because its jar's licence is unverified (R0.27).
- **A real project** in `connectors-nightly`. Exactly-once delivery and IAM run only there, because the emulator's exactly-once support is unverified (spike §5.10).
- `pubsub_ack_after_fabric_and_deadline_extension`, `pubsub_ce_attributes_pass_through` and `service_account_token_is_a_signed_jwt_exchange`, on a fake service.
