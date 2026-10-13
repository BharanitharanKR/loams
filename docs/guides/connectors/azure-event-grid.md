# Azure Event Grid

The `azure-event-grid` connector publishes CloudEvents to Event Grid, and receives Event Grid deliveries by webhook or by pull. It is first-party Rust in `loams_connector_cloud::event_grid` (CN2 Task 19), over Event Grid's HTTP API.

## Sink

The sink posts batches of up to 1 MB in the CloudEvents schema (`application/cloudevents-batch+json`). It can publish to:

- **A topic `endpoint`.** The auth is the topic's access key (`auth.key`, sent as `aeg-sas-key`) or Entra ID: `auth.tenant_id`, plus `auth.client_id` and `auth.client_secret` naming secrets, with the client-credentials grant for `https://eventgrid.azure.net/.default`. `auth.authority` overrides `https://login.microsoftonline.com` for sovereign clouds.
- **A namespace topic** (`namespace_endpoint` and `topic`), with `Authorization: SharedAccessKey`.

Event Grid takes a batch whole. A refused batch (`400`, `413`) goes back to the pipeline, which isolates the offending event to the DLQ.

## Source: webhook (`delivery: webhook`, the default)

Event Grid pushes to the instance's route on the webhooks source. CN2 Task 8 wires the route, and `EventGridFactory::webhook(instance)` hands out the handler.

- **Validation handshakes.**
  - The Event Grid schema's `Microsoft.EventGrid.SubscriptionValidationEvent` is answered with `{"validationResponse": <code>}`.
  - The CloudEvents schema's `OPTIONS` abuse-protection request is answered with `WebHook-Allowed-Origin` and `WebHook-Allowed-Rate`.
- **Events.**
  - CloudEvents (one, or a batch) pass through with their identity.
  - Event Grid schema events become `io.loams.dev.flow.azure-event-grid.event.v1`, with Event Grid's id, the subject, `eventTime` as time, and the whole event as data.
  - A malformed delivery is answered `400` and never half-delivered.
- **Acknowledgement.** A delivery is answered `200` only after the Fabric acknowledges it, and `503` with `Retry-After` when the source is busy, so Event Grid retries.
- **Webhook key.** With `auth.webhook_key` naming a secret, a request must carry it as the `code` query parameter. Put it in the subscription's endpoint URL. It is compared in constant time.

Until Task 28 opens remote ingress (Q30), Azure cannot reach Loams. Until then the webhook source runs on requests shaped like Event Grid's in CI, and on fixtures recorded from Event Grid once they exist (Task 6's `FixtureProxy`).

## Source: pull (`delivery: pull`)

The source reads a namespace topic's event subscription:

- `receive` takes up to 100 events, waiting `max_wait_seconds`.
- `renewLock` runs at half of `lock_seconds` while an event waits for the Fabric.
- `acknowledge` runs after the Fabric's ack.
- `release` runs on stop.

This works without remote ingress, and it is the queue the object-storage family can read Azure Blob notifications through (plan ruling R19.4).

## Config

| Key | |
|---|---|
| `endpoint` | sink, a topic endpoint |
| `namespace_endpoint`, `topic` | sink or pull source |
| `subscription` | pull source |
| `delivery` | `webhook` (default) or `pull` |
| `auth.key` | the name of a secret |
| `auth.tenant_id`, `auth.client_id`, `auth.client_secret`, `auth.authority` | Entra ID |
| `auth.webhook_key` | the name of a secret |
| `lock_seconds` | 1–300, default 60 |
| `max_wait_seconds` | 0–60, default 10 |

## Tested against

- A real topic in `connectors-nightly`, for the sink. It uses `LOAMS_EVENTGRID_ENDPOINT` and `LOAMS_EVENTGRID_KEY`.
- `eventgrid_validation_handshake`, `eventgrid_cloudevents_pass_through`, `eventgrid_sink_posts_cloudevents_batches` and `eventgrid_namespace_pull_acks_after_fabric`, on requests and services shaped like Event Grid's.
- No fixture has been recorded yet (R13.5's precedent: none is fabricated). The webhook source cannot reach `preview` before one is.
