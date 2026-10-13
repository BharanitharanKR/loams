# Lambda, Cloud Run and Azure Functions

One manifest, three platforms, on the native protocols family (`runtime.ref:
loams_connector_protocols::functions`; CN2 Task 21). Every instance is a request-reply sink
(`sink.reply`, OR-19): one invocation per event, one reply event per answer.

| | |
|---|---|
| Tested against | floci 2.1.0 as Lambda in CI's `connectors-p2` stack; synthetic fixtures of the three documented shapes (`conformance/connectors/fixtures/lambda-cloud-run-azure-functions/`); real AWS, Cloud Run and Azure in the nightly job |
| Delivery | at least once: a retried batch invokes again; its replies keep their ids and are deduplicated by `ingest` |
| Auth | `iam` (SigV4), `sa` (an ID token), `aad` (a function key) |

## Platforms

- **Lambda** (`provider: lambda`): `POST /2015-03-31/functions/{function}/invocations`
  (`?Qualifier=`), `X-Amz-Invocation-Type: RequestResponse`, signed with SigV4 for `lambda` by
  the HTTP family's signer (ruling R21.8; no AWS SDK). `endpoint` overrides
  `https://lambda.<region>.amazonaws.com`. A function error (`X-Amz-Function-Error`) is
  `Poison`, or with `function_error: reply` a reply `{function_error, payload}`.
- **Cloud Run** (`provider: cloud_run`): `POST url` with `Authorization: Bearer <id_token>`.
  The ID token (audience: the service) is a secret the secret store mints and rotates; it is
  resolved by name.
- **Azure Functions** (`provider: azure_functions`): `POST url` with `x-functions-key`.

The request body is the event's data, or the `body` template filled from it (`{{/field}}`,
`{{data}}`, `{{subject}}`, `{{id}}`). 404 is `Fatal` (no such function), 429 `Throttled`, 5xx
`Retryable`, another 4xx `Poison`.

## Replies

A reply goes to the instance's `reply_topic`:

- `type` `io.loams.dev.flow.lambda-cloud-run-azure-functions.reply.v1`, `id` `<input id>/reply`;
- `subject` the input's subject; extensions `loamsinreplyto` and `loamsinreplytosource`;
- `data` the answer body, with its content type.

A reply never carries a credential: it is built from the answer body only, and one containing
a credential the call sent is refused. Without `reply_topic`, no replies are sent.

```yaml
provider: lambda
function: price-order
aws: { access_key_id: aws-key-id, secret_access_key: aws-secret, region: eu-west-1 }
reply_topic: orders.prices
```
