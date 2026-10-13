# OpenAI

OpenAI runs on the native protocols family (`runtime.ref:
loams_connector_protocols::openai`; CN2 Task 21) as a request-reply sink (`sink.reply`, OR-19)
over the public REST API, `POST /v1/chat/completions`, through the SDK's egress-checked client.

| | |
|---|---|
| Tested against | synthetic fixtures of the documented shapes (`conformance/connectors/fixtures/openai/`); the real API nightly with `LOAMS_OPENAI_API_KEY` |
| Delivery | at least once (each event is one call; reply ids are stable, so `ingest` drops a repeated reply) |
| Auth | `key` (`Authorization: Bearer`), by secret name |

## Requests

`prompt.system` and `prompt.user` are templates filled from the event (`{{/field}}`,
`{{data}}`, `{{subject}}`, `{{id}}`); a placeholder with nothing to fill is `Poison`, so a
typo never sends an empty prompt. `max_tokens` is sent as `max_completion_tokens`. `params`
adds fixed fields (`temperature`, `response_format`, …) but may not set what the connector
sets. `base_url` points at a gateway.

## Replies

`io.loams.dev.flow.openai.reply.v1`, keeping the input's `subject`, with data `{provider,
model, id, text, stop_reason, usage}` built from the answer only: the key is never in a reply,
and a reply containing it is refused (`openai_reply_has_no_key_and_keeps_subject`).

## Rate limits

429 is `Throttled` with `Retry-After`. When `x-ratelimit-remaining-requests` or
`-tokens` reaches 0, every later request of the instance waits for
`x-ratelimit-reset-requests`/`-tokens` (`1s`, `6m0s`, `20ms`). 5xx is `Retryable`; another 4xx
(a prompt too long) is `Poison`.

```yaml
model: gpt-4o-mini
api_key: openai-key
max_tokens: 256
prompt: { system: "You label support tickets.", user: "Label: {{/text}}" }
reply_topic: tickets.labels
```
