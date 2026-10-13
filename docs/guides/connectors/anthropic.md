# Anthropic

Anthropic runs on the native protocols family (`runtime.ref:
loams_connector_protocols::anthropic`; CN2 Task 21) as a request-reply sink (`sink.reply`,
OR-19) over the Messages API, `POST /v1/messages` with `anthropic-version: 2023-06-01`,
through the SDK's egress-checked client.

| | |
|---|---|
| Tested against | synthetic fixtures of the documented shapes (`conformance/connectors/fixtures/anthropic/`); the real API nightly with `LOAMS_ANTHROPIC_API_KEY` |
| Delivery | at least once (each event is one call; reply ids are stable) |
| Auth | `key` (`x-api-key`), by secret name |

## Requests

`model` and `max_tokens` are required. `prompt.system` becomes `system` and `prompt.user` the
one user message, both templates filled from the event (`{{/field}}`, `{{data}}`,
`{{subject}}`, `{{id}}`). `params` adds fixed fields (`output_config`, `thinking`, …) but may
not set `model`, `messages`, `system`, `max_tokens` or `stream`.

## Replies

`io.loams.dev.flow.anthropic.reply.v1`, keeping the input's `subject`, with data `{provider,
model, id, text, stop_reason, usage}`; `text` joins the answer's text blocks. Check
`stop_reason` downstream: a `refusal` is a reply too. The key is never in a reply.

## Rate limits and errors

429 is `Throttled` with `retry-after`; 529 (`overloaded_error`) and 5xx are `Retryable`. When
an `anthropic-ratelimit-*-remaining` header reaches 0, every later request of the instance
waits until its `*-reset` time (RFC 3339). 401/403 are `Auth`; another 4xx is `Poison`.

```yaml
model: claude-opus-5-5
api_key: anthropic-key
max_tokens: 1024
prompt: { system: "You label support tickets.", user: "Label: {{/text}}" }
reply_topic: tickets.labels
```
