![Loams — Your data. Your bucket.](../../docs/assets/loams-banner.svg)

# `connectors/routes/` — the import routes

The paths that carry data **into** Loams' own collaboration applications (CN1
Task 15, D628), on native runtimes (CN2 Task 20, OR-11). They replace the Camel
route templates that were under `connect/routes/templates/`: the sources are
now HTTP-API profiles run by `loams-connector-http`'s profile engine, and the
targets are the native Zulip, ItsPlane and Forgejo connectors of CN1 Task 15.

Until FL3's route format exists, a route is a **pair of instances joined by a
Fabric topic**: each source instance writes the topic, the sink instance reads
it. The file format is `loams_connector_http::route::ImportRoute`; `{route.<name>}`
in an instance config is a deployment parameter listed under `parameters`.

| Route | Sources | Sink | Idempotence carried by |
|---|---|---|---|
| `forgejo-import` | GitHub issues | Forgejo issues | the event id (the issue's content id) as `Idempotency-Key` |
| `itsplane-import` | GitHub issues, Jira issues | ItsPlane issues | the event id as the sink's idempotency key |
| `zulip-import` | Slack messages (topic per thread) | Zulip stream messages | the event id as Zulip's `Idempotency-Key` |

## Rules every route keeps

1. **Projection first** (D356). Each source instance carries a `projection` map in
   its config (`loams_connector_http::route::Projection`: JSON pointers,
   `|` alternatives, `*` over arrays, `{/pointer}` templates, constants), so only
   the fields the target needs are written to the topic. The event id stays the
   raw item's, so a projection change never re-imports anything.
2. **Rate limits are upstream facts.** Zulip: 200 requests/60 s shared across every
   endpoint; the sink self-throttles from `X-RateLimit-*` and stops at
   `rate_limit_floor`. ItsPlane: 100 requests/s per key with no remaining-budget
   header, so the sink budgets. Forgejo: no upstream limit and `limit` silently
   clamped to 50, so the sink's own concurrency is the only brake.
3. **Separate write credentials.** Forgejo derives a token's scope from the HTTP
   method, so the sink's write token is a different secret from any read token.
   Secrets are named, never inlined; they resolve through the namespace's secret
   store.

The routes' source halves are tested by replaying the GitHub, Jira and Slack
fixtures through the route's own source config (`github_to_forgejo_import_replays`,
`jira_to_itsplane_import_replays`, `slack_to_zulip_import_replays` in
`fabric/crates/loams-connector-http/tests/saas.rs`). The sink halves run when the
native targets of CN1 Task 15 exist (CN2 R20.11).
