# Connector profiles

`connectors/profiles/<id>.yaml` holds the **profiles** a manifest names with
`runtime.profile` (CN2 plan, Task 7; OR-2 and OR-11). The shape is
[`connectors/schema/profile.schema.json`](../schema/profile.schema.json);
`loams-flow`'s `profile` module loads and checks them, and
`scripts/connectors/profiles.py --check` runs the same checks in CI.

There are two kinds:

- **`target`**: a protocol-compatible system served by a native family crate,
  for example Redpanda on the Kafka family. The target keeps its own manifest,
  its own suite against the real target system (never a stand-in, OR-2), and
  its own `narrows`.
- **`http-api`**: a REST API described declaratively and run by the `http`
  family's profile engine (`fabric/crates/loams-connector-http/src/profile/`).
  Every P2 SaaS row is one (OR-11), and CN3's generator emits the same format.

## Rules

1. **The id is the file name**: `redpanda.yaml` has `id: redpanda`.
2. **Module** (R-CN2-2): a target's `module` is one of the family modules
   (`http`, `kafka`, `postgres`, `mysql`, `sqlserver`, `objects`, `kv`,
   `search`, `mongodb`, `analytics`, `lakehouse`, `brokers`, `cloud`,
   `protocols`); an `http-api` profile's is `http`, and only it has an `http`
   block. A manifest whose native runtime names a family crate
   (`loams_connector_<family>::…`) must name the profile's.
3. **Real system** (OR-2): `system.service` is a compose service, a fixture
   recorded from the real system (`conformance/connectors/fixtures/<id>`), or
   the environment variables of a nightly real account (`LOAMS_*`).
4. **Narrowing**: `narrows` lists capability paths the target does not have,
   relative to the manifest's `capabilities`: `source` or `sink` (a whole
   direction), `source.<flag>` or `sink.<flag>`, `bulk.arrow`, or
   `formats.<slug>`. The target's effective capabilities are the family's minus
   these, and a delivery claim the narrowed sink can no longer make comes down
   to the strongest it still can. Each path needs a note on the manifest that
   starts `narrows: <path>` and says why. A narrowing a suite finds is added
   here with its note, never silently skipped.
5. **No secrets** (Review Focus 8): no key anywhere in a profile names a
   credential (`password`, `secret`, `token`, `api_key`, `Authorization`, …;
   `token_url` is a URL and is allowed), no fixed header carries one, and
   `base_url` has no user information. The instance config names its secrets,
   which the runtime resolves.
6. `defaults` are instance-config defaults merged *under* the user's config.

## HTTP-API profiles

The twenty SaaS and service rows of CN2 Task 20 are real profiles here, each
citing the vendor documentation it follows; `connectors/profiles/hubspot.yaml`
is the example below. Their fixtures under `conformance/connectors/fixtures/<id>/`
are synthetic until the nightly job records the real ones (R20.1).

```yaml
apiVersion: loams.flow/v1
kind: Profile
id: hubspot
profile_kind: http-api
module: http
system:
  name: HubSpot
  service: { fixture: conformance/connectors/fixtures/hubspot }
http:
  base_url: "https://api.hubapi.com"
  auth: { scheme: bearer }                       # a private-app access token
  headers: { Accept: application/json }        # fixed, non-secret headers
  resources:
    contacts:
      list:   { method: GET, path: "/crm/v3/objects/contacts", items: "/results",
                cursor: { next: "/paging/next/after", param: "after" }, key: "/id",
                query: { limit: "100" } }
      upsert: { method: POST, path: "/crm/v3/objects/contacts/batch/upsert",
                batch: 100, key: "/id", body: "/inputs" }
  rate_limit: { requests: 100, per_seconds: 10,
                honour_headers: ["Retry-After", "X-HubSpot-RateLimit-Remaining"] }
  webhook: { scheme: hubspot-v3, header: "X-HubSpot-Signature-v3" }
```

What the engine does with each block:

| Block | Engine |
|---|---|
| `list` | A polling source, one split per resource. Pages by `cursor` (a JSON pointer to the next cursor, sent back as `param`; `from: last_item` reads it from the page's last item, `from: header` from a response header, and `more` points at a "has more" boolean), by `next_url` (a JSON pointer to the next page's URL), by `offset` (`param` advanced by the items read, or by one page with `pages: true`; a short page is the last), or by the `Link: <…>; rel="next"` header when there is none of these. A next-page URL must be on `base_url`'s origin, or on the profile's own origin when an instance overrides `base_url` (it is then rebased). `items` points at the item array; `format: csv` reads CSV rows as objects of strings, `format: ndjson` one JSON document per line (`items` inside each). `key` points at an item's key, which becomes the event's `subject`; `event` names the event (default `item`). The position is the page and the items taken from it, so a restart resumes there. Event ids are derived from the item's content, so a re-read of an unchanged item has the same id and `ingest` drops it. |
| `upsert`, `create` | A sink. Items (each event's JSON `data`, or the part at `select`, put under `wrap`) go `batch` at a time, at the pointer `body` (`/inputs`) inside `envelope` (a fixed body), or alone as the body when there is no `batch`. `encoding` is `json`, `ndjson` (one item per line) or `form` (one item per request, nested values as `a[b]=c`). `{item:<pointer>}` in the path sends one request per item. With `idempotency_key: true` each request carries `Idempotency-Key`: the event id, or a digest of a batch's event ids, so a retry sends the same key. `conflict_ok` counts a `409` as written. `ack` waits for each request's acknowledgement id to be confirmed on the instance's channel before the batch counts (Splunk HEC); an unconfirmed batch is resent. A resource with `idempotency_key` or `key` is `effectively_once`; anything else is `at_least_once`. |
| `async_job` | A source: `create` the job, read its id at `id`, `poll` it every `interval_ms` until its `state` is in `done` (in `failed`: the instance stops), then page through `results` like a `list`. `{id}` in a path is the job id. |
| `rate_limit` | A token bucket of `requests` per `per_seconds`. Of `honour_headers`, `Retry-After` on a 429 or 503 delays the next request and makes the error `Throttled`; a `*-Remaining` header of 0 holds the next request for the `*-Reset` header's seconds, or for `per_seconds`. Headers not listed are not read. |
| `webhook` | Routed to the webhooks source, which verifies each delivery with the named scheme (`loams_connector_http::webhook::SCHEMES`: `hmac-sha256`, `stripe`, `github`, `slack`, `shopify`, `hubspot-v3`, `zendesk`, `twilio`, `segment`, `sendgrid`, `token`). |
| `ok` | A JSON pointer every response must carry as `true` (Slack's `ok`); `false` is an error, poison for a write and fatal for a read. |
| `auth` | `bearer` (`auth.token`), `basic` (`auth.username`, `auth.password`), `header` (`auth.api_key` in `header`, after `prefix`), `oauth2` (`auth.client_id`, `auth.client_secret`, and `auth.refresh_token` for the refresh-token grant; client credentials otherwise) and `aws-sigv4` (`auth.access_key_id`, `auth.secret_access_key`, optional `auth.session_token`; `service`, and `region` or the instance's `auth.region`). Each `auth.*` value in the instance config is a **secret name**, resolved through the instance's secret store. |

An instance of an HTTP-API manifest names its resource with `resource`, and may
set `poll_interval_ms` (re-list after the last page; absent, a listing is read
once), `base_url` and `token_url` (a sandbox host and its token endpoint), and
`projection` (an import route's field mapping, `loams_connector_http::route`).

### Placeholders

- `{config.<key>}`: a value of the instance config (after `defaults`), filled in
  once when the instance starts, in `base_url`, `token_url`, `auth.region`, paths
  (percent-encoded, `/` included), query values and JSON bodies. A missing value
  stops the instance with its name.
- `{auth.<field>}`: the secret the instance names under `auth.<field>`, resolved at
  send time, in a path (Kestra's webhook key) or an `envelope` (PostHog's project
  token). A credential-named key may hold one (`api_key: "{auth.api_key}"`), never a
  value. It is never expanded inside an item: data never pulls a secret into a
  request.
- `{item:<pointer>}`: in a write's path, a value of the item (Vault's secret path).
