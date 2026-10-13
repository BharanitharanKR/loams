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
  auth: { scheme: oauth2, token_url: "https://api.hubapi.com/oauth/v1/token" }
  headers: { Accept: application/json }        # fixed, non-secret headers
  resources:
    contacts:
      list:   { method: GET, path: "/crm/v3/objects/contacts", items: "/results",
                cursor: { next: "/paging/next/after", param: "after" }, key: "/id" }
      upsert: { method: POST, path: "/crm/v3/objects/contacts/batch/upsert",
                batch: 100, key: "/id", body: "/inputs" }
  rate_limit: { requests: 100, per_seconds: 10,
                honour_headers: ["Retry-After", "X-HubSpot-RateLimit-Remaining"] }
  webhook: { scheme: hmac-sha256, header: "X-HubSpot-Signature-v3" }
```

What the engine does with each block:

| Block | Engine |
|---|---|
| `list` | A polling source, one split per resource. Pages by `cursor` (a JSON pointer to the next cursor, sent back as `param`), or by the `Link: <…>; rel="next"` header when there is no `cursor` (same origin as `base_url` only). `items` points at the item array; `key` at an item's key, which becomes the event's `subject`. The position is the page cursor and the items taken from it, so a restart resumes there. Event ids are derived from the item's content, so a re-read of an unchanged item has the same id and `ingest` drops it. |
| `upsert`, `create` | A sink. Items (each event's JSON `data`) go `batch` at a time, at the pointer `body` (`/inputs`), or alone as the body when there is no `batch`. With `idempotency_key: true` each request carries `Idempotency-Key`: the event id, or a digest of a batch's event ids, so a retry sends the same key. A resource with `idempotency_key` or `key` is `effectively_once`; anything else is `at_least_once`. |
| `async_job` | A source: `create` the job, read its id at `id`, `poll` it every `interval_ms` until its `state` is in `done` (in `failed`: the instance stops), then page through `results` like a `list`. `{id}` in a path is the job id. |
| `rate_limit` | A token bucket of `requests` per `per_seconds`. Of `honour_headers`, `Retry-After` on a 429 or 503 delays the next request and makes the error `Throttled`; a `*-Remaining` header of 0 holds the next request for the `*-Reset` header's seconds, or for `per_seconds`. Headers not listed are not read. |
| `webhook` | Routed to the webhooks source with the named signature scheme. |
| `auth` | `bearer` (`auth.token`), `basic` (`auth.username`, `auth.password`), `header` (`auth.api_key` in `header`, after `prefix`), `oauth2` (`auth.client_id`, `auth.client_secret`, and `auth.refresh_token` for the refresh-token grant; client credentials otherwise) and `aws-sigv4` (`auth.access_key_id`, `auth.secret_access_key`, optional `auth.session_token`; `service`, and `region` or the instance's `auth.region`). Each `auth.*` value in the instance config is a **secret name**, resolved through the instance's secret store. |

An instance of an HTTP-API manifest names its resource with `resource`, and may
set `poll_interval_ms` (re-list after the last page; absent, a listing is read
once) and `base_url` (a sandbox host).
