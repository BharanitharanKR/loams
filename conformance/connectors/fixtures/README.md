# Connector fixtures

Recorded exchanges with real cloud and SaaS systems, replayed in PR CI by
`loams-flow-conformance`'s `FixtureProxy` (CN2 plan Task 6, owner ruling
OR-2). One directory per manifest id (`fixtures/<id>/`), one JSON file per
scenario. `conformance/connectors/systems.toml` names the directory as the
manifest's `fixture` service.

## Rules

- **Recorded only from the real system.** A fixture is written by
  `FixtureProxy::record` while a connector talks to the real system (a
  nightly account, or a developer's own account), and its `recorded_from`
  names that system. Never recorded from a mock, an emulator or a Loams
  module, and never hand-written unless marked synthetic (below). A fixture
  without `recorded_from` is refused.
- **Synthetic fixtures are marked, and never count** (CN2 Task 20, R20.1).
  Where no account could be recorded from yet, a family task may build a
  fixture by hand from the vendor's *published* request and response shapes.
  It says so with `"synthetic": true` and lists the documentation it follows
  in `"docs"` (`Fixture::load` refuses a synthetic fixture without them). It
  replays exactly like a recording, so the family's own tests run in PR CI,
  but `Fixture::counts_toward_preview` is false and the kit never treats a
  directory of synthetic fixtures as an available service: the manifest stays
  `planned` until the `connectors-nightly` job records the real exchanges and
  replaces it. Values in a synthetic fixture are invented (`example.test`
  addresses, `synthetic-…` credentials, documentation IP ranges).
- **Scenarios.** A fixture may carry a `"scenario"` object (ignored by the
  kit) that the family's tests read: the resource, the instance config
  (`{proxy}` is the replay's URL), the secrets by field, and the items written
  or expected. A webhook fixture holds the delivery as its request, with the
  body as `{ "text": … }` so the signed bytes are exact.
- **No secrets.** Recording scrubs header values (`Authorization`, cookies,
  API-key headers, …), query parameters (`access_token`, `client_secret`,
  signatures, …) and JSON body fields by pointer (`/access_token`,
  `/password`, …) in requests and responses, and every secret value the
  connector resolved, wherever it appears (`ScrubRules`). Review a new
  fixture before committing it anyway.
- **Replay is strict.** A request matches a recording by method, path (query
  parameters sorted, scrubbed) and canonical body (JSON with sorted keys,
  scrubbed). Headers are not matched. An unmatched request is answered `500`
  and fails the test (`ProxyHandle::finish`); a connector change that sends a
  new request needs a new recording, not a looser matcher.
- **A skipped suite is not green.** A manifest whose only PR-CI service is a
  fixture is skipped until its directory holds a recording, and a skipped
  suite never counts toward `preview` (OR-23).

## Format

```json
{
  "recorded_from": "HubSpot",
  "exchanges": [
    {
      "request": {
        "method": "GET",
        "path": "/crm/v3/objects/contacts?after=<scrubbed>&limit=100",
        "headers": { "authorization": "<scrubbed>" },
        "body": "empty"
      },
      "response": {
        "status": 200,
        "headers": { "content-type": "application/json" },
        "body": { "json": { "results": [] } }
      }
    }
  ]
}
```

`body` is `"empty"`, `{ "json": … }` or `{ "text": "…" }`.

## Refreshing

The `connectors-nightly` job runs each family's suites against the real
accounts; a family task re-records its fixtures from that run when the
system's API changes, and commits the diff for review.
