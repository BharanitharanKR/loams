# Grafeo (Loams Graph)

Grafeo runs on the native protocols family (`runtime.ref:
loams_connector_protocols::grafeo`; CN2 Task 21). Since D741 the graph engine is no longer
in-process for Flow (D634 (b) is dropped): the connector is a client of `loams.graph.v1` over
Connect, with the messages `loams-flow-proto` generates from the repository's
`proto/loams/graph/v1/graph.proto` (OR-20, ruling R21.4).

| | |
|---|---|
| Tested against | a loopback Connect peer checking the generated messages and the key (everywhere), and a Loams graph endpoint in the nightly job (`LOAMS_GRAFEO_*`) |
| Sink | `ExecuteBatch` of parameterised GQL, atomic by default |
| Source | `ExecuteStream` with a cursor property |
| Auth | `key`: a Loams API key, required |

## Transport

The Connect protocol's binary form over the SDK's egress-checked HTTP client:
`application/proto` for unary calls, `application/connect+proto` envelopes for the stream,
`Connect-Protocol-Version: 1`. The API key goes as `Authorization: Bearer
loams_<key_id>_<secret>`. An instance without `api_key` is refused when it is created
(`Auth`). Connect error codes map as: `unauthenticated`/`permission_denied` → `Auth`,
`resource_exhausted` → `Throttled`, `unavailable`/`deadline_exceeded`/`aborted`/`internal` →
`Retryable`, `not_found`/`unimplemented` → `Fatal`, other codes `Poison` on a write and
`Fatal` on a read.

## Sink

One statement per event; the event's top-level fields are bound as `$name` parameters, plus
`$ce_id`, never interpolated. `atomic: true` runs each `ExecuteBatch` (up to `batch`
statements) as one transaction, and its `idempotency_key` is a digest of the batch's event ids,
so a retry is the same request to the server.

## Source

One split per `source.label`, read with `$cursor`, `$after` (the node id) and `$limit`:

```gql
MATCH (n:`Label`) WHERE n.`cursor` IS NOT NULL AND ($cursor IS NULL OR n.`cursor` > $cursor
  OR (n.`cursor` = $cursor AND id(n) > $after))
RETURN n, n.`cursor` AS cursor, id(n) AS eid ORDER BY cursor, eid LIMIT $limit
```

The position keeps the last cursor as `loams.graph.v1.Value` bytes, so it compares as itself.
Rows become `io.loams.dev.flow.grafeo.rows.v1` events, id `<graph>/<label>/<id>@<cursor>`. The
default statement's `id(n)` is unverified on Grafeo until the nightly run; `source.statement`
overrides it.

## Config

```yaml
url: https://loams.internal:8443
namespace: acme
graph: knowledge
api_key: grafeo-key              # a secret's name
language: gql                    # or cypher (D748)
sink: { statement: "MERGE (d:Doc {id: $id}) SET d.title = $title", atomic: true, batch: 100 }
source: { label: Doc, cursor: rev, page_size: 100, poll_interval_ms: 10000 }
```
