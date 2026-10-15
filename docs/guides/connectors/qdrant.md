# Qdrant

Qdrant runs on the native search family, `loams-connector-search`
(`runtime.ref: loams_connector_search::qdrant`; CN2 Task 15). It speaks Qdrant's REST API
through the SDK's egress-checked HTTP client (ruling R15.2).

| | |
|---|---|
| Tested against | Qdrant v1.19.1 (`qdrant/qdrant`, Apache-2.0) with an API key, in CI's `connectors-p2` stack |
| Extra target | Loams collections' Qdrant API (`crates/loams-qdrant`), never counted toward `preview` (OR-2) |
| Sink delivery | effectively once: the point id is UUID v5 of the document key |
| Source delivery | at least once into `ingest`, effectively once there: event ids carry a content digest |
| Auth | `key` (the `api-key` header) |

## Sink: upserts with UUID v5 ids

Each event becomes a point. The point id is UUID v5 of the document key (the value at `key`,
or the event's `ce_id`), under the namespace UUID v5 of
`https://loams.dev/flow/connectors/qdrant/point` (`6d6efca0-be1a-57d0-aab8-fa00e89cbe92`).
So the same key always gives the same point, and a replay overwrites it.

- **The vector** is the value at `vector` (default `/vector`): an array of numbers, or an
  object of named vectors.
- **The payload** is the object at `payload`. Without `payload`, it is the whole data object
  minus a top-level vector.
- **Writes.** Upserts go to `PUT /collections/<c>/points?wait=true`, so each is applied
  before the write returns.
- **Deletes.** An event with `loamsop = d` deletes the point
  (`POST /collections/<c>/points/delete?wait=true`).
- **Order.** Consecutive upserts share a request (up to `batch`, default 256), and a delete
  starts a new request, so per-key order holds.
- **Refusals.** An event without a vector, or one Qdrant refuses (a wrong dimension), is
  `Poison` and goes to the dead-letter queue event by event.

## Source: scroll

The source reads one split per collection, with `scroll` in point-id order, carrying payload
and (by default) vectors. Each point becomes an `io.loams.dev.flow.qdrant.document.v1` event:

- `subject` is the point id;
- `data` is `{id, payload, vector}`;
- `id` is `<collection>/<point id>@<first 16 hex of SHA-256 of the data>`.

So an unchanged point re-read has the same id, and a changed one a new id.

The position is the scroll offset, which is the id of the next point to read. A page cut short
by the budget therefore resumes at the first point not taken.

- **Without `source.poll_interval_ms`**, the source reads one pass and ends.
- **With `source.poll_interval_ms`**, a finished pass starts again from the first point after
  the interval. Unchanged points are deduplicated by `ingest`.

`source.filter` restricts the scroll.

## Config

```yaml
url: http://qdrant.internal:6333
collection: articles
api_key: qdrant-articles-key          # a secret's name
key: /payload/article_id
vector: /embedding
payload: /payload
```

## Tests

`fabric/crates/loams-connector-search/tests/qdrant.rs`:

- The UUID v5 derivation, request shapes, poison handling and scroll resume run without a
  server.
- `qdrant_upsert_idempotent_uuid_v5`, `qdrant_scroll_resumes` and the kit's `roundtrip`,
  `kill-restart`, `dup-check` and `auth` suites run against real Qdrant when
  `LOAMS_CONNECTORS_P2_STACK` is set.
