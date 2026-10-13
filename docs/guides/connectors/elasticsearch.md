# Elasticsearch

Elasticsearch ★ runs on Loams' native search family, `loams-connector-search`
(`runtime.ref: loams_connector_search::elasticsearch`; CN2 Task 15, OR-8). Iggy's
`elasticsearch_sink` and `elasticsearch_source` plugins remain as the declared fallback
(OR-4) until the manifest's runtime moves (CN2 Task 2, ruling R15.5).

| | |
|---|---|
| Tested against | Elasticsearch 9.5.4 (`docker.elastic.co/elasticsearch/elasticsearch`, a `ci-service`: AGPL-3.0, SSPL or Elastic-2.0; OR-3), single node, security on, in CI's `connectors-ci-only` stack |
| Extra target | Loams collections' Elasticsearch API, tested after the real system and never counted toward `preview` (OR-2) |
| Sink delivery | effectively once: `_id` is the document key or the event's `ce_id`, so a replay overwrites |
| Source delivery | at least once into `ingest`, effectively once there: event ids are stable per document version |
| Auth | `basic`, `key` (API key) |

## Sink: `_bulk`

Each event becomes an `index` action whose `_id` is the value at `key` (a JSON pointer, or a
top-level field name) in the event's data, or the event's `ce_id` without `key`. An event with
`loamsop = d` becomes a `delete` by `_id` (turn this off with `apply_deletes: false`); deleting
a document that is not there counts as done.

- **Partial failures** resend only the failed items. An item answered 429 or 5xx is retried
  alone, with a doubling backoff from `bulk.retry_backoff_ms`, up to `bulk.max_item_retries`
  times in a row without progress. Then the write fails as `Throttled` (or `Retryable`) and the
  pipeline retries it.
- **A refused document** (any other 4xx, such as a mapping conflict) fails the write as
  `Poison`. The pipeline then writes the batch event by event and sends each refused document
  to the dead-letter queue with Elasticsearch's error. The other documents are written once
  more, which is harmless because writes are by `_id`.
- **429s shrink the batch.** A 429 for the whole request, or for any item, halves the
  documents per request (never below one). Clean requests double it back up to
  `bulk.max_docs`. Requests are also cut at `bulk.max_bytes`.

Arrow batches are refused as `Poison`: the sink takes JSON documents.

## Source: point in time and `search_after`

The source reads one split per index. A pass opens a point in time (`POST /<index>/_pit`) and
pages with `search_after`. It sorts by `source.cursor_field` (when set) and then the tiebreaker
`_shard_doc`. Each hit becomes an `io.loams.dev.flow.elasticsearch.document.v1` event:

- `id` is `<index>/<_id>@<_seq_no>.<_primary_term>`, so a re-read of the same version has the
  same id and a changed document has a new one;
- `subject` is `_id`;
- `data` is `_source`.

The position is the point-in-time id and the last sort values, plus the pass's lower bound and
the greatest cursor value read:

- **Resumed while the point in time lives** (`source.keep_alive`, default `2m`), the source
  continues inside it, with no gap and no repeat.
- **Resumed after it expired**, a new pass starts at `cursor >= last value read`. Documents
  tied at that value are read again and deduplicated by `ingest` through their stable ids.
  Without a cursor field, the new pass reads the whole index.
- **Without `source.poll_interval_ms`**, the source reads one pass and ends.
- **With `source.poll_interval_ms`**, a finished pass starts again after the interval, from
  the last cursor value. This is incremental polling, so the cursor field must only grow: an
  update timestamp or a sequence.

`source.query` filters the read. A document larger than the batch budget is skipped as
`Poison` (R4.7).

## Config

See `connectors/schemas/elasticsearch.config.json`. `basic.password` and `api_key` name
secrets in the namespace's store; they are never values. The URL must not carry
credentials. Every connection goes through the SDK's egress check after DNS.

```yaml
url: https://search.internal:9200
index: orders
key: /order_id
basic: { username: loams, password: es-orders-password }   # a secret's name
source: { cursor_field: updated_at, poll_interval_ms: 30000 }
```

## Tests

`fabric/crates/loams-connector-search/tests/elasticsearch.rs`:

- The request-shape tests run without a server: partial failure, 429 shrinking, refused
  items, deletes, Basic auth, and the PIT body, resume and expiry.
- `es_source_pit_search_after_resumes` and the kit's `roundtrip`, `kill-restart`, `dup-check`
  and `auth` suites run against real Elasticsearch when `LOAMS_CONNECTORS_CI_ONLY_STACK` is
  set. The harness is `loams_connector_search::conformance::SearchHarness`.
