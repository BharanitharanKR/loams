# The app scenario (SDKP P0d)

A notes app, recorded once from `loams dev` and replayed by every SDK through
its **public facade**. The recordings are `recorded/app/*.json`; the scenario
that produced them is `sdks/conformance/app-scenarios.mjs`. This page says the
same thing in prose, for whoever writes a language's `<lang>_app_*` tests.

## The data

Namespace `app`, collection `notes`, created from the `Schema` builder's
`schema_notes` case: `title` and `body` (text), `tags` (keyword), `n` (i64),
`embedding` (four dimensions, cosine). Five notes:

| id | title | tags | n | embedding |
|---|---|---|---|---|
| 1 | Refund policy | policy | 1 | [1, 0, 0, 0] |
| 2 | Shipping times | shipping | 2 | [0.9, 0.1, 0, 0] |
| 3 | Refund window | policy | 3 | [0, 0, 1, 0] |
| 4 | Returns | policy | 4 | [0.5, 0.5, 0, 0] |
| 5 | Gift cards | billing | 5 | [0, 0, 0, 1] |

`app_collections` patches note 2 and deletes note 5, so `app_search` runs over
four notes.

## Replaying a step

Each step of a recording carries:

- `call`: the facade call that makes it, as `module.call` in the facade index's
  names (`documents.write`, `vector.search`). Use the language's casing.
- `request.body`: the request's proto3 JSON. Decode it into the call's request
  message and make the call through the facade. The fixture server compares the
  body it receives with the recording, as JSON, so what the facade sends must
  decode to the same message.
- `builder` (some steps): the builder corpus case this request is. For those
  steps, build the request with the builder from the case's `spec` rather than
  decoding the body, and the replay proves the builder too.
- `response`: what the server answered. Compare the facade's result with it,
  ignoring the fixture's `volatile` fields; for a refusal, the typed error's
  class and `reason`.

Send `loams-fixture-name: <fixture>` and `loams-fixture-step: <index>` on each
call (the index is 0-based): several steps share one RPC, and the headers are
how the fixture server knows which recording you mean.

Every write carries a fixed `idempotencyKey` (`app-upsert-1`, `app-patch-1`,
…). An SDK mints a UUIDv7 per logical call, so pass the recorded key
explicitly, or the body will not match.

## The steps

**`app_collections`**

0. `instance.get_instance`: feature detection.
1. `namespaces.create("app")`.
2. The same again: `already_exists`.
3. `collections.create("notes", schema_notes)`.
4. `collections.get("notes")`.
5. `collections.create("scratch", schema_notes)`, so the list has two pages.
6. `collections.list` with `page_size` 1: the first page and a token.
7. The second page from that token (what `list_all` does).
8. `documents.write`: upsert the five notes, key `app-upsert-1`.
9. The same write and key: the first answer again (R3).
10. `documents.get` of all five at step 8's `token` (`consistency.at_least`, R4).
11. `documents.write`: patch note 2 (`documents.patch`), key `app-patch-1`.
12. `documents.write`: delete note 5 (`documents.delete`), key `app-delete-1`.
13. `documents.count` with `where` = the `filter_term` case, at step 12's token.
14. `documents.scroll` with `limit` 2: the first page.
15. The second page, `after` step 14's `next`: no `next` in the answer, which
    is where `scroll_all` stops.
16. `collections.get("missing")`: `not_found`.
17. `documents.count` with both `filter` and `where`: `filter_conflict`.
18. `collections.drop("scratch")`.

**`app_search`**

0. `search.query`: the `search_text_filter_highlight_select` case.
1. The next page: `search_after` step 0's last hit's `sort_values` (the
   `search_after_page` case; `query_all` makes it).
2. `vector.search`: the `search_vector_ann_params` case.
3. `search.query`: `search_hybrid_rrf`.
4. `search.query`: `search_hybrid_dbsf`.
5. `search.query`: `search_hybrid_weighted`.
6. `search.query`: a weighted sum with one weight for two retrievers:
   `invalid_argument`. Send the raw request: the builder refuses it before
   sending (`<lang>_builders_match_corpus` pins that half).

**`app_graph`** (the default `loams dev` build has no `graph` feature)

0. `graph.create("notes_graph")`: `feature_not_in_variant`.
1. `graph.query`: a GQL `INSERT` with parameters, keyed: the same refusal.
2. `graph.query`: a read-only `MATCH … RETURN`: the same refusal.
3. `search.query` with the `search_expand` case: the expand stage needs the
   graph feature.
4. `graph.delete("notes_graph")`: the same refusal.

**`app_sql`** and **`app_streams`** are recorded by SDKP P0b and P0c.

## Re-recording

```sh
loams dev --listen 127.0.0.1:8080 --no-live --no-flight-sql --no-qdrant --no-es &
LOAMS_TEST_ENDPOINT=http://127.0.0.1:8080 node sdks/conformance/record-fixtures.mjs --source app
```

The server must be fresh: step 1 creates the namespace. Scores are not volatile;
a re-recording that changes one is a ranking change, and the builder corpus's
`search_after_page` follows the recorded `sort_values`.
