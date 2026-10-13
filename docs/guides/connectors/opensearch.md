# OpenSearch

OpenSearch runs on the native search family's Elasticsearch code as a **target profile**,
`connectors/profiles/opensearch.yaml` (`module: search`; CN2 Task 15, OR-2). Its manifest
names the profile and `runtime.ref: loams_connector_search::elasticsearch`. The suites run
against OpenSearch itself, never against Elasticsearch.

| | |
|---|---|
| Tested against | OpenSearch 3.9.0 (`opensearchproject/opensearch`, Apache-2.0), single node, security plugin off, in CI's `connectors-p2` stack; Amazon OpenSearch Service with SigV4 in the nightly job |
| Extra target | Loams collections' Elasticsearch API, never counted toward `preview` (OR-2) |
| Narrowings | none (`narrows: []`); the differences are profile defaults |
| Auth | `basic`; `iam` (AWS SigV4) for Amazon OpenSearch Service |

Everything in the [Elasticsearch guide](elasticsearch.md) applies: the `_bulk` sink, the
point-in-time source, positions and delivery. Events are typed
`io.loams.dev.flow.opensearch.document.v1`.

## What the profile changes

- **`api: opensearch`** selects OpenSearch's point-in-time API, introduced in 2.4:
  - open with `POST /<index>/_search/point_in_time?keep_alive=…`, which answers `pit_id`;
  - search with `POST /_search` and `"pit": {"id", "keep_alive"}`;
  - close with `DELETE /_search/point_in_time` and the body `{"pit_id": [...]}`.

  The spike found no change to this API in 3.x but could not verify it (spike §5.5);
  `opensearch_suite_against_opensearch` is what proves it.
- **`source.tiebreaker: _id`**: OpenSearch documents no `_shard_doc` sort, so the tiebreaker
  inside a point in time is `_id` (ruling R15.4). This is also unverified on 3.x until the
  suite runs. If the suite finds otherwise, the profile changes, and a lost capability is
  added to `narrows` with a `narrows:` note.

## Amazon OpenSearch Service

Set `aws` in the instance config:

- `region`;
- `service`: `es` for domains (the default), or `aoss` for Serverless;
- `access_key_id` and `secret_access_key`, each a secret's name;
- optionally `session_token`, also a secret's name.

Every request is signed with SigV4 (`x-amz-date` and `x-amz-content-sha256` signed). The
nightly job runs `opensearch_sigv4_nightly` with these repository secrets:

- `LOAMS_AMAZON_OPENSEARCH_URL`;
- `LOAMS_AWS_ACCESS_KEY_ID`;
- `LOAMS_AWS_SECRET_ACCESS_KEY`;
- `LOAMS_AWS_REGION`.

## Tests

`fabric/crates/loams-connector-search/tests/opensearch.rs`:

- `opensearch_profile_is_a_search_target`, `opensearch_pit_api_shapes` and
  `opensearch_sigv4_signs_requests` run without a server.
- `opensearch_suite_against_opensearch` runs the kit's `roundtrip`, `kill-restart` and
  `dup-check`, plus a point-in-time resume, when `LOAMS_CONNECTORS_P2_STACK` is set.
- `opensearch_sigv4_nightly` runs in the nightly job.
