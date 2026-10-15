# GraphQL

GraphQL runs on the native protocols family (`runtime.ref:
loams_connector_protocols::graphql`; CN2 Task 21). Queries and mutations are `POST {query,
variables}` through the SDK's egress-checked HTTP client; subscriptions use
`graphql-transport-ws` over the family's own WebSocket client, also egress-checked
(ruling R21.2).

| | |
|---|---|
| Tested against | synthetic fixtures of the GitHub GraphQL API's documented shapes (`conformance/connectors/fixtures/graphql/`), a loopback `graphql-transport-ws` peer, and (nightly, `LOAMS_GITHUB_TOKEN`) the GitHub GraphQL API |
| Source | a Relay-cursor query (batch, resumable) or a subscription (streaming, `resumable: false`) |
| Sink | one mutation per event |
| Auth | `key` (a header) or `oauth2`/bearer token, by secret name |

## Query source

`query.connection` points at a Relay connection in the answer (`/data/repository/issues`). The
source pages it with the `first`/`after` variables (`first_variable`, `after_variable`) and
`pageInfo { hasNextPage endCursor }`. Items come from `nodes`, or from `edges[].node` when the
query selects edges, in which case the position can stop mid-page at an edge's `cursor`.
Each item is an `io.loams.dev.flow.graphql.item.v1` event whose id is a digest of the item, so
an unchanged item re-read is deduplicated by `ingest`; its `subject` is the item's `id`.
With `poll_interval_ms` the listing restarts from the first page after the interval; without,
it is read once. A GraphQL `errors` answer on a read stops the source (`Fatal`), unless it says
it was rate limited (`Throttled`, 60 s).

## Subscription source

`connection_init` (with the credential as `Authorization` in the payload and on the upgrade),
`connection_ack`, `subscribe`, then each `next` payload's `data` is one event. `complete` ends
the source. A restart subscribes afresh: nothing is replayed.

## Mutation sink

The event's data (or the object at `mutation.variables_pointer`) is the variables object. A
GraphQL error is `Poison` for that event (DLQ); a rate-limit error is `Throttled`.

## Config

```yaml
url: https://api.github.com/graphql
auth: { bearer: github-token }        # or header: { name: X-Api-Key, secret: …, prefix: "" }
query:
  query: "query($first: Int!, $after: String) { viewer { repositories(first: $first, after: $after) { pageInfo { hasNextPage endCursor } nodes { id name } } } }"
  connection: /data/viewer/repositories
  page_size: 50
  poll_interval_ms: 600000
mutation:
  mutation: "mutation($input: AddCommentInput!) { addComment(input: $input) { clientMutationId } }"
```
