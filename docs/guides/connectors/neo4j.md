# Neo4j

Neo4j runs on the native protocols family (`runtime.ref: loams_connector_protocols::neo4j`;
CN2 Task 21, OR-14) over Neo4j's **HTTP Query API**, `POST /db/{database}/query/v2`. There is
no Bolt client in the build (D634 (c); the test `no_bolt_dependency` checks the lock), and
`camel-neo4j` is not used.

| | |
|---|---|
| Tested against | Neo4j 5.26 Community (`neo4j:5.26.30-community`, GPL-3.0) in CI's `connectors-p2` stack, and synthetic fixtures of the documented shapes (`conformance/connectors/fixtures/neo4j/`) |
| Minimum server | 5.19 (the Query API's first release; on by default for self-managed servers from 5.25). Older servers are refused at connect |
| Sink | a parameterised statement per batch (`UNWIND $rows AS row MERGE …`); idempotent when it `MERGE`s on a key |
| Source | cursor property, `(cursor, elementId)` order |
| Auth | `basic` (password by secret name) or a bearer token |

Whether the Query API ships in **Community Edition** was unverified when this was written
(spike §7); `neo4j_sink_merge_idempotent` and `neo4j_source_http_query_api_cursor` settle it in
CI. A 404 from the query endpoint is reported as "no such database, or the HTTP Query API is
disabled".

## Sink

The batch's event data objects are bound as `$rows`, one request (one implicit transaction)
per `batch` rows. With `delete_statement`, events with `loamsop = d` run it instead, and a
switch between upserts and deletes starts a new request. Errors: a `Neo.TransientError.*` is
retried, a `Neo.ClientError.Security.*` is `Auth`, any other statement error is `Poison` (the
batch is isolated event by event into the DLQ). The Query API may answer 202 with `errors`;
those are read too.

## Source

One split per `source.label`. The default statement:

```cypher
MATCH (n:`Label`) WHERE n.`cursor` IS NOT NULL AND ($cursor IS NULL OR n.`cursor` > $cursor
  OR (n.`cursor` = $cursor AND elementId(n) > $after))
RETURN n, n.`cursor` AS cursor, elementId(n) AS eid ORDER BY cursor, eid LIMIT $limit
```

A custom `source.statement` takes `$cursor`, `$after` and `$limit` and returns `cursor` and
`eid`. Reads use Neo4j's typed JSON (`application/vnd.neo4j.query`), so the cursor goes back as
the type it came as (a datetime compares as a datetime). Rows become
`io.loams.dev.flow.neo4j.rows.v1` events, plain JSON (nodes as `{elementId, labels,
properties}`), id `<label>/<elementId>@<cursor>`. With `poll_interval_ms` the source keeps
polling from its cursor; without, it ends after one pass.

## Config

```yaml
url: http://neo4j.internal:7474
database: neo4j
auth: { basic: { username: neo4j, password: neo4j-password } }   # a secret's name
sink:
  statement: "UNWIND $rows AS row MERGE (p:Person {id: row.id}) SET p += row"
  delete_statement: "UNWIND $rows AS row MATCH (p:Person {id: row.id}) DETACH DELETE p"
  batch: 500
source: { label: Person, cursor: updatedAt, page_size: 500, poll_interval_ms: 30000 }
```
