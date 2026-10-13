# ClickHouse (sink)

The ClickHouse ★'s sink is native (`loams_connector_analytics::clickhouse_sink`,
CN2 Task 17, OR-8), replacing Iggy's `clickhouse_sink` as the default. It speaks
ClickHouse's HTTP interface through the connector SDK's client, so every
connection passes the instance's egress check. The ★'s source stays CN1's native
`loams_flow::connectors::clickhouse` until CN2 Task 8 moves it into the family
crate.

| | |
|---|---|
| Insert | one `INSERT INTO <database>.<table> FORMAT ArrowStream` per Arrow batch, or `FORMAT JSONEachRow` with one row per event's `data` (a JSON object) |
| Delivery | `effectively_once` within the table's deduplication window: every insert carries `insert_deduplication_token` = the batch's id (a digest of its event ids), the same on every retry |
| Auth | `user` and `password` (the name of a secret), sent as `X-ClickHouse-User` / `X-ClickHouse-Key` |
| Errors | by ClickHouse exception code: authentication → `Auth`; bad rows (`CANNOT_PARSE_*`, `TYPE_MISMATCH`, …) → `Poison` (DLQ); unknown table or column → `Fatal`; `TOO_MANY_PARTS` → `Throttled`; memory, timeouts, Keeper → `Retryable` |
| Tested against | a local endpoint in every build (request shape, the token across a pipeline retry); real ClickHouse in CN1's `connectors` stack; Loams House as an extra target (OR-2) |

## Configuration

```yaml
url: https://clickhouse.example.com:8443
database: analytics
table: events
user: loams
password: clickhouse-password   # a secret name
dedup: true                     # default
settings: { max_insert_block_size: "1048576" }
```

## Deduplication

Replicated MergeTree tables deduplicate inserted blocks by default. A plain
MergeTree table needs the table setting `non_replicated_deduplication_window`
(for example `100`); without it a retried batch can land twice, and the delivery
is at-least-once.
