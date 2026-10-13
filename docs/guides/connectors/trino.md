# Trino

Native, `loams_connector_analytics::trino` (CN2 Task 17): a **source only**, over
Trino's client REST protocol (`POST /v1/statement`, then `GET nextUri` until it
is absent) through the SDK's egress-checked HTTP client. A `nextUri` must stay on
the configured coordinator.

| | |
|---|---|
| Source | one split (the query), read once; rows arrive as JSON arrays and are decoded to Arrow by the declared column types |
| Types | the shared SQL mapper plus Trino's spellings: `varchar`, `char`, `uuid`, `json`, `ipaddress` → `Utf8`; `varbinary` → `Binary`; `timestamp(p)` exact for p ≤ 6, lossy above; nested types (`array`, `map`, `row`) pause the instance |
| Delivery | `effectively_once` (§33 §6 run ids over the query and the as-of time) |
| Auth | `user` (`X-Trino-User`); `password` (a secret name, HTTP Basic, HTTPS only) or `token` (a secret name, Bearer) |
| Resume | Trino serves a `nextUri` again until the next one is requested, so a restart re-fetches the page it was reading; once Trino has moved past it (`404`/`410`), the query runs again and the delivered rows are skipped: give the query an `ORDER BY` (R17.9) |
| Tested against | a local endpoint in every build; Trino 483 (compose `trino`) in `connectors-p2` |

```yaml
url: https://trino.example.com:8443
user: loams
catalog: hive
schema: sales
query: SELECT * FROM orders ORDER BY id
password: trino-password
```
