# DuckDB

DuckDB is a target profile (`connectors/profiles/duckdb.yaml`) on the ADBC
module `loams_connector_analytics::adbc` (CN2 Task 17, R17.7). The driver is
DuckDB's own shared library, `libduckdb` (entry point `duckdb_adbc_init`):
Loams compiles no DuckDB code.

A driver is native code loaded into the Loams process, so it is loaded **by path
from the runtime's allowlist with a SHA-256 check** (CN1 Ruling 9). The instance
names a driver; only the allowlist (a JSON file named by `LOAMS_ADBC_ALLOWLIST`,
set by the namespace's admin) says where its library is and what it must hash to:

```json
{"drivers": {"duckdb": {"path": "/opt/loams/drivers/libduckdb.so",
                         "sha256": "3f4962cdd266eab2b479a5b5a4a6d063d279b2954ae814ed00385d553b747633",
                         "entrypoint": "duckdb_adbc_init"}}}
```

(The digest above is `libduckdb.so` from DuckDB v1.5.6's
`libduckdb-linux-amd64.zip`.)

| | |
|---|---|
| Source | `query`, executed on a blocking thread; the result reader streams Arrow batches through a bounded channel, one page per batch; a restart re-runs the query and skips what was delivered (R17.9) |
| Sink | ADBC bulk ingest per batch into `sink.table` (`append`, or `create_append` to create it from the first batch) |
| Delivery | source `at_least_once`; sink `at_least_once` |
| Tested against | the real DuckDB library (v1.5.6) in `connectors-p2`'s analytics shard, which fetches the release asset and checks its digest |

```yaml
driver: duckdb
database: { path: /data/warehouse.duckdb }
query: SELECT * FROM orders ORDER BY id
sink: { table: orders, mode: create_append }
```
