# Databricks

Native, `loams_connector_analytics::databricks` (CN2 Task 17), over the SQL
Statement Execution API (`/api/2.0/sql/statements`) and the Files API, through
the SDK's egress-checked HTTP client.

| | |
|---|---|
| Source | the statement runs with `format: ARROW_STREAM`, `disposition: EXTERNAL_LINKS`; each result chunk is an Arrow IPC stream behind a short-lived presigned link, read without the workspace token (the link carries its own credentials, and is never logged); one page per chunk |
| Sink | each batch as one Parquet file in a Unity Catalog volume (`/Volumes/…/<instance>/<batch id>.parquet`, uploaded with `overwrite=false`), then `COPY INTO <table> FROM '<file>' FILEFORMAT = PARQUET` (`append`) or `MERGE INTO … USING read_files(…) … UPDATE SET * / INSERT *` on `keys` (`merge`) |
| Delivery | source: `effectively_once`; sink: `effectively_once` in both modes (`COPY INTO` skips a file it has loaded; `MERGE` is keyed) |
| Auth | `token`: the name of a secret holding a personal access token or an OAuth access token (`Authorization: Bearer`) |
| Resume | a chunk cursor while the statement's result is kept; once it is closed, the query runs again and the delivered rows are skipped (R17.9) |
| Tested against | a local endpoint answering like the API in every build; a real SQL warehouse nightly; fixtures once Task 6's `FixtureProxy` carries binary (Arrow) bodies (R17.10) |

## Configuration

```yaml
host: https://adb-1234567890.12.azuredatabricks.net
warehouse_id: abcdef1234567890
token: databricks-token     # a secret name
query: SELECT * FROM main.sales.orders ORDER BY id
sink:
  table: main.sales.orders_copy
  mode: append
  volume: /Volumes/main/sales/landing/loams
```
