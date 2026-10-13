# Amazon Redshift

Native, `loams_connector_analytics::redshift` (CN2 Task 17), over the Redshift
Data API: JSON over HTTPS, signed with SigV4 by the family crate (R17.4), so the
AWS SDK is not compiled. S3 (the bulk path and the sink's staging) goes through
`object_store` 0.13.2 with the egress-checked client.

| | |
|---|---|
| Source (rows) | `ExecuteStatement`, `DescribeStatement` until finished, `GetStatementResult` page by page (`NextToken`); typed by the column metadata through the shared SQL mapper |
| Source (bulk, Arrow) | with `unload`: `UNLOAD (query) TO 's3://…/<instance>/<run>/' IAM_ROLE … FORMAT AS PARQUET ALLOWOVERWRITE`, then each Parquet file as Arrow, one page per file |
| Sink | each batch as one Parquet file under `sink.staging` (named by the batch's id), then one `BatchExecuteStatement` (a transaction): a temporary table shaped like the batch, `COPY … FORMAT AS PARQUET`, then `INSERT … SELECT` (`append`) or `MERGE` on `keys` (`merge`); the staged file is deleted after |
| Delivery | source: `effectively_once` (§33 §6 run ids); sink: `at_least_once` (`append`), `effectively_once` (`merge`) |
| Auth | `auth.access_key_id`, `auth.secret_access_key`, `auth.session_token` (secret names); `cluster_identifier` + `db_user`/`secret_arn`, or `workgroup_name` (Serverless) |
| Resume | a page cursor (statement id and `NextToken`) while Redshift keeps the result (24 h); after that the query runs again and the rows already delivered are skipped (exact with an `ORDER BY`, R17.9) |
| Tested against | a local endpoint answering like the Data API and an in-memory bucket in every build; a real Serverless workgroup nightly; fixtures recorded from it once Task 6's `FixtureProxy` carries binary bodies (R17.10) |

## Configuration

```yaml
region: us-east-1
workgroup_name: analytics
database: dev
auth: { access_key_id: aws-key-id, secret_access_key: aws-secret }
query: SELECT * FROM sales ORDER BY id
unload: { s3_uri: s3://loams-unload/redshift, iam_role: arn:aws:iam::123456789012:role/unload }
sink:
  table: public.orders
  mode: merge
  keys: [id]
  staging: { s3_uri: s3://loams-stage/redshift, iam_role: arn:aws:iam::123456789012:role/copy }
```

Types: `int2/4/8`, `float4/8`, `numeric(p,s)`, `bool`, `date`, `timestamptz`
map exactly; `varchar`/`char`/`text` are `Utf8`; `super` is JSON text; a
`timestamp` without a zone is lossy (list it under `lossy`). Other types pause the
instance (`SchemaIncompatible`).
