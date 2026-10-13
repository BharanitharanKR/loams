# CockroachDB connector

CockroachDB runs on the native Postgres family through the target profile `cockroachdb` (port 26257, database `defaultdb` by default). It speaks the Postgres wire protocol but has no logical replication and no binary `COPY … TO`, so two paths differ from [PostgreSQL](postgresql.md) (plan ruling R0.20):

- **CDC** (`mode: cdc`) is a sinkless changefeed over the SQL connection: `CREATE CHANGEFEED FOR TABLE … WITH updated, diff, resolved = '<resolved_ms>ms'`, plus `cursor = '<last resolved>'` on a restart. The deprecated `EXPERIMENTAL CHANGEFEED FOR` is not used. Rows of the initial scan carry `loamsop = r`; later ones `c`, `u` or `d` from the before and after images. The position is the last resolved timestamp the Fabric acknowledged, so a restart re-emits only changes after it, under the same ids (`<table>/<updated>/<key>`), which `ingest` deduplicates. `loamslsn` is the HLC `updated` timestamp. The cluster must have `kv.rangefeed.enabled = true`. A sinkless changefeed holds nothing on the server once its connection closes, so there is no slot to drop.
- **Batch reads** page in primary-key order with `SELECT … AS OF SYSTEM TIME <as-of>`, so a resumed read sees the same snapshot (within the GC TTL).
- **The sink** is the same upsert sink with `_loams_checkpoints`. CockroachDB's transaction retry errors (40001) replay the batch in a new transaction.

**Tested against:** CockroachDB v26.2.7, single node, insecure (`cockroachdb/cockroach`, compose service `cockroachdb`, profile `connectors-ci-only`). It is a `ci-service` under the CockroachDB Software License, run in CI only and never shipped (OR-3).

No narrowings are known; the suite adds any it finds to the profile with a `narrows:` note.
