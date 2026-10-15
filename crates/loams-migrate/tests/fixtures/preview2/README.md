# preview.2 stack fixture (UP1 Task 0)

A small stack written by the `loams` engine of v0.1.0-preview.2. Task 5 (`0000-adopt-preview-2`) and later
tasks test against it. It was captured on 2026-10-15 from `dev` at `3ac1e866`, the state preview.2 is tagged
from (#518). The release images were not yet published, so the engine was built from source with the release
image's features.

## What is in it

| File | What |
|---|---|
| `loams_meta.jsonl` | Every key of TiKV keyspace `loams_meta` (the metastore), one `{"k": hex, "v": hex}` per line, in key order, from one snapshot. The root prefix is empty, as in the compose stack. |
| `loams_live_dev.jsonl` | The same for keyspace `loams_live_dev`. It holds only Live's own records: no Live app was deployed. |
| `durable/default.db` | The Durable SQLite store, checkpointed by a clean engine shutdown. `_sqlx_migrations` lists only `1 initial`. It holds promises `up1-p1` (pending) and `up1-p2` (resolved). |
| `bucket/` | The bucket objects the metastore's pointers name: the graph catalog document (`graphs/1/catalog/*.json`, the `graph-catalog` pointer), collection 1's manifest (`OPCM` 1) and its PK delta (`OPPD` 1). Lance, Tantivy, SlateDB and WAL objects are left out. |

The data:
- namespace `up1` (id 1);
- collection `docs` (id 1, 4 partitions) with documents 1 and 2, applied at manifest version 1;
- stream `events` (id 2) with records `k1=v1` and `k2=v2` on partition 0;
- owned graph `social` (`gr_01M4Y143G5T5EV2ZJDG3T46DDH`, implicit stream `_graph.gr_…`, id 3) with
  `(:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Bob'})`;
- two Durable promises.

The fixture holds no secret. The engine ran with `LOAMS_AUTH_MODE=off` on loopback and no API keys file. TiKV's
records hold no key material.

## How it was made

1. Build the engine with the release features (`deploy/release/Containerfile.loams`):

   ```sh
   cargo build -p loams --bin loams --features live,live-tikv,durable,graph,tikv
   ```

2. Start PD and TiKV v8.5.8 (and TiDB `v8.5.8-loams.1`) with `deploy/release/config/{pd,tikv,tidb}.toml`, on host
   networking at PD `127.0.0.1:41379`. Start the engine as the compose stack does, on loopback ports:

   ```sh
   LOAMS_AUTH_MODE=off loams standalone --bucket file://$D/bucket --data-dir $D \
     --graph-data-dir $D/graph --listen 127.0.0.1:43080 \
     --meta tikv://127.0.0.1:41379/loams_meta --live-store tikv://127.0.0.1:41379/loams_live_dev \
     --flight-sql-listen 127.0.0.1:43082 --qdrant-listen 127.0.0.1:43333 \
     --qdrant-grpc-listen 127.0.0.1:43334 --es-listen 127.0.0.1:43920 \
     --durable-listen 127.0.0.1:43001 --live-listen 127.0.0.1:43710
   ```

3. Write the data:
   - `POST /v1/namespaces {"name":"up1"}`.
   - `POST /v1/namespaces/up1/collections` with a schema of a text field `body`, a keyword `tenant` and a
     3-dimensional cosine vector `embedding`.
   - `POST …/collections/docs/documents` with two upserts.
   - `POST /v1/namespaces/up1/streams {"name":"events","partitions":1}`, then two records on partition 0.
   - `loams.graph.v1.GraphAdminService/CreateGraph` (owned), then `GraphService/Execute` with one `INSERT`.
   - Durable's `promise.create` twice and `promise.settle` once, on `--durable-listen`.

4. Stop the engine with SIGTERM, then dump the keyspaces:

   ```sh
   cargo run -p loams-tikv --example dump_keyspace -- 127.0.0.1:41379 loams_meta > loams_meta.jsonl
   cargo run -p loams-tikv --example dump_keyspace -- 127.0.0.1:41379 loams_live_dev > loams_live_dev.jsonl
   ```

   Copy `$D/durable/default.db` and the bucket objects listed above.

The same stack was then backed up with BR `backup txn`, restored into a fresh PD+TiKV, and dumped again. Both
dumps were byte-identical to these files (UP1 ruling T0-7).
