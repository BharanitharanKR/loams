# Engine quick start

Build the `loams` binary from source and try its APIs on one machine. To run the whole preview stack
(engine, TiKV, TiDB, Postgres, House) instead, see [Running the preview](release/preview.md).

## Prerequisites

- **Rust.** Install [rustup](https://rustup.rs); the toolchain pinned in [`rust-toolchain.toml`](../rust-toolchain.toml) installs on first build.
- **protoc**, the Protocol Buffers compiler: `apt install protobuf-compiler`, `brew install protobuf`, or `pacman -S protobuf`.
- **Memory.** The first build compiles DataFusion, Lance and Tantivy, so it is heavy. On machines with less than 32 GB of RAM, limit parallel jobs with `-j 4`.

On **Windows** and **macOS** there are extra prerequisites (the MSVC Build Tools and NASM on Windows); see [the build-from-source guides](build-from-source/README.md). Prebuilt `loams` binaries for Linux, macOS and Windows are on the [GitHub releases](https://github.com/ostrium-labs/loams/releases).

## Run a dev server

`loams dev` runs everything in one process, with data in a local directory (`.loams/` by default):

```sh
cargo run --release -p loams -- dev
```

It serves these listeners, each of which you can move or turn off (`loams dev --help`):

| Surface | Default address | Flag |
|---|---|---|
| Native HTTP API | `127.0.0.1:8080` | `--listen` |
| Arrow Flight SQL | `127.0.0.1:8082` | `--flight-sql-listen`, `--no-flight-sql` |
| Qdrant REST and gRPC | `127.0.0.1:6333`, `127.0.0.1:6334` | `--qdrant-listen`, `--qdrant-grpc-listen`, `--no-qdrant` |
| Elasticsearch REST | `127.0.0.1:9200` | `--es-listen`, `--no-es` |
| Resonate (with `--features durable`) | `127.0.0.1:8001` | `--durable-listen`, `--no-durable` |

To run against a bucket instead of a local directory, use `loams standalone --bucket s3://bucket/prefix`. For a multi-node deployment, use `loams cluster`.

## Hybrid search with the native API

Create a collection, write documents and run a hybrid query. Every write returns a consistency token, and reads are strong by default, so they see the write at once.

```sh
curl -s localhost:8080/v1/namespaces/demo/collections -H 'content-type: application/json' -d '{
  "name": "kb",
  "schema": {
    "fields": [
      {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
      {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true}
    ],
    "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
    "sparse_vectors": [], "dynamic": "ignore", "max_fields": 1000
  }
}'

curl -s localhost:8080/v1/namespaces/demo/collections/kb/documents -H 'content-type: application/json' -d '{"ops": [
  {"upsert": {"id": 1, "source": {"body": "refund policy", "tenant": "a"}, "vectors": {"embedding": [1.0, 0.0, 0.0]}}},
  {"upsert": {"id": 2, "source": {"body": "shipping times", "tenant": "a"}, "vectors": {"embedding": [0.9, 0.1, 0.0]}}},
  {"upsert": {"id": 3, "source": {"body": "refund window", "tenant": "b"}, "vectors": {"embedding": [0.0, 0.0, 1.0]}}}
]}'

curl -s localhost:8080/v1/namespaces/demo/query -H 'content-type: application/json' -d '{
  "from": "collections.kb",
  "retrieve": [
    {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}},
    {"text": {"field": "body", "query": "refund", "k": 10}}
  ],
  "filter": {"term": {"tenant": "a"}},
  "fuse": {"method": "rrf", "k": 60},
  "limit": 3
}'
```

The same query in SQL:

```sh
curl -s localhost:8080/v1/namespaces/demo/sql -H 'content-type: application/json' -d @- <<'JSON'
{"query": "SELECT _id, body, _score FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), text_search('kb', 'refund', 'body', 10)) LIMIT 3"}
JSON
```

## Use an existing Qdrant client

```python
# pip install qdrant-client
from qdrant_client import QdrantClient, models

client = QdrantClient(url="http://localhost:6333")
client.create_collection("docs", vectors_config=models.VectorParams(size=3, distance=models.Distance.COSINE))
client.upsert("docs", points=[models.PointStruct(id=1, vector=[1.0, 0.0, 0.0], payload={"title": "refunds"})], wait=True)
print(client.query_points("docs", query=[1.0, 0.0, 0.0], limit=1))
```

Requests without a `Loams-Namespace` header go to the `default` namespace. The Elasticsearch API on port 9200 works the same way with `elasticsearch-py`.

## Query over Flight SQL

```python
# pip install adbc-driver-flightsql pyarrow
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect(
    "grpc://127.0.0.1:8082",
    db_kwargs={"adbc.flight.sql.rpc.call_header.loams-namespace": "demo"},
) as conn, conn.cursor() as cur:
    cur.execute("SELECT _id, body FROM kb ORDER BY _id")
    print(cur.fetch_arrow_table())
```

## Read the bucket directly

`POST /v1/namespaces/{ns}/collections/{c}/scan` returns a pinned Lance version, which pylance reads straight from the bucket (`pip install pylance==12.0.0`):

```python
import json, urllib.request
import lance

request = urllib.request.Request(
    "http://127.0.0.1:8080/v1/namespaces/demo/collections/kb/scan",
    data=b"{}", headers={"content-type": "application/json"},
)
plan = json.load(urllib.request.urlopen(request))
if plan["lance"] is not None:  # null until the first commit
    dataset = lance.dataset(plan["lance"]["uri"], version=plan["lance"]["version"])
    print(dataset.count_rows(), "live documents; tail:", plan["tail_records"], "records")
```

Writes that the pinned version does not hold yet (the plan's *tail*) are readable through the native API or Flight SQL.

## API surfaces

| Surface | Protocol | Use it for | Code |
|---|---|---|---|
| Native API | REST (JSON) | Namespaces, streams, collections, links, hybrid query, SQL, scan plans | [`crates/loams/src/api`](../crates/loams/src/api) |
| Flight SQL | Arrow Flight over gRPC | SQL from any ADBC or Flight SQL client, with Arrow results | [`crates/loams`](../crates/loams) |
| Qdrant | REST and gRPC | Existing Qdrant clients and framework integrations | [`crates/loams-qdrant`](../crates/loams-qdrant) |
| Elasticsearch subset | REST (JSON, NDJSON) | Existing Elasticsearch 8 clients, LangChain and LlamaIndex stores | [`crates/loams-es`](../crates/loams-es) |
| Resonate | HTTP | Durable promises and tasks from the Resonate SDKs (TypeScript, Python, Rust, Go, Java) | [`crates/loams-durable`](../crates/loams-durable) |
| Loams Live | Connect / gRPC (`loams.live.v1`) | Reactive documents and live queries (in progress) | [`proto/loams`](../proto/loams), [`sdks/typescript`](../sdks/typescript) |
| Console API | REST (OpenAPI) | The web console (in progress, served by a mock for now) | [`api/console`](../api/console) |

Each gateway documents where it differs from the original in its crate's module docs.
