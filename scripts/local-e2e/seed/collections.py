"""Collections and hybrid search, on DBpedia entities with OpenAI
text-embedding-3-small (512-d) vectors (Qdrant's public 100K set on Hugging
Face, data/dbpedia).

- native REST: namespace `e2e`, collection `dbpedia` (all rows): `title` and
  `body` BM25 text, `dbpedia_id` keyword, `letter` keyword, `embedding`
  (512, cosine); then vector, text and hybrid (RRF) queries, and SQL;
- Qdrant API (qdrant-client): collection `dbpedia_qdrant` in namespace
  `default`, the first QDRANT_ROWS rows, payload title/text; query_points;
- Elasticsearch API (elasticsearch-py): index `dbpedia-es` in namespace
  `default`, the first ES_ROWS rows with a dense_vector; match, knn, hybrid;
- Flight SQL (ADBC), Postgres wire (psycopg) and MySQL wire (PyMySQL) reads of
  `e2e.dbpedia`.

Idempotent: collections are created if missing, documents upserted by id.

    venv/bin/python -I scripts/local-e2e/seed/collections.py [--rows N]
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (ENGINE, ES, FLIGHT, MYSQLWIRE, NS, PGWIRE, QDRANT, Report,  # noqa: E402
                    data_path, log, namespace, ok, request, until)

import pyarrow.parquet as pq  # noqa: E402

PARQUET = ("dbpedia", "train-00000-of-00001.parquet")
VEC = "text-embedding-3-small-512-embedding"
SCHEMA = {
    "fields": [
        {"name": "title", "source_path": "title",
         "kind": {"text": {"analyzer": "standard", "positions": True}}, "indexed": True, "fast": False},
        {"name": "body", "source_path": "body",
         "kind": {"text": {"analyzer": "standard", "positions": True}}, "indexed": True, "fast": False},
        {"name": "dbpedia_id", "source_path": "dbpedia_id", "kind": "keyword", "indexed": True, "fast": True},
        {"name": "letter", "source_path": "letter", "kind": "keyword", "indexed": True, "fast": True},
        {"name": "body_chars", "source_path": "body_chars", "kind": "i64", "indexed": True, "fast": True},
    ],
    "vectors": [{"name": "embedding", "dim": 512, "distance": "cosine"}],
    "sparse_vectors": [],
    "dynamic": "ignore",
    "max_fields": 1000,
}
BASE = f"/v1/namespaces/{NS}/collections/dbpedia"


def rows(limit):
    """(id, title, text, dbpedia_id, vector) from the parquet file, in batches."""
    table = pq.ParquetFile(data_path(*PARQUET))
    n = 0
    for batch in table.iter_batches(batch_size=1000, columns=["_id", "title", "text", VEC]):
        d = batch.to_pydict()
        for i in range(len(d["_id"])):
            if n >= limit:
                return
            n += 1
            yield n, d["title"][i], d["text"][i], d["_id"][i], [float(x) for x in d[VEC][i]]


def native(report, limit, batch=400):
    namespace()
    status, reply = request("POST", f"{ENGINE}/v1/namespaces/{NS}/collections",
                            {"name": "dbpedia", "schema": SCHEMA, "partitions": 4})
    report.add("native: create collection e2e/dbpedia", status in (200, 201, 409), f"{status} {str(reply)[:200]}")
    ops, written, first_vec = [], 0, None
    for pk, title, text, dbid, vec in rows(limit):
        first_vec = first_vec or vec
        ops.append({"upsert": {"id": pk, "source": {
            "title": title, "body": text, "dbpedia_id": dbid,
            "letter": (title[:1] or "?").upper(), "body_chars": len(text)},
            "vectors": {"embedding": vec}}})
        if len(ops) == batch:
            written += write_ops(ops)
            ops = []
            if written % 10000 == 0:
                log(f"native: {written} documents")
    if ops:
        written += write_ops(ops)
    report.add("native: upsert documents", written == limit, f"{written} documents")
    count = report.attempt("native: count", lambda: ok("POST", f"{BASE}/documents/count", {}))
    log("count", count)
    q = first_vec
    report.attempt("native: vector query (k=5)", lambda: hits(ok("POST", f"/v1/namespaces/{NS}/query", {
        "collection": "dbpedia",
        "retrievers": [{"vector": {"field": "embedding", "query": q, "k": 5}}], "limit": 5})))
    report.attempt("native: text query 'river'", lambda: hits(ok("POST", f"/v1/namespaces/{NS}/query", {
        "collection": "dbpedia",
        "retrievers": [{"text": {"query": {"match": {"field": "body", "text": "river"}}, "k": 5}}], "limit": 5})))
    report.attempt("native: hybrid query (docs/engine-quickstart.md shape)",
                   lambda: hits(ok("POST", f"/v1/namespaces/{NS}/query", {
                       "from": "collections.dbpedia",
                       "retrieve": [{"vector": {"field": "embedding", "query": q, "k": 20}},
                                    {"text": {"field": "body", "query": "river", "k": 20}}],
                       "fuse": {"method": "rrf", "k": 60}, "limit": 5})))
    report.attempt("native: hybrid query (retrievers shape)",
                   lambda: hits(ok("POST", f"/v1/namespaces/{NS}/query", {
                       "collection": "dbpedia",
                       "retrievers": [{"fused": {"inputs": [
                           {"vector": {"field": "embedding", "query": q, "k": 20}},
                           {"text": {"query": {"match": {"field": "body", "text": "river"}}, "k": 20}}],
                           "fusion": {"rrf": {"k": 60}}, "k": 20}}], "limit": 5})))
    vec_sql = "[" + ",".join(f"{x:.6f}" for x in q) + "]"
    report.attempt("native: SQL rrf(vector_search, text_search)", lambda: ok("POST", f"/v1/namespaces/{NS}/sql", {
        "query": f"SELECT _id, title, _score FROM rrf(vector_search('dbpedia', {vec_sql}, 'embedding', 10), "
                 "text_search('dbpedia', 'river', 'body', 10)) LIMIT 3"}))
    report.attempt("native: SQL group by letter", lambda: ok("POST", f"/v1/namespaces/{NS}/sql", {
        "query": "SELECT letter, count(*) AS n FROM dbpedia GROUP BY letter ORDER BY n DESC LIMIT 5"}))
    return q


def write_ops(ops):
    for attempt in range(30):
        status, reply = request("POST", f"{ENGINE}{BASE}/documents", {"ops": ops}, timeout=300)
        if status in (200, 201):
            return len(ops)
        if status in (429, 503):  # backpressure: the collection's tail is over budget
            import time
            time.sleep(min(2 ** attempt, 20))
            continue
        raise AssertionError(f"documents: {status} {str(reply)[:400]}")
    raise AssertionError("documents: still refused after retries")


def hits(reply):
    return [(h.get("pk") or h.get("id"), round(h.get("score", 0), 4), (h.get("source") or {}).get("title"))
            for h in (reply or {}).get("hits", [])]


def qdrant(report, limit, q):
    from qdrant_client import QdrantClient, models
    client = QdrantClient(url=QDRANT, prefer_grpc=False, timeout=120, check_compatibility=False)
    name = "dbpedia_qdrant"
    if not client.collection_exists(name):
        client.create_collection(name, vectors_config=models.VectorParams(size=512, distance=models.Distance.COSINE))
    batch = []
    for pk, title, text, dbid, vec in rows(limit):
        batch.append(models.PointStruct(id=pk, vector=vec, payload={"title": title, "text": text, "dbpedia_id": dbid}))
        if len(batch) == 500:
            client.upsert(name, points=batch, wait=True)
            batch = []
    if batch:
        client.upsert(name, points=batch, wait=True)
    report.attempt("qdrant: count", lambda: client.count(name, exact=True).count)
    report.attempt("qdrant: query_points", lambda: [(p.id, round(p.score, 4), p.payload.get("title"))
                                                    for p in client.query_points(name, query=q, limit=5).points])
    report.attempt("qdrant: filtered query (title match)", lambda: [p.payload.get("title") for p in client.query_points(
        name, query=q, limit=3,
        query_filter=models.Filter(must=[models.FieldCondition(key="title", match=models.MatchText(text="river"))]),
    ).points])
    grpc = QdrantClient(url=QDRANT.replace("16333", "16334"), prefer_grpc=True, grpc_port=16334,
                        check_compatibility=False)
    report.attempt("qdrant gRPC: query_points", lambda: [p.id for p in grpc.query_points(name, query=q, limit=3).points])


def elasticsearch(report, limit, q):
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch(ES, request_timeout=120)
    report.attempt("es: info", lambda: es.info().body.get("version", {}).get("number"))
    index = "dbpedia-es"
    if not es.indices.exists(index=index):
        es.indices.create(index=index, mappings={"properties": {
            "title": {"type": "text"}, "text": {"type": "text"}, "dbpedia_id": {"type": "keyword"},
            "embedding": {"type": "dense_vector", "dims": 512, "index": True, "similarity": "cosine"}}})
    actions = ({"_index": index, "_id": str(pk), "_source": {"title": t, "text": x, "dbpedia_id": d, "embedding": v}}
               for pk, t, x, d, v in rows(limit))
    # The gateway refuses bodies over 2 MB (axum's default; bug: its configured
    # http.max_content_length is 100 MiB), so chunks stay under 1.5 MB.
    n, errors = helpers.bulk(es, actions, chunk_size=500, max_chunk_bytes=1_500_000, raise_on_error=False)
    report.add("es: _bulk", not errors, f"{n} indexed, {len(errors) if errors else 0} errors {str(errors)[:200]}")
    es.indices.refresh(index=index)
    report.attempt("es: _count", lambda: es.count(index=index).body["count"])
    report.attempt("es: match 'river'", lambda: [(h["_id"], h["_source"]["title"]) for h in es.search(
        index=index, query={"match": {"text": "river"}}, size=3, source_excludes=["embedding"]).body["hits"]["hits"]])
    report.attempt("es: knn", lambda: [(h["_id"], h["_source"]["title"]) for h in es.search(
        index=index, knn={"field": "embedding", "query_vector": q, "k": 5, "num_candidates": 50}, size=5,
        source_excludes=["embedding"]).body["hits"]["hits"]])
    report.attempt("es: hybrid (query + knn)", lambda: [(h["_id"], h["_source"]["title"]) for h in es.search(
        index=index, query={"match": {"text": "river"}},
        knn={"field": "embedding", "query_vector": q, "k": 5, "num_candidates": 50}, size=5,
        source_excludes=["embedding"]).body["hits"]["hits"]])


def wire(report):
    def flight():
        import adbc_driver_flightsql.dbapi as flight_sql
        with flight_sql.connect(FLIGHT, db_kwargs={"adbc.flight.sql.rpc.call_header.loams-namespace": NS}) as conn, \
                conn.cursor() as cur:
            cur.execute("SELECT letter, count(*) AS n FROM dbpedia GROUP BY letter ORDER BY n DESC LIMIT 3")
            return cur.fetch_arrow_table().to_pylist()
    report.attempt("flight sql: group by", flight)

    def pg():
        import psycopg
        host, port = PGWIRE.split(":")
        with psycopg.connect(host=host, port=int(port), user="loams", dbname=NS, connect_timeout=10) as conn:
            return conn.execute("SELECT _id, title FROM dbpedia ORDER BY _id LIMIT 3").fetchall()
    report.attempt("pg wire: select", pg)

    def my():
        import pymysql
        host, port = MYSQLWIRE.split(":")
        with pymysql.connect(host=host, port=int(port), user="loams", password="", database=NS,
                             connect_timeout=10) as conn, conn.cursor() as cur:
            cur.execute("SELECT _id, title FROM dbpedia ORDER BY _id LIMIT 3")
            return cur.fetchall()
    report.attempt("mysql wire: select (PyMySQL)", my)

    def my_cli():
        import subprocess
        host, port = MYSQLWIRE.split(":")
        return subprocess.run(["mysql", f"-h{host}", f"-P{port}", "-uloams", NS, "-N", "-e",
                               "SELECT letter, count(*) FROM dbpedia GROUP BY letter ORDER BY 2 DESC LIMIT 3"],
                              check=True, capture_output=True, text=True, timeout=60).stdout.strip()
    report.attempt("mysql wire: select (mysql CLI)", my_cli)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--qdrant-rows", type=int, default=20_000)
    p.add_argument("--es-rows", type=int, default=20_000)
    p.add_argument("--only", choices=["native", "qdrant", "es", "wire"])
    a = p.parse_args()
    report = Report("collections")
    q = next(rows(1))[4]
    if a.only in (None, "native"):
        report.attempt("native", lambda: native(report, a.rows) and "done")
    if a.only in (None, "qdrant"):
        report.attempt("qdrant", lambda: qdrant(report, a.qdrant_rows, q) or "done")
    if a.only in (None, "es"):
        report.attempt("es", lambda: elasticsearch(report, a.es_rows, q) or "done")
    if a.only in (None, "wire"):
        until("the engine answers", lambda: request("GET", f"{ENGINE}/health")[0] == 200, within=10)
        wire(report)
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
