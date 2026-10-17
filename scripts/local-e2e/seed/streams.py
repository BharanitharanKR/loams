"""Streams and links, replaying one hour of the public GitHub event timeline
(GH Archive, data/gharchive/2024-01-15-15.json.gz).

- stream `gh-events` (4 partitions): every event, key = the repository, value
  = the event as compact JSON (actor, repo, type, time, a short text);
- stream `gh-type-deltas` with the counter link `gh-event-types`: one record
  per event, key = its type, value `1` (events per type);
- stream `gh-repo-deltas` with the counter link `gh-repo-activity` (events per
  repository);
- collection `gh_events` (namespace `e2e`): the events with text (commit
  messages, issue and PR titles), written through the documents API, which is
  the collection's own stream and link.

Idempotent per run only in the collection (upserts by event id): streams are
append-only, so a second run appends the hour again (counters double). Pass
--skip-streams to rerun the collection part alone.

    venv/bin/python -I scripts/local-e2e/seed/streams.py [--events N]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import ENGINE, NS, Report, b64, data_path, log, namespace, ok, request, until  # noqa: E402

FILE = ("gharchive", "2024-01-15-15.json.gz")
PARTS = 4


def text_of(e):
    p = e.get("payload") or {}
    if e["type"] == "PushEvent":
        return " | ".join((c.get("message") or "")[:200] for c in (p.get("commits") or [])[:5])
    for k in ("pull_request", "issue", "release"):
        if isinstance(p.get(k), dict):
            return (p[k].get("title") or p[k].get("name") or "")[:300]
    if isinstance(p.get("comment"), dict):
        return (p["comment"].get("body") or "")[:300]
    return p.get("description") or ""


def events(limit):
    with gzip.open(data_path(*FILE), "rt", encoding="utf-8") as f:
        for n, line in enumerate(f):
            if n >= limit:
                return
            e = json.loads(line)
            yield {"id": int(e["id"]), "type": e["type"], "actor": e["actor"]["login"],
                   "repo": e["repo"]["name"], "org": (e.get("org") or {}).get("login"),
                   "created_at": e["created_at"], "public": e.get("public", True), "text": text_of(e)}


def create_stream(report, name, partitions):
    status, reply = request("POST", f"{ENGINE}/v1/namespaces/{NS}/streams",
                            {"name": name, "partitions": partitions})
    report.add(f"create stream {name}", status in (200, 201, 409), f"{status} {str(reply)[:160]}")


def create_link(report, name, source):
    status, reply = request("POST", f"{ENGINE}/v1/namespaces/{NS}/links", {"name": name, "source": source})
    report.add(f"create counter link {name} <- {source}", status in (200, 201, 409), f"{status} {str(reply)[:160]}")


def produce(stream, partition, pairs):
    body = {"records": [{"key": b64(k), "value": b64(v)} for k, v in pairs]}
    ok("POST", f"/v1/namespaces/{NS}/streams/{stream}/partitions/{partition}/records", body, timeout=300)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=10**9)
    p.add_argument("--skip-streams", action="store_true")
    a = p.parse_args()
    report = Report("streams")
    namespace()
    if not a.skip_streams:
        for name, parts in (("gh-events", PARTS), ("gh-type-deltas", 1), ("gh-repo-deltas", 1)):
            create_stream(report, name, parts)
        create_link(report, "gh-event-types", "gh-type-deltas")
        create_link(report, "gh-repo-activity", "gh-repo-deltas")

    status, reply = request("POST", f"{ENGINE}/v1/namespaces/{NS}/collections", {
        "name": "gh_events", "partitions": 4, "schema": {
            "fields": [
                {"name": "type", "source_path": "type", "kind": "keyword", "indexed": True, "fast": True},
                {"name": "actor", "source_path": "actor", "kind": "keyword", "indexed": True, "fast": True},
                {"name": "repo", "source_path": "repo", "kind": "keyword", "indexed": True, "fast": True},
                {"name": "org", "source_path": "org", "kind": "keyword", "indexed": True, "fast": True},
                {"name": "created_at", "source_path": "created_at", "kind": "date", "indexed": True, "fast": True},
                {"name": "text", "source_path": "text",
                 "kind": {"text": {"analyzer": "standard", "positions": True}}, "indexed": True, "fast": False},
            ], "vectors": [], "sparse_vectors": [], "dynamic": "ignore", "max_fields": 1000}})
    report.add("create collection gh_events", status in (200, 201, 409), f"{status} {str(reply)[:200]}")

    by_part = {i: [] for i in range(PARTS)}
    types, repos, docs = [], [], []
    total = 0
    expected_types = {}

    def flush(force=False):
        nonlocal by_part, types, repos, docs
        if not a.skip_streams:
            for part, pairs in by_part.items():
                if pairs and (force or len(pairs) >= 1000):
                    produce("gh-events", part, pairs)
                    by_part[part] = []
            if types and (force or len(types) >= 4000):
                produce("gh-type-deltas", 0, types)
                produce("gh-repo-deltas", 0, repos)
                types, repos = [], []
        if docs and (force or len(docs) >= 1000):
            ops = [{"upsert": {"id": d["id"], "source": {k: v for k, v in d.items() if k != "id"}}} for d in docs]
            for attempt in range(20):
                status, reply = request("POST", f"{ENGINE}/v1/namespaces/{NS}/collections/gh_events/documents",
                                        {"ops": ops}, timeout=300)
                if status in (200, 201):
                    break
                if status in (429, 503):
                    import time
                    time.sleep(min(2 ** attempt, 20))
                    continue
                raise AssertionError(f"gh_events documents: {status} {str(reply)[:300]}")
            docs = []

    for e in events(a.events):
        total += 1
        value = json.dumps(e, separators=(",", ":"))
        by_part[zlib.crc32(e["repo"].encode()) % PARTS].append((e["repo"], value))
        types.append((e["type"], "1"))
        repos.append((e["repo"], "1"))
        expected_types[e["type"]] = expected_types.get(e["type"], 0) + 1
        docs.append(e)
        flush()
        if total % 20000 == 0:
            log(f"{total} events")
    flush(force=True)
    report.add("replayed events", total > 0, f"{total} events, {len(expected_types)} types")

    if not a.skip_streams:
        def counters():
            reply = ok("GET", f"/v1/namespaces/{NS}/links/gh-event-types")
            got = reply.get("counters") or {}
            return got if all(got.get(k, 0) >= v for k, v in expected_types.items()) else None
        report.attempt("link gh-event-types caught up", lambda: sorted(until(
            "the counters reach the replayed counts", counters, within=600, every=5).items(),
            key=lambda kv: -int(kv[1]))[:5])
        report.attempt("list streams", lambda: [s.get("name") for s in (ok("GET", f"/v1/namespaces/{NS}/streams") or {}).get("streams", [])])
        report.attempt("list links", lambda: [lk.get("name") for lk in (ok("GET", f"/v1/namespaces/{NS}/links") or {}).get("links", [])])
        report.attempt("fetch gh-events p0", lambda: len(ok(
            "GET", f"/v1/namespaces/{NS}/streams/gh-events/partitions/0/records?offset=0&max_bytes=65536").get("records", [])))
    report.attempt("gh_events count", lambda: ok("POST", f"/v1/namespaces/{NS}/collections/gh_events/documents/count", {}))
    report.attempt("gh_events text search 'fix bug'", lambda: [h.get("pk") for h in ok("POST", f"/v1/namespaces/{NS}/query", {
        "collection": "gh_events", "retrievers": [{"text": {"query": {"match": {"field": "text", "text": "fix bug"}}, "k": 5}}],
        "limit": 5}).get("hits", [])])
    report.attempt("SQL: events per type", lambda: ok("POST", f"/v1/namespaces/{NS}/sql", {
        "query": "SELECT type, count(*) AS n FROM gh_events GROUP BY type ORDER BY n DESC LIMIT 5"}))
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
