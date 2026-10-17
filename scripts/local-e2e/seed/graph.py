"""Loams Graph: two owned graphs in namespace `e2e`, loaded with GQL.

- `github`: who did what to which repository in one hour of GH Archive
  (data/gharchive): (:Actor {login})-[:PUSHED|STARRED|FORKED|OPENED_PR|
  OPENED_ISSUE|COMMENTED|RELEASED|CREATED {n}]->(:Repo {name}) and
  (:Repo)-[:IN_ORG]->(:Org {login});
- `email_eu`: SNAP's email-Eu-core network (data/snap-email): 1005
  (:Person {id, dept}) and 25571 [:EMAILED] edges.

Rows go in with `UNWIND $rows AS r INSERT ...` (parameters, never
interpolated). Idempotent: an existing graph is left as it is (pass --reload
to delete and load it again).

    venv/bin/python -I scripts/local-e2e/seed/graph.py [--events N]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import NS, HttpError, Report, connect, data_path, log, namespace  # noqa: E402

VERB = {"PushEvent": "PUSHED", "WatchEvent": "STARRED", "ForkEvent": "FORKED", "PullRequestEvent": "OPENED_PR",
        "IssuesEvent": "OPENED_ISSUE", "IssueCommentEvent": "COMMENTED", "PullRequestReviewCommentEvent": "COMMENTED",
        "ReleaseEvent": "RELEASED", "CreateEvent": "CREATED", "DeleteEvent": "DELETED"}


def val(x):
    if isinstance(x, bool):
        return {"boolean": x}
    if isinstance(x, int):
        return {"int64": str(x)}
    if isinstance(x, float):
        return {"float64": x}
    if isinstance(x, str):
        return {"string": x}
    if isinstance(x, dict):
        return {"map": {"entries": {k: val(v) for k, v in x.items()}}}
    if isinstance(x, (list, tuple)):
        return {"list": {"values": [val(v) for v in x]}}
    raise TypeError(type(x))


def plain(v):
    if not isinstance(v, dict) or not v:
        return v
    (k, x), = v.items()
    if k == "list":
        return [plain(i) for i in x.get("values", [])]
    if k == "map":
        return {a: plain(b) for a, b in x.get("entries", {}).items()}
    if k == "node":
        return {"labels": x.get("labels"), **{a: plain(b) for a, b in x.get("properties", {}).items()}}
    return x


def execute(graph, statement, params=None, max_rows=0):
    reply = connect("loams.graph.v1.GraphService/Execute", {
        "namespace": NS, "graph": graph, "statement": statement,
        "parameters": {k: val(v) for k, v in (params or {}).items()}, "maxRows": max_rows}, timeout=600)
    rows = (reply.get("rows") or {}).get("rows", [])
    return [[plain(v) for v in r.get("values", [])] for r in rows], reply.get("counters")


def ensure(report, name, reload):
    try:
        connect("loams.graph.v1.GraphAdminService/GetGraph", {"namespace": NS, "name": name})
        if not reload:
            report.add(f"graph {name} exists", True, "kept (pass --reload to load it again)")
            return False
        connect("loams.graph.v1.GraphAdminService/DeleteGraph", {"namespace": NS, "name": name})
    except HttpError as e:
        if e.status not in (404,):
            raise
    connect("loams.graph.v1.GraphAdminService/CreateGraph",
            {"namespace": NS, "name": name, "idempotencyKey": f"e2e-{name}"})
    report.add(f"create graph {name}", True)
    return True


def chunks(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def load(graph, statement, rows, batch=500):
    done = 0
    for part in chunks(rows, batch):
        execute(graph, statement, {"rows": part})
        done += len(part)
        if done % 5000 < batch:
            log(f"{graph}: {done}/{len(rows)}")
    return done


def github(report, limit, reload):
    if not ensure(report, "github", reload):
        return
    actors, repos, orgs, edges, in_org = set(), set(), set(), Counter(), set()
    with gzip.open(data_path("gharchive", "2024-01-15-15.json.gz"), "rt", encoding="utf-8") as f:
        for n, line in enumerate(f):
            if n >= limit:
                break
            e = json.loads(line)
            verb = VERB.get(e["type"])
            if not verb:
                continue
            a, r = e["actor"]["login"], e["repo"]["name"]
            actors.add(a)
            repos.add(r)
            edges[(a, verb, r)] += 1
            org = (e.get("org") or {}).get("login")
            if org:
                orgs.add(org)
                in_org.add((r, org))
    log(f"github: {len(actors)} actors, {len(repos)} repos, {len(orgs)} orgs, {len(edges)} edges")
    report.attempt("github: actors", lambda: load("github", "UNWIND $rows AS r INSERT (:Actor {login: r})", sorted(actors)))
    report.attempt("github: repos", lambda: load("github", "UNWIND $rows AS r INSERT (:Repo {name: r})", sorted(repos)))
    report.attempt("github: orgs", lambda: load("github", "UNWIND $rows AS r INSERT (:Org {login: r})", sorted(orgs)))
    by_verb = {}
    for (a, verb, r), c in edges.items():
        by_verb.setdefault(verb, []).append({"a": a, "r": r, "n": c})
    for verb, rows in sorted(by_verb.items()):
        report.attempt(f"github: {verb} edges", lambda verb=verb, rows=rows: load(
            "github", f"UNWIND $rows AS e MATCH (a:Actor {{login: e.a}}), (r:Repo {{name: e.r}}) "
                      f"INSERT (a)-[:{verb} {{n: e.n}}]->(r)", rows, batch=250))
    report.attempt("github: IN_ORG edges", lambda: load(
        "github", "UNWIND $rows AS e MATCH (r:Repo {name: e.r}), (o:Org {login: e.o}) INSERT (r)-[:IN_ORG]->(o)",
        [{"r": r, "o": o} for r, o in sorted(in_org)], batch=250))


def email(report, reload):
    if not ensure(report, "email_eu", reload):
        return
    dept = {}
    with gzip.open(data_path("snap-email", "email-Eu-core-department-labels.txt.gz"), "rt") as f:
        for line in f:
            a, b = line.split()
            dept[int(a)] = int(b)
    edges = []
    with gzip.open(data_path("snap-email", "email-Eu-core.txt.gz"), "rt") as f:
        for line in f:
            a, b = line.split()
            edges.append({"a": int(a), "b": int(b)})
    report.attempt("email_eu: persons", lambda: load(
        "email_eu", "UNWIND $rows AS r INSERT (:Person {id: r.id, dept: r.dept})",
        [{"id": k, "dept": v} for k, v in sorted(dept.items())]))
    report.attempt("email_eu: EMAILED edges", lambda: load(
        "email_eu", "UNWIND $rows AS e MATCH (a:Person {id: e.a}), (b:Person {id: e.b}) INSERT (a)-[:EMAILED]->(b)",
        edges, batch=500))


def checks(report):
    q = [
        ("github", "MATCH (a:Actor) RETURN count(a)"),
        ("github", "MATCH (a:Actor)-[e:PUSHED]->(r:Repo) RETURN r.name, count(a) AS pushers ORDER BY pushers DESC LIMIT 5"),
        ("github", "MATCH (a:Actor)-[:STARRED]->(r:Repo)<-[:STARRED]-(b:Actor) WHERE a.login < b.login "
                   "RETURN a.login, b.login, r.name LIMIT 3"),
        ("email_eu", "MATCH (p:Person) RETURN count(p)"),
        ("email_eu", "MATCH (a:Person)-[:EMAILED]->(b:Person) WHERE a.dept <> b.dept RETURN count(*)"),
        ("email_eu", "MATCH (p:Person)-[:EMAILED]->(q) RETURN p.id, count(q) AS out ORDER BY out DESC LIMIT 5"),
    ]
    for graph, stmt in q:
        report.attempt(f"{graph}: {stmt[:70]}", lambda graph=graph, stmt=stmt: execute(graph, stmt, max_rows=10)[0])
    report.attempt("ListGraphs", lambda: [g.get("name") for g in connect(
        "loams.graph.v1.GraphAdminService/ListGraphs", {"namespace": NS}).get("graphs", [])])
    report.attempt("GetSchema github", lambda: str(connect(
        "loams.graph.v1.GraphAdminService/GetSchema", {"namespace": NS, "name": "github"}))[:300])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=60_000)
    p.add_argument("--reload", action="store_true")
    a = p.parse_args()
    report = Report("graph")
    namespace()
    report.attempt("github", lambda: github(report, a.events, a.reload) or "done")
    report.attempt("email_eu", lambda: email(report, a.reload) or "done")
    checks(report)
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
