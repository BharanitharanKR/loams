#!/usr/bin/env python3
"""The reference for `Search.expand` on Loams Graph (GR1 Task 20; design §07 §4, §5.2).

Builds the fixture graph (`expand_fixture.json`) and computes, with networkx, what every case of
it must answer, before and after the fixture's deletes (`expand_expected.json`). The Rust test
`expand_matches_networkx_reference` (crates/loams-graph/tests/retrieval.rs) loads both and checks
`LoamsGraphExpander` against them; CI runs only that test, so networkx is needed only to
regenerate:

    python3 -m venv /tmp/nx && /tmp/nx/bin/pip install networkx==3.4.2
    /tmp/nx/bin/python conformance/graph/reference/expand.py

The semantics, as §07 §4 and the plan's rulings (R20.x) state them:

* a vertex is (label, _key); a seed is every vertex with the seed's key (of `seed_label` if given);
* each seed's reached vertices are kept at their smallest hop; among equal-hop paths, the one with
  the smallest canonical key bytes of the previous vertex, then of the edge;
* `edge_filter` prunes edges walked, `vertex_filter` the vertices reached (neither answered nor
  walked through); a seed's own vertices are never its neighbors;
* per seed: hop ascending, then canonical key bytes, then label, truncated to `limit_per_seed`;
  then the whole answer in that order (seed order breaking ties) truncated to `limit`.
"""

import json
import random
import sys
from pathlib import Path

import networkx as nx

HERE = Path(__file__).resolve().parent


def canon(key):
    """`PrimaryKey::canonical`: 0x01 + u64 big-endian, or 0x03 + UTF-8."""
    if isinstance(key, int):
        return b"\x01" + key.to_bytes(8, "big")
    return b"\x03" + key.encode()


def get(props, field):
    value = props
    for segment in field.split("."):
        if not isinstance(value, dict) or segment not in value:
            return None
        value = value[segment]
    return value


def each(value):
    return value if isinstance(value, list) else [value]


def cmp(value, wanted):
    if isinstance(wanted, bool) or isinstance(value, bool):
        if isinstance(wanted, bool) and isinstance(value, bool):
            return (value > wanted) - (value < wanted)
        return None
    if isinstance(wanted, str):
        return (value > wanted) - (value < wanted) if isinstance(value, str) else None
    if isinstance(value, (int, float)):
        return (value > wanted) - (value < wanted)
    return None


def matches(query, props):
    (kind, body), = query.items() if isinstance(query, dict) else ((query, None),)
    if kind == "match_all":
        return True
    if kind == "match_none":
        return False
    if kind == "term":
        value = get(props, body["field"])
        return value is not None and any(cmp(v, body["value"]) == 0 for v in each(value))
    if kind == "terms":
        value = get(props, body["field"])
        return value is not None and any(
            cmp(v, w) == 0 for v in each(value) for w in body["values"]
        )
    if kind == "range":
        value = get(props, body["field"])
        if value is None:
            return False

        def ok(v):
            for bound, accept in (("gt", (1,)), ("gte", (0, 1)), ("lt", (-1,)), ("lte", (-1, 0))):
                if body.get(bound) is not None and cmp(v, body[bound]) not in accept:
                    return False
            return True

        return any(ok(v) for v in each(value))
    if kind == "exists":
        return get(props, body["field"]) is not None
    if kind == "is_null":
        return get(props, body["field"]) is None
    if kind == "prefix":
        value = get(props, body["field"])
        return value is not None and any(
            isinstance(v, str) and v.startswith(body["value"]) for v in each(value)
        )
    if kind == "bool":
        if not all(matches(q, props) for q in body.get("must", []) + body.get("filter", [])):
            return False
        if any(matches(q, props) for q in body.get("must_not", [])):
            return False
        should = body.get("should", [])
        if not should:
            return True
        wanted = body.get("minimum_should_match")
        wanted = int(wanted) if wanted is not None else int(not body.get("must") and not body.get("filter"))
        return sum(matches(q, props) for q in should) >= wanted
    raise ValueError(f"not in the served subset: {kind}")


def fixture():
    """A graph with both key types, three edge types, parallel edges, a self-loop, cycles and a hub."""
    rng = random.Random(20)
    nodes = []
    for i in range(12):
        nodes.append({"label": "entity", "key": f"e{i:02}", "props": {
            "name": f"E{i}", "kind": ["PERSON", "ORG", "PLACE"][i % 3], "rank": i}})
    for i in range(6):
        nodes.append({"label": "chunk", "key": 100 + i, "props": {"text": f"chunk {i}"}})
    # A key shared by two labels: a seed without a label reaches from both.
    nodes.append({"label": "chunk", "key": "e00", "props": {"text": "shadow"}})
    edges = []
    n = 0

    def edge(ty, src, dst, **props):
        nonlocal n
        n += 1
        edges.append({"key": f"r{n:03}", "type": ty, "src": src, "dst": dst, "props": props})

    entity = lambda i: ["entity", f"e{i:02}"]
    chunk = lambda i: ["chunk", 100 + i]
    for _ in range(26):
        a, b = rng.randrange(12), rng.randrange(12)
        edge("RELATED", entity(a), entity(b), weight=round(rng.random(), 3),
             **({"valid_to": 2020} if rng.random() < 0.25 else {}))
    for i in range(6):
        for j in rng.sample(range(12), 3):
            edge("MENTIONS", chunk(i), entity(j))
    # The hub: e05 knows everyone.
    for j in range(12):
        if j != 5:
            edge("KNOWS", entity(5), entity(j))
    edge("KNOWS", entity(3), entity(3))  # a self-loop
    edge("KNOWS", entity(1), entity(2))  # parallel edges: the smaller edge key wins
    edge("KNOWS", entity(1), entity(2))
    edge("MENTIONS", ["chunk", "e00"], entity(4))
    cases = [
        {"name": "out_one_hop", "seeds": ["e00", "e01"], "hops": 1, "direction": "out"},
        {"name": "in_one_hop", "seeds": ["e02"], "hops": 1, "direction": "in"},
        {"name": "both_two_hops", "seeds": ["e03", "e07"], "hops": 2, "direction": "both",
         "limit_per_seed": 30, "limit": 60},
        {"name": "types", "seeds": ["e05", "e06"], "hops": 2, "direction": "out",
         "edge_types": ["RELATED"]},
        {"name": "chunks_to_entities", "seeds": [100, 101, 102], "seed_label": "chunk", "hops": 2,
         "direction": "out", "edge_types": ["MENTIONS", "RELATED"]},
        {"name": "edge_filter", "seeds": ["e01", "e04", "e08"], "hops": 2, "direction": "both",
         "edge_filter": {"bool": {"must_not": [{"exists": {"field": "valid_to"}}]}}},
        {"name": "vertex_filter", "seeds": ["e05"], "hops": 2, "direction": "both",
         "vertex_filter": {"term": {"field": "kind", "value": "PERSON"}}},
        {"name": "range_filter", "seeds": ["e09", "e10"], "hops": 1, "direction": "both",
         "edge_filter": {"range": {"field": "weight", "gte": 0.3, "lt": 0.9}}},
        {"name": "limit_per_seed", "seeds": ["e05", "e00"], "hops": 2, "direction": "both",
         "limit_per_seed": 4},
        {"name": "limit", "seeds": ["e05", "e00", "e02"], "hops": 1, "direction": "both",
         "limit": 7},
        {"name": "unlabelled_seed_reaches_both_labels", "seeds": ["e00"], "hops": 1,
         "direction": "out"},
        {"name": "missing_and_repeated_seeds", "seeds": ["zz", "e01", "e01", None, 999],
         "hops": 1, "direction": "out"},
    ]
    deletes = {"nodes": [entity(1), chunk(2)], "edges": ["r003", "r010", "r040"]}
    return {"nodes": nodes, "edges": edges, "cases": cases, "deletes": deletes}


def build(fix, deleted):
    g = nx.MultiDiGraph()
    gone_nodes = {tuple(v) for v in fix["deletes"]["nodes"]} if deleted else set()
    gone_edges = set(fix["deletes"]["edges"]) if deleted else set()
    for v in fix["nodes"]:
        ident = (v["label"], v["key"])
        if ident not in gone_nodes:
            g.add_node(ident, **v["props"])
    for e in fix["edges"]:
        src, dst = tuple(e["src"]), tuple(e["dst"])
        if e["key"] in gone_edges or src in gone_nodes or dst in gone_nodes:
            continue
        g.add_edge(src, dst, key=e["key"], type=e["type"], props=e["props"])
    return g


def steps(g, v, direction):
    """(other end, edge key, edge type, edge props) of every edge walked from `v`."""
    out = []
    if direction in ("out", "both"):
        for _, w, k, d in g.out_edges(v, keys=True, data=True):
            out.append((w, k, d["type"], d["props"]))
    if direction in ("in", "both"):
        for u, _, k, d in g.in_edges(v, keys=True, data=True):
            out.append((u, k, d["type"], d["props"]))
    return out


def expand(g, case):
    label = case.get("seed_label")
    hops = case.get("hops", 1)
    direction = case.get("direction", "out")
    types = set(case.get("edge_types", []))
    ef, vf = case.get("edge_filter"), case.get("vertex_filter")
    per_seed_limit = case.get("limit_per_seed", 50)
    limit = case.get("limit", 100)
    edge_ok = lambda t, p: (not types or t in types) and (ef is None or matches(ef, p))
    vertex_ok = lambda v: vf is None or matches(vf, g.nodes[v])
    found = {v[1] for v in g.nodes if label is None or v[0] == label}
    skipped = [i for i, s in enumerate(case["seeds"]) if s is not None and s not in found]
    answer, done = [], []
    for index, seed in enumerate(case["seeds"]):
        if seed is None or seed in done or seed not in found:
            continue
        done.append(seed)
        starts = [v for v in g.nodes if v[1] == seed and (label is None or v[0] == label)]
        best = {}

        def offer(v, hop, via, edge, ty):
            if v[1] == seed and (label is None or v[0] == label):
                return
            cand = (hop, canon(via), canon(edge))
            kept = best.get(v)
            if kept is None or cand < kept[0]:
                best[v] = (cand, {"seed": index, "label": v[0], "key": v[1], "hop": hop,
                                   "via": via, "edge_type": ty, "edge": edge})

        firsts = []
        for s in starts:
            for w, k, t, p in steps(g, s, direction):
                if edge_ok(t, p) and vertex_ok(w):
                    offer(w, 1, seed, k, t)
                    firsts.append(w)
        if hops == 2:
            hop1 = {v for v, (c, _) in best.items() if c[0] == 1}
            for m in sorted(set(firsts), key=lambda v: (canon(v[1]), v[0])):
                if m not in hop1 and not any(m == v for v in hop1):
                    continue
                for w, k, t, p in steps(g, m, direction):
                    if edge_ok(t, p) and vertex_ok(w):
                        offer(w, 2, m[1], k, t)
        rows = sorted((r for _, r in best.values()),
                      key=lambda r: (r["hop"], canon(r["key"]), r["label"]))[:per_seed_limit]
        answer.extend(rows)
    order = lambda r: (r["hop"], canon(r["key"]), r["label"], r["seed"])
    answer = sorted(sorted(answer, key=order)[:limit],
                    key=lambda r: (r["seed"], r["hop"], canon(r["key"]), r["label"]))
    return {"reached": answer, "skipped": skipped}


def main():
    fix = fixture()
    expected = {}
    for phase, deleted in (("before", False), ("after", True)):
        g = build(fix, deleted)
        expected[phase] = {case["name"]: expand(g, case) for case in fix["cases"]}
    (HERE / "expand_fixture.json").write_text(json.dumps(fix, indent=1, sort_keys=True) + "\n")
    (HERE / "expand_expected.json").write_text(json.dumps(expected, indent=1, sort_keys=True) + "\n")
    print(f"wrote {len(fix['cases'])} cases, {len(fix['nodes'])} nodes, {len(fix['edges'])} edges",
          file=sys.stderr)


if __name__ == "__main__":
    main()
