#!/usr/bin/env python3
"""The reference for Loams Graph's `pagerank`, `wcc` and `leiden` (GR1 Task 22; design §07 §5.3,
§48 §26.3; rulings R22.0-R22.6).

Builds a seeded 1,000-vertex fixture graph (`algos_fixture.json`) and computes what each case must
answer (`algos_expected.json`): PageRank and weakly connected components with networkx 3.4.2,
hierarchical Leiden with `graspologic-native` 1.3.1 (the PyPI wheel of the very
`network_partitions` source Loams pins), and the modularity of each Leiden partition with
networkx. The Rust tests (`crates/loams-graph/tests/retrieval.rs`) load both files; CI runs only
them, so Python is needed only to regenerate:

    uv venv /tmp/algos && VIRTUAL_ENV=/tmp/algos uv pip install networkx==3.4.2 graspologic-native==1.3.1
    /tmp/algos/bin/python conformance/graph/reference/algos.py

The semantics, as the rulings state them:

* the graph is a directed multigraph; every edge counts once (parallel edges count each, a
  self-loop is an out-edge of its vertex); `edge_types` keeps the edges of those types;
* `pagerank`: networkx's power iteration (uniform start, uniform teleport, dangling mass spread
  uniformly), unweighted, on the subgraph induced by the vertices carrying `label` when given;
  it stops when the L1 change is below N x tolerance, or after `max_iterations` (no error);
* `wcc`: components over both directions; numbered 0, 1, ... by their smallest member in the
  vertex order (the canonical key bytes, then the element id);
* `leiden`: the undirected projection, one edge per vertex pair with the sum of the weights of
  every edge between them in either direction (weight 1 when `weight` is not asked for or the
  edge has none), fed in vertex-ordinal order (ordinal = rank by element id; edges by (low, high)
  ordinal); graspologic's defaults (randomness 0.001, one iteration, modularity) and a seeded
  `SmallRng`; vertices with no edge kept are not in the answer.
"""

import json
import random
import sys
from pathlib import Path

import graspologic_native as gn
import networkx as nx

HERE = Path(__file__).resolve().parent

VERTICES = 1000
COMMUNITIES = 20
SEED = 22


def canon(key):
    """`PrimaryKey::canonical`: 0x01 + u64 big-endian, or 0x03 + UTF-8."""
    if isinstance(key, int):
        return b"\x01" + key.to_bytes(8, "big")
    return b"\x03" + key.encode()


def fixture():
    rng = random.Random(SEED)
    nodes = []
    for i in range(VERTICES):
        element = 1 + i * 3  # element ids are sparse: ordinals are ranks, not ids
        label = "entity" if i % 5 else "chunk"
        # Mixed keys: integers and strings, so the canonical order is not the id order.
        key = i * 7 % 1013 if i % 2 else f"v{(i * 13) % 997:04d}"
        if i % 97 == 0:
            key = None  # keyless: computed, never answered
        nodes.append({"id": element, "labels": [label], "key": key})
    edges = []
    next_edge = 10_000
    members = [list(range(c, VERTICES - 30, COMMUNITIES)) for c in range(COMMUNITIES)]
    def add(src, dst, ty, weight):
        nonlocal next_edge
        edges.append(
            {
                "id": next_edge,
                "ty": ty,
                "src": nodes[src]["id"],
                "dst": nodes[dst]["id"],
                "weight": weight,
            }
        )
        next_edge += 7
    for group in members:
        for a in group:
            for b in group:
                if a < b and rng.random() < 0.18:
                    s, d = (a, b) if rng.random() < 0.5 else (b, a)
                    add(s, d, "RELATED" if rng.random() < 0.8 else "MENTIONS", rng.choice([None, 1.0, 2.0, 0.5]))
    for _ in range(300):
        a, b = rng.randrange(VERTICES - 30), rng.randrange(VERTICES - 30)
        add(a, b, "MENTIONS", rng.choice([None, 1.0, 3.0]))
    # Parallel edges and self-loops.
    for _ in range(40):
        e = rng.choice(edges)
        src = next(i for i, n in enumerate(nodes) if n["id"] == e["src"])
        dst = next(i for i, n in enumerate(nodes) if n["id"] == e["dst"])
        add(src, dst, e["ty"], 1.5)
    for i in range(0, VERTICES - 30, 111):
        add(i, i, "RELATED", None)
    # A separate small component and isolated vertices at the end (VERTICES-30 .. VERTICES).
    tail = list(range(VERTICES - 30, VERTICES - 20))
    for a, b in zip(tail, tail[1:]):
        add(a, b, "RELATED", None)
    # A component joined only by MENTIONS edges, so the type filter splits it.
    add(VERTICES - 20, VERTICES - 19, "MENTIONS", None)
    add(VERTICES - 19, VERTICES - 18, "RELATED", None)
    return {"nodes": nodes, "edges": edges}


def order(nodes):
    """The canonical vertex order the components are numbered in: keyed vertices by canonical
    key bytes (then element id), then keyless ones by element id."""
    return sorted(
        nodes,
        key=lambda n: (0, canon(n["key"]), n["id"]) if n["key"] is not None else (1, b"", n["id"]),
    )


def multigraph(fx, types=None, label=None):
    g = nx.MultiDiGraph()
    keep = {n["id"] for n in fx["nodes"] if label is None or label in n["labels"]}
    for n in fx["nodes"]:
        if n["id"] in keep:
            g.add_node(n["id"])
    for e in fx["edges"]:
        if types is not None and e["ty"] not in types:
            continue
        if e["src"] in keep and e["dst"] in keep:
            g.add_edge(e["src"], e["dst"])
    return g


def pagerank(fx, label, types, damping, max_iterations, tolerance):
    g = multigraph(fx, types, label)
    try:
        ranks = nx.pagerank(g, alpha=damping, max_iter=max_iterations, tol=tolerance, weight=None)
    except nx.PowerIterationFailedConvergence:
        return None
    return {str(k): v for k, v in ranks.items()}


def wcc(fx, types):
    g = multigraph(fx, types)
    component = {}
    for members in nx.weakly_connected_components(g):
        for m in members:
            component[m] = frozenset(members)
    numbered = {}
    out = {}
    for n in order(fx["nodes"]):
        c = component[n["id"]]
        if c not in numbered:
            numbered[c] = len(numbered)
        out[str(n["id"])] = numbered[c]
    return out


def undirected(fx, types, weighted):
    ordinal = {n["id"]: i for i, n in enumerate(sorted(fx["nodes"], key=lambda n: n["id"]))}
    pairs = {}
    for e in fx["edges"]:
        if types is not None and e["ty"] not in types:
            continue
        a, b = sorted((ordinal[e["src"]], ordinal[e["dst"]]))
        w = (e["weight"] if e["weight"] is not None else 1.0) if weighted else 1.0
        pairs[(a, b)] = pairs.get((a, b), 0.0) + w
    return ordinal, sorted(pairs.items())


def leiden(fx, types, weighted, resolution, max_cluster_size, seed):
    ordinal, pairs = undirected(fx, types, weighted)
    edges = [(str(a), str(b), w) for (a, b), w in pairs]
    clusters = gn.hierarchical_leiden(
        edges,
        resolution=resolution,
        randomness=0.001,
        iterations=1,
        use_modularity=True,
        max_cluster_size=max_cluster_size,
        seed=seed,
    )
    by_ordinal = {i: nid for nid, i in ordinal.items()}
    rows = sorted(
        (c.level, int(c.node), c.cluster, c.parent_cluster, c.is_final_cluster) for c in clusters
    )
    g = nx.Graph()
    for (a, b), w in pairs:
        g.add_edge(a, b, weight=w)
    def partition(select):
        groups = {}
        for level, node, cluster, _parent, final in rows:
            if select(level, final):
                groups.setdefault(cluster, set()).add(node)
        return list(groups.values())
    level0 = partition(lambda level, _final: level == 0)
    final = partition(lambda _level, final: final)
    return {
        # [level, vertex (element id), community, parent community or null, is final]
        "rows": [
            [level, by_ordinal[node], cluster, parent, final_]
            for level, node, cluster, parent, final_ in rows
        ],
        "modularity_level0": nx.community.modularity(g, level0, weight="weight", resolution=resolution),
        "modularity_final": nx.community.modularity(g, final, weight="weight", resolution=resolution),
        "communities_level0": len(level0),
        "communities_final": len(final),
    }


def main():
    fx = fixture()
    cases = {
        "pagerank": [
            {"name": "all", "label": None, "edge_types": None, "damping": 0.85, "max_iterations": 200, "tolerance": 1e-12},
            {"name": "related", "label": None, "edge_types": ["RELATED"], "damping": 0.85, "max_iterations": 200, "tolerance": 1e-12},
            {"name": "entity_label", "label": "entity", "edge_types": None, "damping": 0.9, "max_iterations": 200, "tolerance": 1e-12},
            {"name": "section_07_defaults", "label": None, "edge_types": None, "damping": 0.85, "max_iterations": 20, "tolerance": 1e-6},
        ],
        "wcc": [
            {"name": "all", "edge_types": None},
            {"name": "related", "edge_types": ["RELATED"]},
        ],
        "leiden": [
            {"name": "unweighted", "edge_types": None, "weighted": False, "resolution": 1.0, "max_cluster_size": 1000, "seed": 42},
            {"name": "weighted_capped", "edge_types": None, "weighted": True, "resolution": 1.0, "max_cluster_size": 10, "seed": 42},
            {"name": "related_seed_7", "edge_types": ["RELATED"], "weighted": True, "resolution": 0.8, "max_cluster_size": 30, "seed": 7},
        ],
    }
    expected = {"pagerank": [], "wcc": [], "leiden": []}
    for case in cases["pagerank"]:
        ranks = pagerank(fx, case["label"], case["edge_types"], case["damping"], case["max_iterations"], case["tolerance"])
        expected["pagerank"].append({**case, "ranks": ranks})
    for case in cases["wcc"]:
        expected["wcc"].append({**case, "components": wcc(fx, case["edge_types"])})
    for case in cases["leiden"]:
        expected["leiden"].append(
            {**case, **leiden(fx, case["edge_types"], case["weighted"], case["resolution"], case["max_cluster_size"], case["seed"])}
        )
    (HERE / "algos_fixture.json").write_text(json.dumps(fx, separators=(",", ":")) + "\n")
    (HERE / "algos_expected.json").write_text(json.dumps(expected, separators=(",", ":"), sort_keys=True) + "\n")
    for case in expected["leiden"]:
        print(
            case["name"],
            "level0",
            case["communities_level0"],
            round(case["modularity_level0"], 6),
            "final",
            case["communities_final"],
            round(case["modularity_final"], 6),
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
