# GraphRAG retrieval harness

`harness.py` measures GraphRAG retrieval (design §07 §9; GR1 Task 23, used by Task 35's
benchmark). It runs a question set through one or more retrieval modes and reports, per
mode, **recall@k** against each question's gold entities and **latency** (p50, p95, max)
for the first run of each question (*cold*) and its later runs (*hot*).

Two modes run against a Loams server, both from the
[GraphRAG example](../../../examples/graphrag/):

| Mode | Retrieval |
|---|---|
| `graph` | One `Search` request: vector seeds on `entities`, then `expand` two hops in the linked graph |
| `naive` | The naive reference pipeline: vector search over `chunks`, no graph; its entities are those its top 3 chunks mention |

```sh
cargo run -p loams --features graph -- dev
python3 bench/graph/graphrag/harness.py --index --repeats 5 --out /path/to/report.json
```

The report (`format: loams.bench.graphrag/1`):

```json
{
  "format": "loams.bench.graphrag/1", "k": 10, "repeats": 5, "questions": 6,
  "modes": {
    "graph": {
      "recall_at_k": 1.0,
      "per_question": {"q1": 1.0, "...": 1.0},
      "latency_ms": {"cold": {"count": 6, "p50": 0.0, "p95": 0.0, "max": 0.0},
                     "hot":  {"count": 24, "p50": 0.0, "p95": 0.0, "max": 0.0}},
      "errors": []
    },
    "naive": {"...": "..."}
  },
  "dataset": {"corpus": "...", "questions": "..."}
}
```

A mode is any callable from a question to ranked entity keys, so `measure()` can be driven
by other pipelines and datasets. A question file is
`[{"id": ..., "question": ..., "gold": [entity keys]}]`.

What the harness does not do yet (Task 35): the benchmark dataset and its licence record,
cold-cache control (here *cold* only means a question the server has not answered), S3 GETs
per query, and the nightly job with its gates.

Tests: `python3 -m unittest discover -s bench/graph/graphrag/tests -v`
(`test_harness_reports_recall_and_latency`; with `LOAMS_URL` set, also against a server).
