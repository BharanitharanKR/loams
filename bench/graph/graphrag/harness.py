#!/usr/bin/env python3
"""The GraphRAG retrieval harness (GR1 Task 23; used by Task 35's benchmark, design §07 §9).

It runs a set of questions through one or more retrieval *modes* and reports, per mode:

- **recall@k**: the share of each question's gold entities among the first `k` entities
  the mode retrieved, averaged over the questions;
- **latency** in milliseconds, p50, p95 and max, split into *cold* (each question's first
  run) and *hot* (its later runs, `repeats - 1` of them);
- **errors**: runs that raised, which count as zero recall and are left out of latency.

A mode is any callable `question -> ranked entity keys`, so the harness knows nothing of
Loams; `loams_modes` builds the two this repository measures against a running server:

- `graph`: the GraphRAG example's one Search request (vector seeds, then `expand`);
- `naive`: the naive reference pipeline (vector search over chunks, no graph), whose
  entities are those its chunks mention.

"Cold" here is a question the server has not answered yet, not a cold process or empty
caches: Task 35 owns cache control, its datasets and its gates. Only the standard
library is used.

    python3 bench/graph/graphrag/harness.py --url http://127.0.0.1:8080 --index \\
        --repeats 5 --out bench/results/graph/graphrag.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

REPO = Path(__file__).resolve().parents[3]
EXAMPLE = REPO / "examples" / "graphrag"

FORMAT = "loams.bench.graphrag/1"
# The chunks the naive reference retrieves per question.
NAIVE_CHUNKS = 3

Mode = Callable[[str], Sequence[str]]


@dataclass(frozen=True)
class Question:
    id: str
    question: str
    gold: tuple[str, ...]

    @staticmethod
    def load(path: Path) -> list["Question"]:
        """`[{"id", "question", "gold": [entity keys]}, ...]`; other fields are ignored."""
        return [Question(q["id"], q["question"], tuple(q["gold"])) for q in json.loads(path.read_text(encoding="utf-8"))]


def recall_at_k(retrieved: Sequence[str], gold: Sequence[str], k: int) -> float:
    """The share of `gold` among the first `k` of `retrieved` (1.0 for empty gold)."""
    if not gold:
        return 1.0
    top = set(retrieved[:k])
    return sum(1 for g in set(gold) if g in top) / len(set(gold))


def percentile(values: Sequence[float], p: float) -> float | None:
    """The nearest-rank percentile (`p` in 0..100) of `values`; None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def latency_summary(samples_ms: Sequence[float]) -> dict:
    def r(v: float | None) -> float | None:
        return None if v is None else round(v, 3)

    return {
        "count": len(samples_ms),
        "p50": r(percentile(samples_ms, 50)),
        "p95": r(percentile(samples_ms, 95)),
        "max": r(max(samples_ms) if samples_ms else None),
    }


def measure(
    modes: Mapping[str, Mode],
    questions: Sequence[Question],
    k: int = 10,
    repeats: int = 3,
    clock: Callable[[], float] = time.perf_counter,
) -> dict:
    """Runs every question `repeats` times through every mode (all of a mode's first runs
    before any second run, so a first run is never warmed by its own repeat) and reports
    recall and latency. Recall is taken from each question's first successful run."""
    if repeats < 1 or k < 1:
        raise ValueError("repeats and k must be at least 1")
    report: dict = {"format": FORMAT, "k": k, "repeats": repeats, "questions": len(questions), "modes": {}}
    for name, mode in modes.items():
        cold: list[float] = []
        hot: list[float] = []
        recall: dict[str, float] = {}
        errors: list[dict] = []
        for run in range(repeats):
            for q in questions:
                start = clock()
                try:
                    retrieved = list(mode(q.question))
                except Exception as err:  # noqa: BLE001 - every failure is reported, not raised
                    errors.append({"question": q.id, "run": run, "error": f"{type(err).__name__}: {err}"})
                    continue
                elapsed_ms = (clock() - start) * 1000.0
                (cold if run == 0 else hot).append(elapsed_ms)
                recall.setdefault(q.id, recall_at_k(retrieved, q.gold, k))
        per_question = {q.id: round(recall.get(q.id, 0.0), 6) for q in questions}
        report["modes"][name] = {
            "recall_at_k": round(sum(per_question.values()) / len(questions), 6) if questions else None,
            "per_question": per_question,
            "latency_ms": {"cold": latency_summary(cold), "hot": latency_summary(hot)},
            "errors": errors,
        }
    return report


def loams_modes(url: str, namespace: str, index: bool, corpus: Path | None = None) -> dict[str, Mode]:
    """The `graph` and `naive` modes over a running Loams, with the GraphRAG example's
    pipeline; `index` writes the corpus first (into a namespace that must not exist)."""
    sys.path.insert(0, str(EXAMPLE))
    import graphrag  # noqa: PLC0415 - the example lives outside any package

    rag = graphrag.GraphRAG(graphrag.Loams(url), namespace)
    corpus = corpus or graphrag.DEFAULT_CORPUS
    built = rag.index(corpus) if index else graphrag.build_index(graphrag.load_corpus(corpus), rag.extractor)

    def graph(question: str) -> list[str]:
        return rag.retrieve(question).entities

    def naive(question: str) -> list[str]:
        chunks = graphrag.retrieve_naive(rag.loams, namespace, rag.embedder, question, k=NAIVE_CHUNKS)
        return built.chunk_entities(chunks)

    return {"graph": graph, "naive": naive}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GraphRAG retrieval harness: recall@k and latency per mode")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--namespace", default="graphrag")
    parser.add_argument("--index", action="store_true", help="index the corpus first")
    parser.add_argument("--corpus", type=Path, default=EXAMPLE / "corpus")
    parser.add_argument("--questions", type=Path, default=EXAMPLE / "questions.json")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--modes", default="graph,naive", help="comma-separated: graph, naive")
    parser.add_argument("--out", type=Path, help="write the JSON report here (default: stdout)")
    args = parser.parse_args(argv)

    available = loams_modes(args.url, args.namespace, args.index, args.corpus)
    chosen = {m: available[m] for m in args.modes.split(",")}
    report = measure(chosen, Question.load(args.questions), args.k, args.repeats)
    report["dataset"] = {"corpus": str(args.corpus), "questions": str(args.questions)}
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
