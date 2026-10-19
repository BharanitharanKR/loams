"""Tests of the GraphRAG harness (GR1 Task 23).

The offline test drives the harness with scripted modes and a scripted clock. The live
test, which needs `LOAMS_URL` (a running Loams with the `graph` feature), runs it over the
GraphRAG example against the server.

    python3 -m unittest discover -s bench/graph/graphrag/tests -v
"""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import harness as h  # noqa: E402


class FakeClock:
    """Each call advances by the next scripted step (seconds)."""

    def __init__(self) -> None:
        self.now = 0.0
        self.pending = 0.0

    def __call__(self) -> float:
        self.now += self.pending
        self.pending = 0.0
        return self.now


class Offline(unittest.TestCase):
    def test_harness_reports_recall_and_latency(self) -> None:
        questions = [h.Question("a", "qa", ("x", "y")), h.Question("b", "qb", ("z",))]
        clock = FakeClock()
        calls: list[str] = []

        def slow_then_fast(question: str) -> list[str]:
            # 10 ms the first time each question is asked, then 2 ms.
            clock.pending = 0.010 if calls.count(question) == 0 else 0.002
            calls.append(question)
            return {"qa": ["x", "w", "y"], "qb": ["w"]}[question]

        def failing(question: str) -> list[str]:
            if question == "qb":
                raise RuntimeError("boom")
            clock.pending = 0.001
            return ["x"]

        report = h.measure({"good": slow_then_fast, "bad": failing}, questions, k=2, repeats=3, clock=clock)
        self.assertEqual(report["format"], h.FORMAT)
        self.assertEqual((report["k"], report["repeats"], report["questions"]), (2, 3, 2))

        good = report["modes"]["good"]
        # qa: x in the top 2, y at rank 3 -> 0.5; qb: z missing -> 0.
        self.assertEqual(good["per_question"], {"a": 0.5, "b": 0.0})
        self.assertEqual(good["recall_at_k"], 0.25)
        self.assertEqual(good["latency_ms"]["cold"], {"count": 2, "p50": 10.0, "p95": 10.0, "max": 10.0})
        self.assertEqual(good["latency_ms"]["hot"], {"count": 4, "p50": 2.0, "p95": 2.0, "max": 2.0})
        self.assertEqual(good["errors"], [])
        # Every first run comes before any second run.
        self.assertEqual(calls, ["qa", "qb", "qa", "qb", "qa", "qb"])

        bad = report["modes"]["bad"]
        self.assertEqual(bad["per_question"], {"a": 0.5, "b": 0.0})
        self.assertEqual(len(bad["errors"]), 3)
        self.assertEqual(bad["errors"][0], {"question": "b", "run": 0, "error": "RuntimeError: boom"})
        self.assertEqual(bad["latency_ms"]["cold"]["count"], 1)
        self.assertEqual(bad["latency_ms"]["hot"]["count"], 2)

    def test_percentile_is_nearest_rank(self) -> None:
        values = [float(v) for v in range(1, 101)]
        self.assertEqual(h.percentile(values, 50), 50.0)
        self.assertEqual(h.percentile(values, 95), 95.0)
        self.assertEqual(h.percentile([3.0], 95), 3.0)
        self.assertIsNone(h.percentile([], 50))

    def test_recall_at_k(self) -> None:
        self.assertEqual(h.recall_at_k(["a", "b", "c"], ["c", "a"], 2), 0.5)
        self.assertEqual(h.recall_at_k(["a"], [], 1), 1.0)
        self.assertEqual(h.recall_at_k(["a", "a"], ["a", "a"], 1), 1.0)

    def test_example_questions_load(self) -> None:
        questions = h.Question.load(h.EXAMPLE / "questions.json")
        self.assertGreaterEqual(len(questions), 6)
        self.assertTrue(all(q.gold for q in questions))


@unittest.skipUnless(os.environ.get("LOAMS_URL"), "needs a running Loams with the graph feature (LOAMS_URL)")
class Live(unittest.TestCase):
    def test_harness_reports_recall_and_latency_against_loams(self) -> None:
        namespace = f"graphrag_bench_{uuid.uuid4().hex[:12]}"
        modes = h.loams_modes(os.environ["LOAMS_URL"], namespace, index=True)
        questions = h.Question.load(h.EXAMPLE / "questions.json")
        report = h.measure(modes, questions, k=10, repeats=2)
        graph, naive = report["modes"]["graph"], report["modes"]["naive"]
        self.assertEqual(graph["errors"], [])
        self.assertEqual(naive["errors"], [])
        self.assertEqual(graph["recall_at_k"], 1.0, graph["per_question"])
        self.assertGreaterEqual(graph["recall_at_k"], naive["recall_at_k"])
        for mode in (graph, naive):
            self.assertEqual(mode["latency_ms"]["cold"]["count"], len(questions))
            self.assertEqual(mode["latency_ms"]["hot"]["count"], len(questions))
            self.assertGreater(mode["latency_ms"]["cold"]["p50"], 0.0)


if __name__ == "__main__":
    unittest.main()
