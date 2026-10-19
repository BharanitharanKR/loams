"""Tests of the GraphRAG example (GR1 Task 23).

The offline tests need nothing. `test_example_runs_end_to_end_in_ci` needs a running Loams
built with the `graph` feature, named by `LOAMS_URL` (for example `http://127.0.0.1:8080`);
without it the test is skipped. CI's Graph job starts `loams dev` and sets it.

    python3 -m unittest discover -s examples/graphrag/tests -v
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import graphrag as g  # noqa: E402


def fresh_namespace(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Offline(unittest.TestCase):
    def test_chunks_are_paragraphs_without_headings(self) -> None:
        chunks = g.chunk_document("doc", "# Title\n\nOne two.\nThree.\n\n\nFour.\n")
        self.assertEqual([(c.id, c.text) for c in chunks], [("doc#0", "One two. Three."), ("doc#1", "Four.")])

    def test_extractor_reads_the_corpus(self) -> None:
        index = g.build_index(g.load_corpus(g.DEFAULT_CORPUS), g.PatternExtractor())
        ids = {r.id for r in index.relations}
        self.assertEqual(len(index.relations), 18)
        self.assertEqual(len(index.entities), 14)
        for expected in ("ada-park--LEADS--kite", "kite--DEPENDS_ON--tern", "chen-wei--MAINTAINS--tern", "wren--USES--heron"):
            self.assertIn(expected, ids)
        # Prose that is not a `<Entity> <phrase> <Entity>` clause yields nothing.
        self.assertFalse(any(r.src in ("The Harbor Collective", "She", "Benchmarks") for r in index.relations))

    def test_embedder_is_fixed(self) -> None:
        embedder = g.HashingEmbedder()
        vector = embedder.embed("Ada Park")
        self.assertEqual(len(vector), 256)
        self.assertAlmostEqual(sum(v * v for v in vector), 1.0, places=5)
        # The vectors are part of the example's contract: a change here changes every
        # answer, so it must be deliberate.
        digest = hashlib.sha256(json.dumps(vector).encode()).hexdigest()[:16]
        self.assertEqual(digest, GOLDEN_ADA_PARK)
        self.assertEqual(embedder.embed("the of and"), [0.0] * 256)

    def test_path_answerer_takes_the_longest_asked_path(self) -> None:
        names = {k: k.title() for k in ("ada", "kite", "rust", "ben", "wren")}
        names["ada"] = "Ada"
        neighbors = [
            {"seed": "ada", "key": "kite", "hop": 1, "via": "ada", "edge_type": "LEADS", "edge": "ada--LEADS--kite", "score": 0.5},
            {"seed": "ada", "key": "ben", "hop": 1, "via": "ada", "edge_type": "WORKS_WITH", "edge": "ada--WORKS_WITH--ben", "score": 0.5},
            {"seed": "ada", "key": "rust", "hop": 2, "via": "kite", "edge_type": "WRITTEN_IN", "edge": "kite--WRITTEN_IN--rust", "score": 0.25},
            {"seed": "ada", "key": "wren", "hop": 2, "via": "ben", "edge_type": "LEADS", "edge": "ben--LEADS--wren", "score": 0.25},
        ]
        relations = {
            "ada--LEADS--kite": {"src": "ada", "dst": "kite", "rel_type": "LEADS", "chunk": "people#1"},
            "kite--WRITTEN_IN--rust": {"src": "kite", "dst": "rust", "rel_type": "WRITTEN_IN", "chunk": "kite#0"},
        }
        retrieval = g.Retrieval(["ada"], neighbors, names)
        answer = g.PathAnswerer().answer("What language is the project Ada leads written in?", retrieval, relations)
        self.assertEqual(answer.text, "Rust")
        self.assertEqual(answer.facts, ["Ada leads Kite", "Kite is written in Rust"])
        self.assertEqual(answer.chunks, ["people#1", "kite#0"])
        # Nothing asked for: no answer.
        none = g.PathAnswerer().answer("Tell me about Ada.", retrieval, relations)
        self.assertEqual(none.text, "")

    def test_search_body_binds_the_question_as_a_vector(self) -> None:
        body = g.search_body([0.0, 1.0])
        self.assertEqual(body["retrievers"][0]["vector"]["query"], [0.0, 1.0])
        self.assertEqual(body["expand"]["graph"], g.GRAPH)
        self.assertEqual(body["expand"]["hops"], 2)
        self.assertEqual(body["expand"]["direction"], "both")


GOLDEN_ADA_PARK = "55ee838979a01378"


@unittest.skipUnless(os.environ.get("LOAMS_URL"), "needs a running Loams with the graph feature (LOAMS_URL)")
class EndToEnd(unittest.TestCase):
    def test_example_runs_end_to_end_in_ci(self) -> None:
        """A small corpus, the fixed hashing embedder and the pattern extractor: every
        question gets its expected answer, facts and sources, and the same again."""
        rag = g.GraphRAG(g.Loams(os.environ["LOAMS_URL"]), fresh_namespace("graphrag_ci"))
        built = rag.index()
        self.assertEqual((len(built.chunks), len(built.entities), len(built.relations)), (15, 14, 18))
        for q in g.load_questions():
            with self.subTest(q["id"]):
                answer = rag.ask(q["question"])
                self.assertEqual(answer.text, q["answer"])
                self.assertEqual(answer.facts, q["facts"])
                self.assertEqual(answer.chunks, q["chunks"])
                retrieval = rag.retrieve(q["question"])
                self.assertTrue(set(q["gold"]) <= set(retrieval.entities), retrieval.entities)
                # Deterministic: the same retrieval and answer on a second run.
                self.assertEqual(rag.retrieve(q["question"]), retrieval)
                self.assertEqual(rag.ask(q["question"]), answer)


if __name__ == "__main__":
    unittest.main()
