#!/usr/bin/env python3
"""NF1 Task 16: tests for scripts/nf1/candidate.py (G3's candidate pins).

Each test copies the pin file and its consumers into a scratch tree and
applies a candidate there; the repository is only read.

Run: python3 -I scripts/nf1/tests/test_candidate.py
"""

import contextlib
import importlib.util
import io
import shutil
import tempfile
import tomllib
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
NF1 = HERE.parent
REPO = NF1.parent.parent
spec = importlib.util.spec_from_file_location("candidate", NF1 / "candidate.py")
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)
pins = candidate.pins

COMMIT = "c" * 40
NEW = "sha256:" + "d" * 64


def toml(doc):
    """A pin document in pins.py's own layout."""
    return pins.render_pins(doc)


class Candidate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        files = [pins.PIN_FILE, pins.README, pins.RELEASE_COMPOSE, *pins.COMPOSE, *pins.WORKFLOWS]
        for f in files:
            dst = self.root / f
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(REPO / f, dst)
        shutil.copytree(REPO / pins.AUDIT_DIR, self.root / pins.AUDIT_DIR)
        self.current = tomllib.loads((REPO / pins.PIN_FILE).read_text())

    def tearDown(self):
        self.tmp.cleanup()

    def built(self, keys=("storage", "compute-v16", "compute-v17")):
        """The current pins with `keys` rebuilt from COMMIT."""
        doc = tomllib.loads((REPO / pins.PIN_FILE).read_text())
        doc["fork_release"] = "lp-2026.11.0"
        doc["fork_commit"] = COMMIT
        for k in keys:
            doc["images"][k]["digest"] = NEW
            doc["images"][k]["source"] = f"ostrium-labs/loams-postgres@{COMMIT}"
        return doc

    def apply(self, doc, fork_commit=COMMIT):
        path = Path(self.tmp.name) / "doc.toml"
        path.write_text(toml(doc))
        env = Path(self.tmp.name) / "env"
        with contextlib.redirect_stdout(io.StringIO()):
            majors = candidate.apply(str(self.root), str(path), fork_commit, str(env))
        return majors, env.read_text()

    def test_candidate_rewrites_the_pins_and_the_headers_image(self):
        majors, env = self.apply(self.built())
        self.assertEqual(majors, [16, 17])
        storage = self.current["images"]["storage"]["repository"]
        self.assertEqual(env, f"PG_HEADERS_IMAGE={storage}@{NEW}\n")
        compose = (self.root / pins.COMPOSE[0]).read_text()
        self.assertIn(f"loams-postgres-compute-v17@{NEW}", compose)
        self.assertIn(f"loams-postgres-storage@{NEW}", compose)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(pins.cmd_check(str(self.root)), 0)

    def test_a_partial_release_keeps_todays_other_pins(self):
        self.apply(self.built(keys=("storage",)))

    def test_the_current_pins_are_a_candidate_of_any_commit(self):
        # g3_reports_status: the dry run sends today's document.
        self.apply(tomllib.loads((REPO / pins.PIN_FILE).read_text()))

    def test_an_image_from_another_commit_is_refused(self):
        doc = self.built()
        doc["images"]["compute-v17"]["source"] = "ostrium-labs/loams-postgres@" + "e" * 40
        with self.assertRaisesRegex(candidate.Refused, "compute-v17: neither built from"):
            self.apply(doc)

    def test_another_repository_is_refused(self):
        doc = self.built()
        doc["images"]["compute-v17"]["repository"] = "ghcr.io/example/compute"
        with self.assertRaisesRegex(candidate.Refused, "compute-v17"):
            self.apply(doc)

    def test_a_mirror_is_refused(self):
        doc = self.built()
        doc["images"]["storage"]["repository"] = "ghcr.io/neondatabase/neon"
        doc["images"]["storage"]["source"] = f"mirror:ghcr.io/neondatabase/neon@{NEW}"
        with self.assertRaisesRegex(candidate.Refused, "storage"):
            self.apply(doc)

    def test_the_neon_archive_is_refused(self):
        doc = self.built()
        doc["images"]["storage"]["repository"] = "ghcr.io/ostrium-labs/neon-archive/neon"
        with self.assertRaisesRegex(candidate.Refused, "never deployed"):
            self.apply(doc)

    def test_an_invalid_document_is_refused_and_nothing_is_written(self):
        doc = self.built()
        doc["images"]["storage"]["digest"] = "sha256:short"
        before = (self.root / pins.COMPOSE[0]).read_text()
        with self.assertRaisesRegex(candidate.Refused, "digest"):
            self.apply(doc)
        self.assertEqual((self.root / pins.COMPOSE[0]).read_text(), before)

    def test_a_bad_fork_commit_is_refused(self):
        with self.assertRaisesRegex(candidate.Refused, "40 hex"):
            self.apply(self.built(), fork_commit="main")

    def test_not_toml_is_refused(self):
        path = Path(self.tmp.name) / "doc.toml"
        path.write_text("schema = [")
        with self.assertRaises(candidate.Refused):
            candidate.apply(str(self.root), str(path), COMMIT)


if __name__ == "__main__":
    unittest.main(verbosity=1)
