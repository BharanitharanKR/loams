#!/usr/bin/env python3
"""NF1 Task 2: tests for scripts/nf1/pins.py and the pin file.

Each test copies the consumers and the audit files into a scratch tree,
renders them from the mirror pin file Task 2 first wrote
(fixtures/pins/pins-mirror-77e22e4b.toml), and runs `pins.py --root <tree>`
there; the repository is only read. `Repository` checks the repository's own
pin file, which names our releases since Task 14. `override_envs_still_work` also runs `docker compose config` when a
compose CLI is installed (no daemon, no image pulls).

Run: python3 -I scripts/nf1/tests/test_pins.py
"""

import difflib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
NF1 = HERE.parent
REPO = NF1.parent.parent
PINS = NF1 / "pins.py"
FIXTURES = HERE / "fixtures" / "pins"

PIN_FILE = "deploy/pins/loams-postgres-images.toml"
README = "deploy/loams-postgres-dev/README.md"
COMPOSE = ("deploy/loams-postgres-dev/compose.yaml", "deploy/loams-pg-bench/compose.yaml")
RELEASE_COMPOSE = "deploy/release/compose.yaml"
WORKFLOWS = (".github/workflows/pg2.yml", ".github/workflows/pg2-e2e.yml", ".github/workflows/loams-pg-bench.yml")
CONSUMERS = COMPOSE + (RELEASE_COMPOSE,) + WORKFLOWS + (README,)
MAJORS_RS = "crates/loams-postgres/src/majors.rs"

NEON = "ghcr.io/neondatabase/neon@sha256:7a4f124917bb929964b2d696d710f19584f80bb9bd51b2af4a6e2425434c761f"
COMPUTE17 = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3"
OTHER = "sha256:" + "e" * 64
MIRROR_PINS = FIXTURES / "pins-mirror-77e22e4b.toml"


def compose_cli():
    """The compose CLI, as an argv prefix, or None."""
    for argv in (["docker", "compose"], ["docker-compose"]):
        if shutil.which(argv[0]):
            r = subprocess.run(argv + ["version"], capture_output=True, text=True)
            if r.returncode == 0:
                return argv
    return None


class Tree(unittest.TestCase):
    """A scratch copy of the pin file, its consumers and the audit files."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="nf1-pins-")
        self.root = Path(self._tmp.name)
        for rel in (PIN_FILE,) + CONSUMERS:
            dst = self.root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO / rel, dst)
        shutil.copytree(REPO / "deploy/pins/audit", self.root / "deploy/pins/audit")
        # Start every test from the mirror pins, whatever the repository pins now.
        shutil.copyfile(MIRROR_PINS, self.root / PIN_FILE)
        r = self.pins("set")
        assert r.returncode == 0, r.stdout + r.stderr

    def tearDown(self):
        self._tmp.cleanup()

    def pins(self, *args):
        return subprocess.run([sys.executable, "-I", str(PINS), "--root", str(self.root), *args],
                              capture_output=True, text=True)

    def check(self):
        return self.pins("check")

    def read(self, rel):
        return (self.root / rel).read_text()

    def write(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def edit(self, rel, old, new, count=1):
        text = self.read(rel)
        self.assertIn(old, text, f"{rel} has no {old!r}")
        self.write(rel, text.replace(old, new, count))

    def assertFails(self, needle):
        r = self.check()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn(needle, r.stderr)
        return r

    def assertPasses(self):
        r = self.check()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


class Repository(unittest.TestCase):
    def test_check_passes_on_the_repository(self):
        r = subprocess.run([sys.executable, "-I", str(PINS), "check"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_first_content_is_neons_reference_for_private_mirrors(self):
        # Task 2: every mirrored package stayed private (Task 1 ruling 6), so the
        # first pin file kept Neon's repositories with `mirror:` sources.
        text = MIRROR_PINS.read_text()
        self.assertIn('fork_release = "mirror-77e22e4b"', text)
        self.assertIn('fork_commit = "77e22e4bf09d88b70b4a83a38c2f6de6301816b4"', text)
        for ref in (NEON, COMPUTE17):
            repo, digest = ref.split("@")
            self.assertIn(f'repository = "{repo}"\ndigest = "{digest}"\nsource = "mirror:{ref}"', text)
        self.assertNotIn("neon-archive", text)

    def test_the_repository_pins_our_own_release(self):
        # Task 14: Loams runs our builds. Every pin is a release of the fork,
        # from the release's commit, and no `mirror:` source is left.
        pins = tomllib.loads((REPO / PIN_FILE).read_text())
        self.assertRegex(pins["fork_release"], r"^lp-\d{4}\.\d{2}\.\d+$")
        self.assertRegex(pins["fork_commit"], r"^[0-9a-f]{40}$")
        self.assertIn("storage", pins["images"])
        for key, img in pins["images"].items():
            with self.subTest(image=key):
                self.assertTrue(img["repository"].startswith("ghcr.io/ostrium-labs/loams-postgres-"), img)
                self.assertEqual(img["source"], "ostrium-labs/loams-postgres@" + pins["fork_commit"])
        for rel in CONSUMERS:
            with self.subTest(consumer=rel):
                text = (REPO / rel).read_text()
                self.assertNotIn("ghcr.io/neondatabase/", text if rel != README else
                                 re.sub(r"^## Mirror\n.*?(?=^## |\Z)", "", text, flags=re.M | re.S))


class PinsMatchConsumers(Tree):
    """pins_match_consumers: a consumer edited by hand fails."""

    def test_unchanged_tree_passes(self):
        self.assertPasses()

    def test_each_compose_anchor_edited_by_hand_fails(self):
        for rel in COMPOSE:
            for ref in (NEON, COMPUTE17):
                with self.subTest(rel=rel, ref=ref):
                    saved = self.read(rel)
                    self.edit(rel, ref, ref.split("@")[0] + "@" + OTHER)
                    self.assertFails(rel)
                    self.write(rel, saved)

    def test_each_headers_image_edited_by_hand_fails(self):
        for rel in WORKFLOWS:
            with self.subTest(rel=rel):
                saved = self.read(rel)
                self.edit(rel, f"PG_HEADERS_IMAGE: {NEON}", f"PG_HEADERS_IMAGE: ghcr.io/neondatabase/neon@{OTHER}")
                self.assertFails(rel)
                self.write(rel, saved)

    def test_release_stack_default_edited_by_hand_fails(self):
        self.edit(RELEASE_COMPOSE, "${NEON_IMAGE:-" + NEON + "}", "${NEON_IMAGE:-ghcr.io/neondatabase/neon@" + OTHER + "}")
        self.assertFails(RELEASE_COMPOSE)

    def test_readme_table_edited_by_hand_fails(self):
        self.edit(README, NEON, "ghcr.io/neondatabase/neon@" + OTHER)
        self.assertFails(README)

    def test_missing_consumer_line_fails(self):
        self.edit(COMPOSE[0], "x-neon-image: &neon-image ${NEON_IMAGE:-", "x-neon-image: &neon-image ${OTHER_IMAGE:-")
        self.assertFails(COMPOSE[0])

    def test_pin_file_edited_by_hand_fails(self):
        # Comments or layout outside pins.py's rendering are refused, so the
        # file stays the one `set` writes.
        self.write(PIN_FILE, self.read(PIN_FILE) + "# a note\n")
        self.assertFails(PIN_FILE)

    def test_stale_majors_rs_fails_and_rust_fixes_it(self):
        self.write(MAJORS_RS, "pub const DEFAULT: u32 = 16;\n")
        self.assertFails(MAJORS_RS)
        r = self.pins("rust")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertPasses()


class EveryPinHasDigest(Tree):
    """every_pin_has_digest: a digest is `sha256:` and 64 hex characters."""

    def test_short_digest_fails(self):
        self.edit(PIN_FILE, 'digest = "sha256:7a4f1249', 'digest = "sha256:7a4f', 1)
        self.assertFails("digest")

    def test_missing_digest_fails(self):
        text = self.read(PIN_FILE)
        self.write(PIN_FILE, re.sub(r'^digest = .*\n', "", text, count=1, flags=re.M))
        self.assertFails("digest")

    def test_tag_instead_of_digest_fails(self):
        text = self.read(PIN_FILE)
        self.write(PIN_FILE, re.sub(r'^digest = .*$', 'digest = "latest"', text, count=1, flags=re.M))
        self.assertFails("digest")

    def test_source_neither_fork_nor_mirror_fails(self):
        self.edit(PIN_FILE, f'source = "mirror:{NEON}"', 'source = "neondatabase/neon@main"')
        self.assertFails("source")

    def test_fork_source_needs_a_loams_postgres_repository(self):
        self.edit(PIN_FILE, f'source = "mirror:{NEON}"', 'source = "ostrium-labs/loams-postgres@' + "a" * 40 + '"')
        self.assertFails("ghcr.io/ostrium-labs/loams-postgres-")

    def test_mirror_source_digest_must_be_the_pinned_digest(self):
        self.edit(PIN_FILE, f'source = "mirror:{NEON}"', f'source = "mirror:ghcr.io/neondatabase/neon@{OTHER}"')
        self.assertFails("source")

    def test_majors_must_have_images_and_minors(self):
        self.edit(PIN_FILE, "maintenance = [16]", "maintenance = [15, 16]")
        self.assertFails("15")

    def test_default_must_be_supported(self):
        self.edit(PIN_FILE, "default = 17", "default = 16")
        self.assertFails("default")


class AuditRunsBeforeVisibility(Tree):
    """audit_runs_before_visibility, the pin-file half (Task 1 ruling 8):
    `pins.py check` refuses a `mirror:` source whose audit file is missing."""

    def test_mirror_source_without_audit_file_fails(self):
        (self.root / "deploy/pins/audit/sha256-7a4f124917bb929964b2d696d710f19584f80bb9bd51b2af4a6e2425434c761f.json").unlink()
        self.assertFails("audit")


class NeonArchiveNeverDeployed(Tree):
    """Controller ruling (Task 1): the neon-archive packages stay private and
    are never deployed. No pin's repository, and no consumer, names them."""

    ARCHIVE_NEON = "ghcr.io/ostrium-labs/neon-archive/neon@sha256:7a4f124917bb929964b2d696d710f19584f80bb9bd51b2af4a6e2425434c761f"

    def test_pin_repository_in_neon_archive_fails(self):
        self.edit(PIN_FILE, 'repository = "ghcr.io/neondatabase/neon"', 'repository = "ghcr.io/ostrium-labs/neon-archive/neon"')
        self.assertFails("neon-archive")

    def test_mirror_source_may_name_the_archive(self):
        # Reference use: the source records where the bytes are archived.
        self.edit(PIN_FILE, f'source = "mirror:{NEON}"', f'source = "mirror:{self.ARCHIVE_NEON}"')
        r = self.pins("set")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn(self.ARCHIVE_NEON, self.read(README))
        self.assertPasses()

    def test_override_in_a_workflow_fails(self):
        self.write(".github/workflows/other.yml", f"env:\n  NEON_IMAGE: {self.ARCHIVE_NEON}\n")
        self.assertFails(".github/workflows/other.yml")

    def test_readme_mirror_section_is_reference_use(self):
        text = self.read(README)
        self.assertIn("ghcr.io/ostrium-labs/neon-archive/", text)
        self.assertPasses()
        self.write(README, text + f"\n## Run elsewhere\n\n`NEON_IMAGE={self.ARCHIVE_NEON}`\n")
        self.assertFails(README)

    def test_set_refuses_an_archive_release(self):
        rel = (FIXTURES / "release-lp-2026.10.0.toml").read_text()
        rel = rel.replace("ghcr.io/ostrium-labs/loams-postgres-storage", "ghcr.io/ostrium-labs/neon-archive/neon")
        self.write("release.toml", rel)
        before = {c: self.read(c) for c in (PIN_FILE,) + CONSUMERS}
        r = self.pins("set", "--from-release", str(self.root / "release.toml"))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("neon-archive", r.stderr)
        self.assertEqual(before, {c: self.read(c) for c in before}, "set wrote files for a refused release")


class SetRewritesAllConsumers(Tree):
    """set_rewrites_all_consumers: golden files."""

    def test_set_from_release(self):
        before = {c: self.read(c) for c in CONSUMERS}
        r = self.pins("set", "--from-release", str(FIXTURES / "release-lp-2026.10.0.toml"))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.read(PIN_FILE), (FIXTURES / "pins-lp-2026.10.0.toml").read_text())
        changed = []
        for rel in CONSUMERS:
            for line in difflib.unified_diff(before[rel].splitlines(), self.read(rel).splitlines(), lineterm="", n=0):
                if line.startswith(("---", "+++", "@@")):
                    continue
                changed.append(f"{rel}: {line}")
        self.assertEqual("\n".join(changed) + "\n", (FIXTURES / "consumers-lp-2026.10.0.txt").read_text())
        self.assertPasses()
        # Idempotent: a second run changes nothing.
        after = {c: self.read(c) for c in (PIN_FILE,) + CONSUMERS}
        r = self.pins("set", "--from-release", str(FIXTURES / "release-lp-2026.10.0.toml"))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(after, {c: self.read(c) for c in after})

    def test_set_rewrites_existing_majors_rs(self):
        self.pins("rust")
        r = self.pins("set", "--from-release", str(FIXTURES / "release-lp-2026.10.0.toml"))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("pub const MAINTENANCE: &[u32] = &[];", self.read(MAJORS_RS))
        self.assertPasses()

    def test_invalid_release_writes_nothing(self):
        rel = (FIXTURES / "release-lp-2026.10.0.toml").read_text().replace("sha256:1111", "sha256:11", 1)
        self.write("release.toml", rel)
        before = {c: self.read(c) for c in (PIN_FILE,) + CONSUMERS}
        r = self.pins("set", "--from-release", str(self.root / "release.toml"))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertEqual(before, {c: self.read(c) for c in before})

    def test_plain_http_release_refused(self):
        r = self.pins("set", "--from-release", "http://example.invalid/pins.toml")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)


class Rust(Tree):
    def test_rust_writes_majors(self):
        r = self.pins("rust")
        self.assertEqual(r.returncode, 0, r.stderr)
        text = self.read(MAJORS_RS)
        self.assertIn("pub const SUPPORTED: &[u32] = &[17];", text)
        self.assertIn("pub const BETA: &[u32] = &[];", text)
        self.assertIn("pub const MAINTENANCE: &[u32] = &[16];", text)
        self.assertIn("pub const DEFAULT: u32 = 17;", text)
        self.assertPasses()


class OverrideEnvsStillWork(Tree):
    """override_envs_still_work: NEON_IMAGE and COMPUTE_IMAGE still override
    the compose defaults (PG2 R2.1)."""

    def test_anchors_keep_the_override(self):
        for rel in COMPOSE:
            text = self.read(rel)
            self.assertIn("x-neon-image: &neon-image ${NEON_IMAGE:-" + NEON + "}", text)
            self.assertIn("x-compute-image: &compute-image ${COMPUTE_IMAGE:-" + COMPUTE17 + "}", text)
        self.assertIn("    image: ${NEON_IMAGE:-" + NEON + "}", self.read(RELEASE_COMPOSE))

    def test_compose_config_resolves_overrides(self):
        cli = compose_cli()
        if cli is None:
            self.skipTest("no docker compose CLI")
        for rel in COMPOSE:
            for env, want in (({}, {NEON, COMPUTE17}),
                              ({"NEON_IMAGE": "example.org/n@" + OTHER, "COMPUTE_IMAGE": "example.org/c@" + OTHER},
                               {"example.org/n@" + OTHER, "example.org/c@" + OTHER})):
                with self.subTest(rel=rel, env=env):
                    full = {k: v for k, v in os.environ.items() if k not in ("NEON_IMAGE", "COMPUTE_IMAGE")}
                    full.update(env)
                    r = subprocess.run(cli + ["-f", str(self.root / rel), "--profile", "*", "config", "--images"],
                                       capture_output=True, text=True, env=full)
                    self.assertEqual(r.returncode, 0, r.stderr)
                    images = {i for i in r.stdout.split() if "rustfs" not in i}
                    self.assertEqual(images, want)


if __name__ == "__main__":
    unittest.main(verbosity=2)
