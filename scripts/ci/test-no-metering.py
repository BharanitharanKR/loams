#!/usr/bin/env python3
"""Exercise the public/private boundary against real tracked Git fixtures."""
from pathlib import Path
import subprocess
import tempfile
import unittest


GUARD = Path(__file__).with_name("no-metering.sh")
MARKERS = ("loams." + "meter", "meter." + "sock", "Host" + "Report",
           "x-loams-" + "usage", "loams_" + "meter_")


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        scratch = Path.home() / ".cache" / "loams-agents"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch, prefix="boundary-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)

    def put(self, path, content, tracked=True):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content if isinstance(content, bytes) else content.encode())
        if tracked:
            subprocess.run(["git", "-C", str(self.root), "add", "--", path], check=True)

    def check(self, expected):
        result = subprocess.run(["bash", str(GUARD)], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def test_guard_rejects_meter_names(self):
        for marker in MARKERS:
            with self.subTest(marker=marker):
                self.put("crates/fixture file.rs", marker)
                result = self.check(1)
                self.assertIn("crates/fixture file.rs", result.stderr)

    def test_guard_allows_allowlisted_docs(self):
        paths = ("docs/design/13-decision-log.md", "docs/design/_pending/41-log.md",
                 "docs/design/27-usage-hooks.md", "docs/design/41-control.md",
                 "docs/open-core.md", "docs/plans/2026-10-01-rn1-runner.md",
                 "docs/plans/2026-10-02-mt4-control.md", "CHANGELOG.md")
        for path in paths:
            self.put(path, "\n".join(MARKERS))
        self.check(0)
        self.put("docs/design/41-control.md.rs", MARKERS[0])
        self.check(1)

    def test_guard_does_not_allow_nested_code_under_a_doc_pattern(self):
        self.put("docs/design/41-control/hidden.md", MARKERS[0])
        self.check(1)

    def test_guard_checks_dangling_symlink_targets(self):
        path = self.root / "fixture-link"
        path.symlink_to(MARKERS[1])
        subprocess.run(["git", "-C", str(self.root), "add", "fixture-link"], check=True)
        self.check(1)

    def test_guard_rejects_symlinked_cargo_manifest(self):
        # A symlink target can itself be valid TOML. It must never stand in for
        # the manifest Cargo reads from its destination.
        target = "manifest = 'fixture'"
        prefix = "loam-" + "platform"
        self.put(target, f'[dependencies]\n{prefix}-meter = "1"\n')
        (self.root / "Cargo.toml").symlink_to(target)
        subprocess.run(["git", "-C", str(self.root), "add", "Cargo.toml"], check=True)
        self.check(1)

    def test_guard_rejects_platform_dependency(self):
        prefix = "loam-" + "platform"
        examples = (f'[package]\nname = "{prefix}-meter"\n',
                    f'[dependencies]\n{prefix}-billing = "1"\n',
                    f'[dependencies]\npublic_alias = {{ package = "{prefix}-meter", version = "1" }}\n',
                    f'[target.\'cfg(unix)\'.build-dependencies]\nprivate_alias = {{ package = "{prefix}", path = "../private" }}\n')
        for content in examples:
            with self.subTest(content=content):
                self.put("Cargo.toml", content)
                self.check(1)
        self.put("Cargo.toml", '[package]\nname = "loams-fixture"\n')
        self.put("Cargo.lock", f'[[package]]\nname = "{prefix}-meter"\nversion = "1.0.0"\n')
        self.check(1)

    def test_guard_ignores_untracked_files_and_manifest_comments(self):
        self.put("scratch.rs", MARKERS[0], tracked=False)
        self.put("Cargo.toml", '# loam-platform is a private repo\n[package]\nname = "loams-fixture"\n')
        self.check(0)

    def test_guard_checks_binary_content_without_logging_it(self):
        self.put("data.bin", b"\0secret-text:" + MARKERS[0].encode())
        result = self.check(1)
        self.assertNotIn("secret-text", result.stderr)

    def test_no_metering_guard_covers_pg(self):
        # PG2 Task 9 (D548, D550): no field, metric, table or endpoint in
        # pg-control or its chart is named for billing.
        self.put("crates/loams-pg-control/src/fixture.rs",
                 "pub struct Compute {\n    pub billable_seconds: u64,\n}\n")
        result = self.check(1)
        self.assertIn("crates/loams-pg-control/src/fixture.rs:2", result.stderr)
        self.assertIn("billable", result.stderr)

    def test_pg_guard_reads_names_not_prose(self):
        path = "crates/loams-pg-control/src/names.rs"
        self.put(path, "// the plan's usage notes, credit to Neon\n/* price */\n"
                       "fn f() { let url = \"http://h/x\"; let c = '\"'; }\n")
        self.check(0)
        for name in ("plan_id", "PriceTable", "loams_pg_usage_total", "invoice",
                     "meterBytes", "credits", "/v1/usage"):
            with self.subTest(name=name):
                self.put(path, f'fn f() {{ let x = "{name}"; }}\n')
                self.check(1)
        self.put(path, "fn f() { let planned = 1; }\nimpl loams_kv::FaultPlan for X {}\n")
        self.check(0)
        chart = "deploy/helm/loams-postgres/values.yaml"
        self.put(chart, "# usage: see the README\nmeter: true\n")
        self.check(1)
        self.put(chart, "# usage: see the README\nreplicas: 2\n")
        self.check(0)
        # Outside the two paths the names are not this guard's business.
        self.put("crates/other/src/lib.rs", "pub struct Plan { pub billable: bool }\n")
        self.check(0)

    def test_pg_guard_skips_only_listed_foreign_documents(self):
        # PG2 Task 15: the Kubernetes PodMetrics fixture keeps its own field
        # names; any other fixture of pg-control is still read.
        doc = '{"containers": [{"name": "compute", "usage": {"cpu": "1"}}]}\n'
        self.put("crates/loams-pg-control/tests/fixtures/kube/pod_metrics.json", doc)
        self.check(0)
        self.put("crates/loams-pg-control/tests/fixtures/kube/other.json", doc)
        self.check(1)

    def test_guard_fails_closed_on_invalid_manifest(self):
        self.put("Cargo.toml", "[package\n")
        self.check(1)


if __name__ == "__main__":
    unittest.main()
