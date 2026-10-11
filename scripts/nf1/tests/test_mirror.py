#!/usr/bin/env python3
"""NF1 Task 1: tests for mirror-images.sh and audit-image.sh.

mirror_digests_match needs `skopeo` and a plain-HTTP registry (registry:3) at
MIRROR_TEST_REGISTRY, e.g. localhost:5000; CI starts one as a service. The
audit tests need `rust-audit-info` and `objcopy`.

Run: python3 -I scripts/nf1/tests/test_mirror.py
"""

import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
NF1 = HERE.parent
REPO = NF1.parent.parent
MIRROR = NF1 / "mirror-images.sh"
AUDIT = NF1 / "audit-image.sh"
AUDIT_PY = NF1 / "audit_image.py"
README = REPO / "deploy" / "loams-postgres-dev" / "README.md"

PINNED = "ghcr.io/neondatabase/neon@sha256:" + "a" * 64
REGISTRY = os.environ.get("MIRROR_TEST_REGISTRY", "")


def run(*args, env=None, **kw):
    full_env = dict(os.environ)
    full_env.update(env or {})
    return subprocess.run(
        [str(a) for a in args], capture_output=True, text=True, env=full_env, **kw
    )


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class OciLayout:
    """A minimal OCI image layout, written by hand (no image tool needed)."""

    def __init__(self, root: Path):
        self.root = root
        (root / "blobs" / "sha256").mkdir(parents=True)
        (root / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')

    def blob(self, data: bytes, media_type: str) -> dict:
        digest = sha256(data)
        (self.root / "blobs" / "sha256" / digest.split(":")[1]).write_bytes(data)
        return {"mediaType": media_type, "digest": digest, "size": len(data)}

    def image(self, arch: str) -> dict:
        layer = io.BytesIO()
        with tarfile.open(fileobj=layer, mode="w") as tar:
            body = f"fixture {arch}\n".encode()
            info = tarfile.TarInfo("etc/fixture")
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        raw = layer.getvalue()
        gz = gzip.compress(raw, mtime=0)
        config = json.dumps(
            {
                "architecture": arch,
                "os": "linux",
                "rootfs": {"type": "layers", "diff_ids": [sha256(raw)]},
                "config": {},
            }
        ).encode()
        manifest = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": self.blob(config, "application/vnd.oci.image.config.v1+json"),
                "layers": [self.blob(gz, "application/vnd.oci.image.layer.v1.tar+gzip")],
            }
        ).encode()
        desc = self.blob(manifest, "application/vnd.oci.image.manifest.v1+json")
        desc["platform"] = {"architecture": arch, "os": "linux"}
        return desc

    def index(self, archs, name: str) -> str:
        index = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [self.image(a) for a in archs],
            }
        ).encode()
        desc = self.blob(index, "application/vnd.oci.image.index.v1+json")
        desc["annotations"] = {"org.opencontainers.image.ref.name": name}
        top = self.root / "index.json"
        existing = json.loads(top.read_text()) if top.exists() else {"schemaVersion": 2, "manifests": []}
        existing["manifests"].append(desc)
        top.write_text(json.dumps(existing))
        return desc["digest"]


class MirrorArgs(unittest.TestCase):
    def test_mirror_refuses_unpinned_source(self):
        for src in ("ghcr.io/neondatabase/neon:latest", "ghcr.io/neondatabase/neon",
                    "ghcr.io/neondatabase/neon@sha256:abc"):
            r = run(MIRROR, src, "ghcr.io/ostrium-labs/neon-archive/neon", "mirror-77e22e4b",
                    env={"SKOPEO": "false"})
            self.assertEqual(r.returncode, 2, f"{src}: {r.stderr}")
            self.assertIn("digest", r.stderr)

    def test_mirror_refuses_tagged_destination(self):
        r = run(MIRROR, PINNED, "ghcr.io/ostrium-labs/neon-archive/neon:x", "mirror-77e22e4b",
                env={"SKOPEO": "false"})
        self.assertEqual(r.returncode, 2, r.stderr)

    def test_dry_run_prints_the_copy_and_runs_nothing(self):
        r = run(MIRROR, "--dry-run", PINNED, "ghcr.io/ostrium-labs/neon-archive/neon",
                "mirror-77e22e4b", env={"SKOPEO": "false"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("copy --all --preserve-digests --retry-times 3", r.stdout)
        self.assertIn(f"docker://{PINNED}", r.stdout)
        self.assertIn("docker://ghcr.io/ostrium-labs/neon-archive/neon:mirror-77e22e4b", r.stdout)

    def test_default_runs_skopeo_pinned_through_podman(self):
        r = run(MIRROR, "--dry-run", PINNED, "ghcr.io/ostrium-labs/x", "t",
                env={"SKOPEO": ""})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertRegex(r.stdout, r"podman run --rm .*quay\.io/skopeo/stable@sha256:[0-9a-f]{64}")

    def test_audit_refuses_unpinned_source(self):
        r = run(AUDIT, "ghcr.io/neondatabase/neon:latest", env={"SKOPEO": "false"})
        self.assertEqual(r.returncode, 2, r.stderr)


@unittest.skipUnless(REGISTRY and shutil.which("skopeo"),
                     "needs MIRROR_TEST_REGISTRY and skopeo")
class MirrorRegistry(unittest.TestCase):
    """mirror_digests_match: a two-platform index copied byte for byte."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="nf1-mirror-"))
        self.layout = OciLayout(self.tmp / "oci")
        self.env = {"SKOPEO": "skopeo", "MIRROR_TLS_VERIFY": "false"}

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def push(self, name: str, archs, repo: str) -> str:
        digest = self.layout.index(archs, name)
        r = run("skopeo", "copy", "--all", "--preserve-digests", "--dest-tls-verify=false",
                f"oci:{self.layout.root}:{name}", f"docker://{REGISTRY}/{repo}")
        self.assertEqual(r.returncode, 0, r.stderr)
        return digest

    def test_mirror_digests_match(self):
        digest = self.push("two", ["amd64", "arm64"], "nf1-test/src:two")
        src = f"{REGISTRY}/nf1-test/src@{digest}"
        dst = f"{REGISTRY}/nf1-test/archive/dst"
        r = run(MIRROR, src, dst, "mirror-test", env=self.env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"index {digest} identical", r.stdout)
        self.assertEqual(len(re.findall(r"^child sha256:[0-9a-f]{64} \S+ identical$", r.stdout, re.M)), 2,
                         r.stdout)

        # The tag now points at another index: verification must fail.
        self.push("one", ["amd64"], "nf1-test/archive/dst:mirror-test")
        r = run(MIRROR, "--verify-only", src, dst, "mirror-test", env=self.env)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("differs", r.stderr)

    def test_audit_then_mirror_from_the_staged_layout(self):
        # audit-image.sh stages the index once; mirror-images.sh pushes those bytes
        # and still verifies against the source registry.
        digest = self.push("staged", ["amd64", "arm64"], "nf1-test/src:staged")
        src = f"{REGISTRY}/nf1-test/src@{digest}"
        layout = self.tmp / "stage"
        r = run(AUDIT, src, env={**self.env, "AUDIT_KEEP_LAYOUT": str(layout),
                                 "AUDIT_WORKDIR": str(self.tmp), "AUDIT_OFFLINE": "1"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout), {})
        self.assertTrue((layout / "blobs" / "sha256" / digest.split(":")[1]).is_file())

        dst = f"{REGISTRY}/nf1-test/archive/staged"
        from_ = f"oci:{layout}:{digest.split(':')[1]}"
        r = run(MIRROR, src, dst, "mirror-test", env={**self.env, "MIRROR_FROM": from_})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"index {digest} identical", r.stdout)
        self.assertEqual(r.stdout.count(" identical\n"), 3, r.stdout)


def dep_v0(packages) -> bytes:
    return zlib.compress(json.dumps({"packages": packages}).encode())


def pkg(name, version, source="crates.io", root=False):
    p = {"name": name, "version": version, "source": source, "kind": "runtime"}
    if root:
        p["root"] = True
    return p


@unittest.skipUnless(shutil.which("rust-audit-info") and shutil.which("objcopy"),
                     "needs rust-audit-info and objcopy")
class Audit(unittest.TestCase):
    """audit_flags_cddl, audit_flags_subzero: fixture `.dep-v0` payloads in a
    skopeo `dir:` image, scanned by audit_image.py."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="nf1-audit-"))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def elf(self, name, packages) -> bytes:
        payload = self.tmp / f"{name}.dep-v0"
        payload.write_bytes(dep_v0(packages))
        out = self.tmp / name
        shutil.copy(shutil.which("true"), out)
        r = run("objcopy", "--add-section", f".dep-v0={payload}", out)
        self.assertEqual(r.returncode, 0, r.stderr)
        return out.read_bytes()

    def image_dir(self, files: dict, arch="amd64") -> Path:
        d = self.tmp / f"img-{arch}"
        d.mkdir()
        layers = []
        for path, data in files.items():
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                info = tarfile.TarInfo(path)
                info.size = len(data)
                info.mode = 0o755
                tar.addfile(info, io.BytesIO(data))
            gz = gzip.compress(buf.getvalue(), mtime=0)
            digest = sha256(gz)
            (d / digest.split(":")[1]).write_bytes(gz)
            layers.append({"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                           "digest": digest, "size": len(gz)})
        (d / "manifest.json").write_text(json.dumps({"schemaVersion": 2, "layers": layers}))
        return d

    def audit(self, d: Path, licences=None, arch="amd64"):
        lic = self.tmp / "licences.json"
        lic.write_text(json.dumps(licences or {}))
        r = run(sys.executable, "-I", AUDIT_PY, "--offline", "--licences", lic,
                f"linux/{arch}={d}")
        out = json.loads(r.stdout) if r.stdout.strip() else None
        return r, out

    def test_clean_image_passes(self):
        d = self.image_dir({
            "usr/local/bin/pageserver": self.elf("pageserver", [pkg("pageserver", "0.1.0", "local", True),
                                                                pkg("anyhow", "1.0.98")]),
            "etc/motd": b"not an elf\n",
        })
        r, out = self.audit(d, {"anyhow@1.0.98": "MIT OR Apache-2.0"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(list(out), ["linux/amd64:/usr/local/bin/pageserver"])
        self.assertIn({"name": "anyhow", "version": "1.0.98", "source": "crates.io"},
                      out["linux/amd64:/usr/local/bin/pageserver"])

    def test_audit_flags_cddl(self):
        # inferno by name, even without a licence lookup ...
        d = self.image_dir({"usr/local/bin/pageserver": self.elf(
            "pageserver", [pkg("pageserver", "0.1.0", "local", True), pkg("inferno", "0.11.21")])})
        r, out = self.audit(d)
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertIn("inferno 0.11.21", r.stderr)
        self.assertIn("linux/amd64:/usr/local/bin/pageserver", out)

    def test_audit_flags_cddl_by_licence(self):
        # ... and any crate whose crates.io licence is CDDL.
        d = self.image_dir({"usr/local/bin/proxy": self.elf(
            "proxy", [pkg("proxy", "0.1.0", "local", True), pkg("somecrate", "1.2.3")])})
        r, _ = self.audit(d, {"somecrate@1.2.3": "CDDL-1.0 OR MIT"})
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertIn("somecrate 1.2.3", r.stderr)
        self.assertIn("CDDL", r.stderr)

    def test_audit_flags_subzero(self):
        d = self.image_dir({"usr/local/bin/proxy": self.elf(
            "proxy", [pkg("proxy", "0.1.0", "local", True), pkg("subzero-core", "3.0.1", "git")])})
        r, _ = self.audit(d)
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertIn("subzero-core", r.stderr)

    def test_subzero_stub_is_not_a_finding(self):
        # Neon's public build links a path stub of subzero-core, not the private crate.
        d = self.image_dir({"usr/local/bin/proxy": self.elf(
            "proxy", [pkg("proxy", "0.1.0", "local", True), pkg("subzero-core", "3.0.1", "local")])})
        r, _ = self.audit(d)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_audit_flags_cddl_without_dep_v0(self):
        # Neon's compute image builds local_proxy with plain `cargo build`: no .dep-v0.
        # The crate source paths left in the binary still name inferno.
        path = (b"\0/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/"
                b"inferno-0.11.21/src/flamegraph/mod.rs\0"
                b"/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/anyhow-1.0.98/src/lib.rs\0")
        elf = Path(shutil.which("true")).read_bytes() + path
        d = self.image_dir({"usr/local/bin/local_proxy": elf})
        r, out = self.audit(d)
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertIn("inferno 0.11.21", r.stderr)
        self.assertIn({"name": "inferno", "version": "0.11.21", "source": "crates.io",
                       "evidence": "strings"}, out["linux/amd64:/usr/local/bin/local_proxy"])

    def test_audit_flags_subzero_checkout_without_dep_v0(self):
        elf = Path(shutil.which("true")).read_bytes() + (
            b"\0/root/.cargo/git/checkouts/subzero-0123456789abcdef/3962646/core/src/lib.rs\0")
        d = self.image_dir({"usr/local/bin/proxy": elf})
        r, _ = self.audit(d)
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertIn("subzero", r.stderr)

    def test_whiteout_removes_a_binary(self):
        bad = self.elf("old", [pkg("old", "0.1.0", "local", True), pkg("inferno", "0.11.21")])
        d = self.image_dir({"usr/bin/old": bad, "usr/bin/.wh.old": b""})
        r, out = self.audit(d)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(out, {})


class Runbook(unittest.TestCase):
    """audit_runs_before_visibility: the README's mirror runbook audits before
    any package is made public. (The pin-file half, `pins.py check` refusing a
    `mirror:` source without its audit file, belongs to Task 2's pins.py.)"""

    def test_audit_runs_before_visibility(self):
        text = README.read_text()
        m = re.search(r"^## Mirror\n(.*?)(?=^## |\Z)", text, re.M | re.S)
        self.assertIsNotNone(m, "README has no '## Mirror' section")
        steps = re.findall(r"^(\d+)\. (.*)$", m.group(1), re.M)
        self.assertTrue(steps, "the Mirror section has no numbered runbook")
        audit = [int(n) for n, s in steps if "audit-image.sh" in s]
        public = [int(n) for n, s in steps if re.search(r"\bpublic\b", s, re.I)]
        mirror = [int(n) for n, s in steps if "mirror-images.sh" in s]
        self.assertTrue(audit and public and mirror, steps)
        self.assertLess(max(mirror), min(audit))
        self.assertLess(max(audit), min(public))

    def test_every_mirrored_index_has_an_audit_file(self):
        text = README.read_text()
        m = re.search(r"^## Mirror\n(.*?)(?=^## |\Z)", text, re.M | re.S)
        self.assertIsNotNone(m)
        dsts = re.findall(r"`ghcr\.io/ostrium-labs/neon-archive/[^`]+` \| `sha256:([0-9a-f]{64})`", m.group(1))
        self.assertEqual(len(dsts), 4, "the Mirror table lists four destination indexes")
        for hexd in dsts:
            self.assertTrue((REPO / "deploy" / "pins" / "audit" / f"sha256-{hexd}.json").is_file(), hexd)


if __name__ == "__main__":
    unittest.main(verbosity=2)
