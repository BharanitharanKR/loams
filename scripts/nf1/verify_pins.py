#!/usr/bin/env python3
"""Verify every image Loams pins that Loams Postgres built (NF1 Task 12).

    verify_pins.py [--pins deploy/pins/loams-postgres-images.toml]

For each image in the pin file whose `source` is a fork commit
(`ostrium-labs/loams-postgres@<sha>`):

1. `cosign verify` with the shared identity: Sigstore's public-good issuer for
   GitHub Actions, and a certificate whose identity is ostrium-labs/loams-postgres's
   loams-release.yml or loams-sign.yml at refs/heads/main. An unsigned digest,
   or one signed from another branch or another repository (the old name
   ostrium-labs/neon included), fails.
2. `gh attestation verify --repo ostrium-labs/loams-postgres --source-digest
   <the pin's commit> --source-ref refs/heads/main`: the build provenance, made
   by the fork for that commit (another repository's attestation does not count).
3. The SPDX SBOM attestations (`--predicate-type https://spdx.dev/Document/v2.3`,
   the same constraints): one per platform (amd64 and arm64, so at least two
   distinct documents); no package licence names CDDL, and every package outside
   Debian's own has only allowlisted licences. Debian packages (purl
   `pkg:deb/...`) are listed separately, for information. NOASSERTION is
   counted, not refused.
4. For a compute image (`compute-v<N>`): an artifact of type
   application/vnd.loams.sources.v1 is attached (`oras discover`), the sources
   of its GPL components, and is signed with the same identity (`cosign verify`).
Every tool call has a timeout (TOOL_TIMEOUT seconds, default 300); a timeout is
a failure of that image.

A `mirror:` source (Neon's images, until NF1 Task 14) is skipped with a notice.
Tools: COSIGN, GH and ORAS name the binaries (default cosign, gh, oras).
Exit 0 all verified, 1 a failure, 2 usage. Stdlib only.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIN_FILE = ROOT / "deploy" / "pins" / "loams-postgres-images.toml"

ISSUER = "https://token.actions.githubusercontent.com"
IDENTITY = (r"^https://github.com/ostrium-labs/loams-postgres/\.github/workflows/"
            r"loams-(release|sign)\.yml@refs/heads/main$")
FORK = "ostrium-labs/loams-postgres"
SOURCE_REF = "refs/heads/main"
PLATFORMS = 2  # amd64 and arm64: one SBOM attestation each
SBOM_PREDICATE = "https://spdx.dev/Document/v2.3"
SOURCES_TYPE = "application/vnd.loams.sources.v1"
FORK_SOURCE = re.compile(r"ostrium-labs/loams-postgres@[0-9a-f]{40}")

# The plan's licence allowlist (shared contracts; GPL only with the sources
# artifact, which step 4 checks for compute images), plus the permissive
# licences deny.toml allows for Rust crates.
ALLOWED = {
    "PostgreSQL", "MIT", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "ISC", "Zlib", "MPL-2.0",
    "LGPL-2.1-or-later", "GPL-2.0-or-later", "GPL-3.0-or-later",
    "0BSD", "CC0-1.0", "Unicode-3.0",
}
NO_ASSERTION = {"NOASSERTION", "NONE", ""}


def tool(name):
    return os.environ.get(name.upper(), name)


def run(*args):
    try:
        return subprocess.run([str(a) for a in args], capture_output=True, text=True,
                              timeout=int(os.environ.get("TOOL_TIMEOUT", "300")))
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(e.cmd, 124, "", f"timed out after {e.timeout} s")


# ------------------------------------------------------------- SPDX licences

def _tokens(expr):
    return re.findall(r"\(|\)|[A-Za-z0-9.+:-]+", expr)


def licence_ok(expr, allowed=ALLOWED):
    """Evaluates an SPDX expression: OR needs one allowed side, AND both.
    `X WITH exception` counts as X. Returns (ok, the licence ids it names)."""
    toks = _tokens(expr)
    names = [t for t in toks if t not in ("(", ")", "AND", "OR", "WITH")]
    pos = 0

    def parse_or():
        nonlocal pos
        ok = parse_and()
        while pos < len(toks) and toks[pos] == "OR":
            pos += 1
            ok = parse_and() or ok
        return ok

    def parse_and():
        nonlocal pos
        ok = parse_atom()
        while pos < len(toks) and toks[pos] == "AND":
            pos += 1
            ok = parse_atom() and ok
        return ok

    def parse_atom():
        nonlocal pos
        if pos >= len(toks):
            raise ValueError(f"bad SPDX expression: {expr!r}")
        t = toks[pos]
        pos += 1
        if t == "(":
            ok = parse_or()
            if pos >= len(toks) or toks[pos] != ")":
                raise ValueError(f"bad SPDX expression: {expr!r}")
            pos += 1
        else:
            ok = t in allowed
        if pos < len(toks) and toks[pos] == "WITH":
            pos += 2  # the exception id does not change the licence
        return ok

    ok = parse_or()
    if pos != len(toks):
        raise ValueError(f"bad SPDX expression: {expr!r}")
    return ok, names


def sbom_findings(spdx):
    """(failures, debian lines, noassertion count) for one SPDX document."""
    failures, debian, unknown = [], [], 0
    for pkg in spdx.get("packages", []):
        name = f"{pkg.get('name', '?')} {pkg.get('versionInfo', '')}".strip()
        purls = [r.get("referenceLocator", "") for r in pkg.get("externalRefs", [])
                 if r.get("referenceType") == "purl"]
        is_debian = any(p.startswith("pkg:deb/") for p in purls)
        exprs = {pkg.get(k, "NOASSERTION") for k in ("licenseConcluded", "licenseDeclared")}
        exprs = {e for e in exprs if e not in NO_ASSERTION}
        if not exprs:
            unknown += 1
            continue
        for expr in exprs:
            try:
                ok, names = licence_ok(expr)
            except ValueError as e:
                failures.append(f"{name}: {e}")
                continue
            if any(n.upper().startswith("CDDL") for n in names):
                failures.append(f"{name}: CDDL ({expr})")
            elif is_debian:
                debian.append(f"{name}: {expr}")
            elif not ok:
                failures.append(f"{name}: not on the allowlist ({expr})")
    return failures, debian, unknown


def spdx_documents(gh_json):
    """The SPDX predicates in `gh attestation verify --format json` output."""
    docs = []

    def walk(node):
        if isinstance(node, dict):
            if node.get("predicateType") == SBOM_PREDICATE and isinstance(node.get("predicate"), dict):
                docs.append(node["predicate"])
                return
            if "spdxVersion" in node and "packages" in node:
                docs.append(node)
                return
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(gh_json)
    return docs


# ------------------------------------------------------------------- checks

def cosign_verify(ref):
    r = run(tool("cosign"), "verify", "--certificate-oidc-issuer", ISSUER,
            "--certificate-identity-regexp", IDENTITY, ref)
    return None if r.returncode == 0 else (r.stderr or r.stdout).strip()[-300:]


def verify_image(key, img):
    ref = f"{img['repository']}@{img['digest']}"
    commit = img["source"].split("@", 1)[1]
    scope = ["--repo", FORK, "--source-digest", commit, "--source-ref", SOURCE_REF]
    problems = []
    err = cosign_verify(ref)
    if err:
        problems.append(f"cosign verify failed: {err}")
    r = run(tool("gh"), "attestation", "verify", f"oci://{ref}", *scope)
    if r.returncode != 0:
        problems.append(f"no build provenance attestation: {(r.stderr or r.stdout).strip()[-300:]}")
    r = run(tool("gh"), "attestation", "verify", f"oci://{ref}", *scope,
            "--predicate-type", SBOM_PREDICATE, "--format", "json")
    docs = []
    if r.returncode == 0:
        try:
            docs = spdx_documents(json.loads(r.stdout))
        except json.JSONDecodeError:
            pass
    # One per platform: distinct documents (by namespace, else by content).
    distinct = {d.get("documentNamespace") or json.dumps(d, sort_keys=True) for d in docs}
    if not docs:
        problems.append("no SPDX SBOM attestation")
    elif len(distinct) < PLATFORMS:
        problems.append(f"{len(distinct)} SPDX SBOM attestation(s); one per platform ({PLATFORMS}) is required")
    for doc in docs:
        failures, debian, unknown = sbom_findings(doc)
        problems += [f"SBOM: {f}" for f in failures]
        if debian:
            print(f"{key}: {len(debian)} Debian package licence(s), listed for information:")
            for line in debian:
                print(f"  {line}")
        if unknown:
            print(f"{key}: {unknown} package(s) without a licence assertion")
    if key.startswith("compute-"):
        r = run(tool("oras"), "discover", "--artifact-type", SOURCES_TYPE, "--format", "json", ref)
        found = []
        if r.returncode == 0:
            try:
                out = json.loads(r.stdout)
                found = out.get("manifests") or out.get("referrers") or []
            except json.JSONDecodeError:
                pass
        if not found:
            problems.append(f"no {SOURCES_TYPE} artifact attached (the GPL components' sources)")
        # Attached is not enough: one of them must be signed by the release.
        elif not any(cosign_verify(f"{img['repository']}@{a.get('digest', '')}") is None for a in found):
            problems.append(f"the {SOURCES_TYPE} artifact is not signed by loams-release.yml or loams-sign.yml")
    return problems


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--pins", default=str(PIN_FILE))
    args = p.parse_args(argv)
    try:
        pins = tomllib.loads(Path(args.pins).read_text())
    except (OSError, tomllib.TOMLDecodeError) as e:
        print(f"verify-pins: {e}", file=sys.stderr)
        return 2
    failed = 0
    for key, img in sorted(pins.get("images", {}).items()):
        source = img.get("source", "")
        if source.startswith("mirror:"):
            print(f"::notice::{key}: {source} is a mirror of Neon's build; not verified (NF1 Task 12)")
            continue
        if not FORK_SOURCE.fullmatch(source):
            print(f"{key}: unknown source {source!r}", file=sys.stderr)
            failed += 1
            continue
        problems = verify_image(key, img)
        if problems:
            failed += 1
            for prob in problems:
                print(f"{key}: {prob}", file=sys.stderr)
        else:
            print(f"{key}: verified {img['repository']}@{img['digest']}")
    if failed:
        print(f"verify-pins: {failed} image(s) failed", file=sys.stderr)
        return 1
    print("verify-pins: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
