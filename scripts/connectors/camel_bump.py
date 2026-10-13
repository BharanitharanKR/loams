#!/usr/bin/env python3
"""The Camel bump procedure for the fallback runtime (CN2 plan Task 27, CN-R5).

Camel runs only as a declared fallback (OR-4), so a Camel bump touches only
the fallback rows. The procedure, in order:

  pins                      the files that pin the current Camel version (what
                            a bump pull request edits)
  catalog-snapshot          regenerate connectors/vendor/camel-catalog-<x>.json.zst
                            (CN2 Task 22's camel_manifest.py)
  catalog-drift             camel_catalog.py --check against the new catalog
                            (CN1 Task 2; the Camel column of Appendix A)
  directions-match-catalog  the Rust test camel_direction_matches_catalog
                            (CN2 Task 22)
  validate-fallback-routes  `camel validate` over connect/routes (CN1 Task 12,
                            CN2 Task 22)
  fallback-suites           every manifest with a Camel runtime or fallback
                            reruns its suites (CN2 Task 22's harnesses)
  camel-iggy-recheck        re-read camel-iggy's commit behaviour (ruling
                            R0.25): until it commits after processing, no
                            fallback delivers through it

Each step is `ready` (its inputs exist and it can run), `deferred` (a task
that has not landed owns its inputs; `owner` names it) or `manual` (a person
does it). Q352's Quarkus measurement is dropped: Camel stays on Camel Main.

Usage:
    camel_bump.py --to VERSION [--from VERSION] [--dry-run] [--json PATH|-]

`--dry-run` plans and exits 0 for a well-formed version. Without it, the
ready steps run (catalog-drift needs $LOAMS_CAMEL_CATALOG, the new version's
camel-catalog jar), and the script exits 1 if a step failed and 3 if a step
is still deferred: a bump is complete only when every step ran. A malformed
version, or a change of Camel's major version (a plan change, not a bump),
exits 2. Standard library only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERSION = re.compile(r"^\d+\.\d+\.\d+$")
WORKFLOW = ROOT / ".github/workflows/fabric.yml"
# Where a Camel version is pinned (docs keep their history and are not pins).
PIN_ROOTS = [".github/workflows", "scripts/connectors", "connectors", "connect", "deploy"]
PIN_SUFFIXES = {".yml", ".yaml", ".toml", ".py", ".sh", ".json", ".properties", ".tmpl"}


def current_version() -> str | None:
    if not WORKFLOW.exists():
        return None
    m = re.search(r"camel-catalog/(\d+\.\d+\.\d+)/", WORKFLOW.read_text())
    return m.group(1) if m else None


def pin_files(version: str) -> list[str]:
    hits: list[str] = []
    for top in PIN_ROOTS:
        base = ROOT / top
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if p.is_file() and p.suffix in PIN_SUFFIXES and p.name != "camel_bump.py":
                try:
                    text = p.read_text()
                except (UnicodeDecodeError, OSError):
                    continue
                if version in text:
                    lines = [i + 1 for i, l in enumerate(text.splitlines()) if version in l]
                    hits.append(f"{p.relative_to(ROOT)}:{','.join(map(str, lines))}")
    return hits


def camel_manifests() -> list[str]:
    """Manifests whose runtime, a direction override, or fallback is Camel."""
    out = []
    for p in sorted((ROOT / "connectors/registry").glob("*.yaml")):
        if re.search(r"^\s+kind:\s*['\"]?camel['\"]?\s*$", p.read_text(), re.M):
            out.append(p.stem)
    return out


def rust_test_exists(name: str) -> bool:
    for p in (ROOT / "fabric/crates").rglob("*.rs"):
        if f"fn {name}" in p.read_text(errors="ignore"):
            return True
    return False


def plan(frm: str, to: str) -> list[dict]:
    steps: list[dict] = []
    steps.append({
        "step": "pins",
        "status": "ready",
        "files": pin_files(frm),
        "detail": f"a bump pull request moves these pins from {frm} to {to}",
    })
    manifest_py = ROOT / "scripts/connectors/camel_manifest.py"
    snapshot = {
        "step": "catalog-snapshot",
        "command": ["python3", "scripts/connectors/camel_manifest.py", "--snapshot", "--version", to],
        "detail": f"regenerate connectors/vendor/camel-catalog-{to}.json.zst",
    }
    if manifest_py.exists():
        snapshot["status"] = "ready"
    else:
        snapshot.update(status="deferred", owner="CN2 Task 22",
                        detail=snapshot["detail"] + " (scripts/connectors/camel_manifest.py is not built)")
    steps.append(snapshot)
    steps.append({
        "step": "catalog-drift",
        "status": "ready",
        "command": ["python3", "scripts/connectors/camel_catalog.py", "--check", "--tag", to],
        "detail": "the Camel column of Appendix A against the new camel-catalog ($LOAMS_CAMEL_CATALOG)",
    })
    directions = {
        "step": "directions-match-catalog",
        "command": ["cargo", "test", "--manifest-path", "fabric/Cargo.toml", "-p", "loams-flow",
                    "--locked", "camel_direction_matches_catalog"],
        "detail": "every Camel direction or fallback names a component with that consumer or producer",
    }
    if rust_test_exists("camel_direction_matches_catalog"):
        directions["status"] = "ready"
    else:
        directions.update(status="deferred", owner="CN2 Task 22",
                          detail=directions["detail"] + " (the test is not written)")
    steps.append(directions)
    routes = ROOT / "connect/routes"
    overlays = routes / "overlays"
    validate = {
        "step": "validate-fallback-routes",
        "detail": "camel validate over connect/routes/{templates,overlays}, in the Camel image",
    }
    if overlays.exists():
        validate.update(status="ready",
                        command=["camel", "validate", str(routes.relative_to(ROOT))])
    else:
        validate.update(status="deferred", owner="CN2 Task 22",
                        detail=validate["detail"] + " (connect/routes/overlays is not built)")
    steps.append(validate)
    manifests = camel_manifests()
    steps.append({
        "step": "fallback-suites",
        "status": "deferred",
        "owner": "CN2 Task 22",
        "manifests": manifests,
        "detail": "rerun the suites of every manifest with a Camel runtime or fallback "
                  "(the fallback harnesses are not built)",
    })
    steps.append({
        "step": "camel-iggy-recheck",
        "status": "manual",
        "detail": "ruling R0.25: re-read camel-iggy's offset commit in the new sources; record "
                  "the finding in docs/plans/cn2-dependency-spike.md section 6",
    })
    return steps


def run(steps: list[dict]) -> int:
    failed = deferred = 0
    for s in steps:
        if s["status"] == "deferred":
            deferred += 1
            print(f"DEFERRED {s['step']}: {s['detail']} [{s['owner']}]", file=sys.stderr)
            continue
        if s["status"] == "manual" or "command" not in s:
            print(f"{s['status'].upper():8} {s['step']}: {s['detail']}", file=sys.stderr)
            continue
        if s["step"] == "catalog-drift" and not os.environ.get("LOAMS_CAMEL_CATALOG"):
            s["status"] = "failed"
            failed += 1
            print(f"FAILED   {s['step']}: $LOAMS_CAMEL_CATALOG names no camel-catalog jar", file=sys.stderr)
            continue
        r = subprocess.run(s["command"], cwd=ROOT)
        s["status"] = "passed" if r.returncode == 0 else "failed"
        failed += r.returncode != 0
        print(f"{s['status'].upper():8} {s['step']}", file=sys.stderr)
    if failed:
        return 1
    if deferred:
        print(f"the bump is not complete: {deferred} step(s) wait for their owning tasks", file=sys.stderr)
        return 3
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--to", required=True, help="the Camel version to move to")
    ap.add_argument("--from", dest="frm", help="the current version (default: fabric.yml's pin)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", help="write the plan as JSON to PATH, or - for stdout")
    a = ap.parse_args()

    frm = a.frm or current_version()
    if not frm or not VERSION.match(frm):
        print(f"cannot read the current Camel version ({frm!r})", file=sys.stderr)
        return 2
    if not VERSION.match(a.to):
        print(f"--to {a.to!r} is not a Camel version (MAJOR.MINOR.PATCH)", file=sys.stderr)
        return 2
    if a.to.split(".")[0] != frm.split(".")[0]:
        print(f"{frm} -> {a.to} changes Camel's major version: that is a plan change, not a bump",
              file=sys.stderr)
        return 2

    steps = plan(frm, a.to)
    code = 0
    if not a.dry_run:
        code = run(steps)
    doc = {"from": frm, "to": a.to, "dry_run": a.dry_run, "steps": steps}
    if a.json == "-":
        print(json.dumps(doc, indent=2))
    else:
        if a.json:
            Path(a.json).write_text(json.dumps(doc, indent=2) + "\n")
        if a.dry_run:
            for s in steps:
                owner = f" [{s['owner']}]" if "owner" in s else ""
                print(f"{s['status']:8} {s['step']}: {s['detail']}{owner}")
    return code


if __name__ == "__main__":
    sys.exit(main())
