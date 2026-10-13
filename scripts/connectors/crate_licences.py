#!/usr/bin/env python3
"""Every crate compiled into a `loams-connector-*` family crate passes D359.

CN2 plan Task 3 (design §33 D359; Owner ruling OR-6). `cargo metadata` over the fabric
workspace gives each package's licence expression and the resolved dependency graph. For each
workspace member named `loams-connector-*` (the SDK included: it is compiled into every family
crate) this walks its normal and build dependencies, transitively — dev dependencies are not
compiled into the crate — and refuses any whose licence is missing or leaves no choice outside
the refused set:

  * the ids under [deny] in connectors/licences.toml, and
  * the refused families whatever the spelling (AGPL, BUSL, SSPL, Elastic, NOASSERTION,
    unknown), as gen_registry.py's REFUSED_LICENCE_FAMILIES has them. BSL-1.0 (Boost) is not
    BUSL and passes.

`OR` (and the old `/` spelling) is satisfied by either side, `AND` needs both, and
`X WITH exception` is judged by X. Crates outside the family are cargo-deny's job
(fabric/deny.toml), which this does not replace.

A workspace with no family crate yet (before CN2 Task 4 adds loams-connector-sdk) passes and
says so. The same rules are `loams_flow::validate::LicenceGate::check_family_crates`, tested by
fabric/crates/loams-flow/tests/licence_gate.rs's `family_crates_pass_licence_gate`;
`--self-test` runs this script over the same fixture metadata.

Usage:
  python3 scripts/connectors/crate_licences.py                       # cargo metadata over fabric/
  python3 scripts/connectors/crate_licences.py --metadata FILE.json  # a saved metadata document
  python3 scripts/connectors/crate_licences.py --self-test
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LICENCES_PATH = REPO_ROOT / "connectors" / "licences.toml"
FABRIC_MANIFEST = REPO_ROOT / "fabric" / "Cargo.toml"
FIXTURES = REPO_ROOT / "fabric" / "crates" / "loams-flow" / "tests" / "fixtures" / "licences"

FAMILY_PREFIX = "loams-connector-"
REFUSED_FAMILIES = ("AGPL", "BUSL", "SSPL", "ELASTIC", "NOASSERTION", "UNKNOWN")


def is_family_crate(name: str) -> bool:
    return name.startswith(FAMILY_PREFIX) and len(name) > len(FAMILY_PREFIX)


def read_denied(path: Path) -> set[str]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    denied = {str(i).strip() for i in data.get("deny", {}).get("ids", [])}
    if not denied:
        raise SystemExit(f"{path}: [deny] ids is empty, so the gate would refuse nothing (D359)")
    return denied


def id_refused(spdx: str, denied: set[str]) -> bool:
    if spdx in denied:
        return True
    return spdx.split("-", 1)[0].upper() in REFUSED_FAMILIES


def expression_acceptable(expression: str, denied: set[str]) -> bool:
    """Whether an SPDX expression leaves a choice outside the refused set. Unreadable is False."""
    tokens = expression.replace("(", " ( ").replace(")", " ) ").replace("/", " OR ").split()
    at = 0

    def peek() -> str | None:
        return tokens[at] if at < len(tokens) else None

    def or_expression() -> bool | None:
        nonlocal at
        acceptable = and_expression()
        while acceptable is not None and (peek() or "").upper() == "OR":
            at += 1
            right = and_expression()
            if right is None:
                return None
            acceptable = acceptable or right
        return acceptable

    def and_expression() -> bool | None:
        nonlocal at
        acceptable = term()
        while acceptable is not None and (peek() or "").upper() == "AND":
            at += 1
            right = term()
            if right is None:
                return None
            acceptable = acceptable and right
        return acceptable

    def term() -> bool | None:
        nonlocal at
        token = peek()
        if token is None:
            return None
        at += 1
        if token == "(":
            inner = or_expression()
            if peek() != ")":
                return None
            at += 1
            return inner
        if token == ")" or token.upper() in ("AND", "OR", "WITH"):
            return None
        if (peek() or "").upper() == "WITH":
            at += 1
            if peek() is None:
                return None
            at += 1
        return not id_refused(token.rstrip("+"), denied)

    result = or_expression()
    return bool(result) and at == len(tokens)


def check_crate(family: str, name: str, version: str, licence: str | None, denied: set[str]) -> str | None:
    if not licence or not licence.strip():
        return f"{family}: dependency {name} {version} declares no licence, which the gate refuses (D359)"
    if not expression_acceptable(licence.strip(), denied):
        return f"{family}: dependency {name} {version} is licensed {licence.strip()}, which the gate refuses (D359)"
    return None


def compiled_in(dep: dict) -> bool:
    kinds = dep.get("dep_kinds") or []
    return not kinds or any(k.get("kind") != "dev" for k in kinds)


def check_metadata(metadata: dict, denied: set[str]) -> tuple[dict[str, list[str]], list[str]]:
    """Family crate -> its compiled-in dependency names, and the problems found."""
    packages = {p["id"]: p for p in metadata.get("packages", [])}
    resolve = metadata.get("resolve") or {}
    if "nodes" not in resolve:
        return {}, ["cargo metadata: no resolve.nodes (run without --no-deps)"]
    edges = {
        node["id"]: [d["pkg"] for d in node.get("deps", []) if compiled_in(d)]
        for node in resolve["nodes"]
    }
    report: dict[str, list[str]] = {}
    problems: list[str] = []
    for member in metadata.get("workspace_members", []):
        package = packages.get(member)
        if not package or not is_family_crate(package["name"]):
            continue
        family = package["name"]
        seen: set[str] = set()
        stack = list(edges.get(member, []))
        while stack:
            pkg = stack.pop()
            if pkg in seen:
                continue
            seen.add(pkg)
            stack.extend(edges.get(pkg, []))
        names: set[str] = set()
        for pkg in sorted(seen):
            dep = packages.get(pkg)
            if dep is None:
                problems.append(f"cargo metadata: {family}: dependency {pkg} has no package entry")
                continue
            problem = check_crate(family, dep["name"], dep.get("version", ""), dep.get("license"), denied)
            if problem:
                problems.append(problem)
            names.add(dep["name"])
        report[family] = sorted(names)
    return report, problems


def cargo_metadata() -> dict:
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--all-features",
        "--manifest-path",
        str(FABRIC_MANIFEST),
    ]
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    return json.loads(output)


def run(metadata: dict, denied: set[str]) -> int:
    report, problems = check_metadata(metadata, denied)
    if not report and not problems:
        print(f"crate-licences: no {FAMILY_PREFIX}* crate in the workspace yet; nothing to gate")
    for family, names in report.items():
        print(f"crate-licences: {family}: {len(names)} compiled-in crate(s) checked")
    for problem in problems:
        print(f"crate-licences: FAIL: {problem}")
    print(f"crate-licences: {len(problems)} problem(s)")
    return 1 if problems else 0


def self_test() -> int:
    """The cases family_crates_pass_licence_gate and denied_ids_still_refused_for_shipped assert."""
    failures: list[str] = []
    denied = read_denied(FIXTURES / "licences.toml")
    metadata = json.loads((FIXTURES / "cargo-metadata.json").read_text(encoding="utf-8"))

    for name, expected in [
        ("loams-connector-sdk", True),
        ("loams-connector-postgres", True),
        ("loams-connect", False),
        ("loams-flow", False),
        ("connector-sdk", False),
    ]:
        if is_family_crate(name) != expected:
            failures.append(f"is_family_crate({name!r}) should be {expected}")

    report, problems = check_metadata(metadata, denied)
    want = {"loams-connector-fixture": ["build-helper", "deep-crate", "dual-agpl", "good-crate", "old-style"]}
    if report != want or problems:
        failures.append(f"fixture should pass with {want}, got {report} and {problems}")

    empty = copy.deepcopy(metadata)
    empty["workspace_members"] = ["path+file:///fabric/crates/loams-flow#0.1.0"]
    if check_metadata(empty, denied) != ({}, []):
        failures.append("a workspace with no family crate should pass with nothing to report")

    def with_licence(name: str, licence: str | None) -> dict:
        changed = copy.deepcopy(metadata)
        for package in changed["packages"]:
            if package["name"] == name:
                package["license"] = licence
        return changed

    _, problems = check_metadata(with_licence("deep-crate", "BUSL-1.1"), denied)
    want_problem = (
        "loams-connector-fixture: dependency deep-crate 2.0.0 is licensed BUSL-1.1, which the gate refuses (D359)"
    )
    if problems != [want_problem]:
        failures.append(f"a transitive BUSL crate should be refused: {problems}")
    _, problems = check_metadata(with_licence("good-crate", None), denied)
    if len(problems) != 1 or "good-crate" not in problems[0] or "no licence" not in problems[0]:
        failures.append(f"an unlicensed crate should be refused: {problems}")

    for licence in ["BUSL-1.1", "AGPL-3.0-only", "SSPL-1.0 AND MIT", "AGPL-3.0", "unknown"]:
        if expression_acceptable(licence, denied):
            failures.append(f"{licence} should be refused")
    for licence in [
        "MIT OR AGPL-3.0-only",
        "(MIT OR Apache-2.0) AND Unicode-3.0",
        "BSL-1.0",
        "MIT/Apache-2.0",
        "Apache-2.0 WITH LLVM-exception",
    ]:
        if not expression_acceptable(licence, denied):
            failures.append(f"{licence} should pass")

    for failure in failures:
        print(f"crate-licences: self-test FAIL: {failure}")
    print(f"crate-licences: self-test {len(failures)} failure(s)")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--metadata", type=Path, help="a saved `cargo metadata --format-version 1` document")
    parser.add_argument("--self-test", action="store_true", help="the shared fixture cases")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    denied = read_denied(LICENCES_PATH)
    metadata = json.loads(args.metadata.read_text(encoding="utf-8")) if args.metadata else cargo_metadata()
    return run(metadata, denied)


if __name__ == "__main__":
    sys.exit(main())
