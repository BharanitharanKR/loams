#!/usr/bin/env python3
"""Crate upkeep per connector family crate (CN2 plan Task 27).

For each `fabric/crates/loams-connector-*` crate (the SDK and every family,
OR-6), the scheduled `crate-upkeep` job reports:

  advisories  every RustSec advisory `cargo deny check advisories` raises on a
              crate in that family's dependency graph (normal and build
              edges, from `cargo metadata`), so an advisory in a shared crate
              reaches every family that compiles it;
  outdated    the family's direct dependencies with a newer release
              (`cargo outdated --root-deps-only`), each marked `update`, or
              `hold` while the release is younger than 14 days (Global
              Constraints, "each new dependency at least 14 days old"; the
              date comes from the crates.io API).

With `--open-issues`, one GitHub issue per family carries the report
(label `crate-upkeep`, title `crate upkeep: <crate>`): created when a family
has findings, updated while it has them, and closed when it is clean.

Usage:
    crate_upkeep.py [--out DIR] [--open-issues] [--today YYYY-MM-DD]
    crate_upkeep.py --self-test

It needs `cargo`, `cargo-deny` and `cargo-outdated` (and `gh` for issues).
Requests to crates.io carry only a User-Agent naming this repository. A tool
that fails is reported in every family's report, never silently dropped.
Standard library only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FABRIC = ROOT / "fabric"
LABEL = "crate-upkeep"
HOLD_DAYS = 14
USER_AGENT = "loams-crate-upkeep (https://github.com/ostrium-labs/loams)"


# ------------------------------------------------------------------ graph --

def family_crates(metadata: dict) -> list[str]:
    members = set(metadata.get("workspace_members", []))
    return sorted(
        p["name"] for p in metadata["packages"]
        if p["id"] in members and p["name"].startswith("loams-connector-")
    )


def closures(metadata: dict, roots: list[str]) -> dict[str, set[str]]:
    """Crate names each root compiles (normal and build edges, not dev)."""
    by_id = {p["id"]: p["name"] for p in metadata["packages"]}
    id_of = {p["name"]: p["id"] for p in metadata["packages"] if p["id"] in set(metadata.get("workspace_members", []))}
    edges: dict[str, list[str]] = {}
    for node in metadata["resolve"]["nodes"]:
        out = []
        for d in node.get("deps", []):
            kinds = [k.get("kind") for k in d.get("dep_kinds", [{"kind": None}])]
            if any(k in (None, "build") for k in kinds):
                out.append(d["pkg"])
        edges[node["id"]] = out
    result = {}
    for r in roots:
        seen: set[str] = set()
        stack = [id_of[r]]
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(edges.get(n, []))
        result[r] = {by_id[i] for i in seen if i != id_of[r]}
    return result


def parse_deny(lines: str) -> list[dict]:
    """cargo-deny's JSON diagnostics → [{id, crate, version, title, severity}]."""
    out = []
    for line in lines.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        f = d.get("fields", {})
        adv = f.get("advisory")
        if d.get("type") != "diagnostic" or not adv:
            continue
        crate, version = adv.get("package"), None
        for g in f.get("graphs", []):
            k = g.get("Krate") or {}
            crate, version = k.get("name", crate), k.get("version")
            break
        out.append({
            "id": adv.get("id", "?"),
            "crate": crate or "?",
            "version": version or "?",
            "title": adv.get("title", f.get("message", "")),
            "severity": f.get("severity", "?"),
            "kind": f.get("code", "?"),
        })
    return out


def parse_outdated(text: str) -> list[dict]:
    """cargo-outdated's JSON → [{name, project, latest}] with a newer latest."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        for dep in d.get("dependencies", []):
            latest, project = dep.get("latest"), dep.get("project")
            if latest and latest not in ("---", "Removed") and latest != project:
                out.append({"name": dep["name"], "project": project, "latest": latest,
                            "kind": dep.get("kind") or "Normal"})
    return out


# ----------------------------------------------------------------- report --

def classify(outdated: list[dict], released: dict[tuple[str, str], str | None], today: dt.date) -> list[dict]:
    rows = []
    for o in outdated:
        date = released.get((o["name"], o["latest"]))
        if date is None:
            action = "check (release date unknown)"
        else:
            age = (today - dt.date.fromisoformat(date[:10])).days
            action = "update" if age >= HOLD_DAYS else f"hold ({age} days old, under {HOLD_DAYS})"
        rows.append({**o, "released": date[:10] if date else None, "action": action})
    return rows


def report(family: str, advisories: list[dict], outdated: list[dict], errors: list[str], today: dt.date) -> str:
    lines = [f"Crate upkeep for `{family}` on {today.isoformat()} (CN2 Task 27, `crate-upkeep`).", ""]
    if errors:
        lines += ["**Tool errors** (this report is incomplete):", ""]
        lines += [f"- {e}" for e in errors] + [""]
    lines += ["### Advisories", ""]
    if advisories:
        lines += ["| Advisory | Crate | Version | Kind | Title |", "|---|---|---|---|---|"]
        lines += [f"| {a['id']} | {a['crate']} | {a['version']} | {a['kind']} | {a['title']} |" for a in advisories]
    else:
        lines.append("None.")
    lines += ["", "### Outdated direct dependencies", ""]
    if outdated:
        lines += ["| Crate | Locked | Latest | Released | Action |", "|---|---|---|---|---|"]
        lines += [f"| {o['name']} | {o['project']} | {o['latest']} | {o['released'] or '?'} | {o['action']} |"
                  for o in outdated]
    else:
        lines.append("None.")
    lines += ["", "A `hold` waits for the 14-day rule; an `update` follows the family's licence and "
              "build checks (D359, `cargo deny check`)."]
    return "\n".join(lines) + "\n"


def build(metadata: dict, deny: list[dict], outdated_by: dict[str, list[dict]],
          released: dict[tuple[str, str], str | None], errors: dict[str, list[str]],
          today: dt.date) -> dict[str, dict]:
    fams = family_crates(metadata)
    graph = closures(metadata, fams)
    out = {}
    for f in fams:
        adv = sorted((a for a in deny if a["crate"] in graph[f]), key=lambda a: (a["id"], a["crate"]))
        rows = classify(outdated_by.get(f, []), released, today)
        errs = errors.get(f, []) + errors.get("*", [])
        out[f] = {
            "advisories": adv,
            "outdated": rows,
            "errors": errs,
            "dirty": bool(adv or rows or errs),
            "body": report(f, adv, rows, errs, today),
        }
    return out


# ------------------------------------------------------------------ tools --

def sh(cmd: list[str], cwd: Path = ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def release_date(name: str, version: str) -> str | None:
    req = urllib.request.Request(f"https://crates.io/api/v1/crates/{name}/{version}",
                                 headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)["version"]["created_at"]
    except Exception:  # noqa: BLE001 - a missing date is reported, not fatal
        return None


def gather(today: dt.date) -> dict[str, dict]:
    errors: dict[str, list[str]] = {}
    meta = sh(["cargo", "metadata", "--format-version", "1", "--locked",
               "--manifest-path", str(FABRIC / "Cargo.toml")])
    if meta.returncode != 0:
        raise SystemExit(f"cargo metadata failed: {meta.stderr}")
    metadata = json.loads(meta.stdout)
    deny = sh(["cargo", "deny", "--manifest-path", str(FABRIC / "Cargo.toml"),
               "--format", "json", "check", "advisories"])
    advisories = parse_deny(deny.stderr + deny.stdout)
    if deny.returncode != 0 and not advisories:
        errors.setdefault("*", []).append(f"cargo deny check advisories failed: {deny.stderr.strip()[-300:]}")
    outdated_by: dict[str, list[dict]] = {}
    released: dict[tuple[str, str], str | None] = {}
    for f in family_crates(metadata):
        r = sh(["cargo", "outdated", "--root-deps-only", "--format", "json",
                "--manifest-path", str(FABRIC / "crates" / f / "Cargo.toml")])
        if r.returncode != 0:
            errors.setdefault(f, []).append(f"cargo outdated failed: {r.stderr.strip()[-300:]}")
            continue
        outdated_by[f] = parse_outdated(r.stdout)
        for o in outdated_by[f]:
            key = (o["name"], o["latest"])
            if key not in released:
                released[key] = release_date(*key)
    return build(metadata, advisories, outdated_by, released, errors, today)


def sync_issues(result: dict[str, dict]) -> None:
    sh(["gh", "label", "create", LABEL, "--color", "c5def5", "--force",
        "--description", "Connector family crate upkeep (CN2 Task 27)"])
    listed = sh(["gh", "issue", "list", "--label", LABEL, "--state", "open",
                 "--limit", "200", "--json", "number,title"])
    if listed.returncode != 0:
        raise SystemExit(f"gh issue list failed: {listed.stderr}")
    open_by_title = {i["title"]: i["number"] for i in json.loads(listed.stdout)}
    for fam, r in result.items():
        title = f"crate upkeep: {fam}"
        number = open_by_title.get(title)
        if r["dirty"]:
            if number:
                cmd = ["gh", "issue", "edit", str(number), "--body", r["body"]]
            else:
                cmd = ["gh", "issue", "create", "--title", title, "--label", LABEL, "--body", r["body"]]
        elif number:
            cmd = ["gh", "issue", "close", str(number), "--comment", r["body"]]
        else:
            continue
        done = sh(cmd)
        print(f"{fam}: {' '.join(cmd[:3])} -> {done.returncode}")
        if done.returncode != 0:
            print(done.stderr, file=sys.stderr)


# -------------------------------------------------------------- self-test --

def self_test() -> int:
    ws = ["a 0.1 (path+file:///w/a)", "b 0.1 (path+file:///w/b)", "flow 0.1 (path+file:///w/flow)"]
    pkg = lambda n, i: {"name": n, "id": i}  # noqa: E731
    metadata = {
        "workspace_members": ws,
        "packages": [
            pkg("loams-connector-a", ws[0]), pkg("loams-connector-b", ws[1]), pkg("loams-flow", ws[2]),
            pkg("shared", "shared 1.0"), pkg("x", "x 1.0"), pkg("devonly", "devonly 1.0"),
            pkg("deep", "deep 1.0"),
        ],
        "resolve": {"nodes": [
            {"id": ws[0], "deps": [
                {"pkg": "shared 1.0", "dep_kinds": [{"kind": None}]},
                {"pkg": "x 1.0", "dep_kinds": [{"kind": "build"}]},
                {"pkg": "devonly 1.0", "dep_kinds": [{"kind": "dev"}]},
            ]},
            {"id": ws[1], "deps": [{"pkg": "shared 1.0", "dep_kinds": [{"kind": None}]}]},
            {"id": ws[2], "deps": [{"pkg": "devonly 1.0", "dep_kinds": [{"kind": None}]}]},
            {"id": "shared 1.0", "deps": [{"pkg": "deep 1.0", "dep_kinds": [{"kind": None}]}]},
            {"id": "x 1.0", "deps": []}, {"id": "devonly 1.0", "deps": []}, {"id": "deep 1.0", "deps": []},
        ]},
    }
    diag = lambda adv, crate: json.dumps({  # noqa: E731
        "type": "diagnostic",
        "fields": {"severity": "error", "code": "vulnerability", "message": "m",
                   "advisory": {"id": adv, "title": f"bad {crate}", "package": crate},
                   "graphs": [{"Krate": {"name": crate, "version": "1.0"}}]},
    })
    deny = "\n".join([
        diag("RUSTSEC-2026-0001", "deep"), diag("RUSTSEC-2026-0002", "x"),
        diag("RUSTSEC-2026-0003", "devonly"), "not json", json.dumps({"type": "summary"}),
    ])
    advisories = parse_deny(deny)
    assert [a["id"] for a in advisories] == ["RUSTSEC-2026-0001", "RUSTSEC-2026-0002", "RUSTSEC-2026-0003"], advisories
    outdated_a = parse_outdated(json.dumps({"crate_name": "loams-connector-a", "dependencies": [
        {"name": "x", "project": "1.0.0", "compat": "1.0.0", "latest": "2.0.0", "kind": "Build"},
        {"name": "shared", "project": "1.0.0", "compat": "1.1.0", "latest": "1.1.0", "kind": "Normal"},
        {"name": "same", "project": "3.0.0", "compat": "3.0.0", "latest": "3.0.0", "kind": "Normal"},
        {"name": "gone", "project": "1.0.0", "compat": "---", "latest": "Removed", "kind": "Normal"},
    ]}))
    assert [o["name"] for o in outdated_a] == ["x", "shared"], outdated_a
    today = dt.date(2026, 10, 13)
    released = {("x", "2.0.0"): "2026-10-08T00:00:00Z", ("shared", "1.1.0"): "2026-09-01T12:00:00Z"}
    r = build(metadata, advisories, {"loams-connector-a": outdated_a}, released,
              {"loams-connector-b": ["cargo outdated failed: boom"]}, today)
    assert sorted(r) == ["loams-connector-a", "loams-connector-b"], r.keys()
    a, b = r["loams-connector-a"], r["loams-connector-b"]
    # A shared crate's advisory reaches both families; a build dependency's
    # reaches its family; a dev-only crate's reaches none.
    assert [x["id"] for x in a["advisories"]] == ["RUSTSEC-2026-0001", "RUSTSEC-2026-0002"], a["advisories"]
    assert [x["id"] for x in b["advisories"]] == ["RUSTSEC-2026-0001"], b["advisories"]
    actions = {o["name"]: o["action"] for o in a["outdated"]}
    assert actions["x"].startswith("hold (5 days old"), actions
    assert actions["shared"] == "update", actions
    assert a["dirty"] and b["dirty"]
    assert "cargo outdated failed: boom" in b["body"] and "incomplete" in b["body"]
    assert "| x | 1.0.0 | 2.0.0 | 2026-10-08 | hold" in a["body"], a["body"]
    clean = build(metadata, [], {}, {}, {}, today)
    assert not clean["loams-connector-a"]["dirty"]
    assert "None." in clean["loams-connector-a"]["body"]
    print("crate_upkeep self-test: ok")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--out", help="write <crate>.md per family and summary.json here")
    ap.add_argument("--open-issues", action="store_true", help="create, update or close one issue per family")
    ap.add_argument("--today", help="the date to judge release age by (default: today, UTC)")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    today = dt.date.fromisoformat(a.today) if a.today else dt.datetime.now(dt.timezone.utc).date()
    result = gather(today)
    if a.out:
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        for f, r in result.items():
            (out / f"{f}.md").write_text(r["body"])
        (out / "summary.json").write_text(json.dumps(
            {f: {k: v for k, v in r.items() if k != "body"} for f, r in result.items()}, indent=2) + "\n")
    for f, r in result.items():
        print(f"{f}: {len(r['advisories'])} advisories, {len(r['outdated'])} outdated"
              f"{', tool errors' if r['errors'] else ''}")
    if a.open_issues:
        sync_issues(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
