#!/usr/bin/env python3
"""NF1 Task 16: a Loams Postgres release candidate's pins, for G3.

    candidate.py [--root DIR] apply --doc FILE --fork-commit SHA [--env FILE]

ostrium-labs/loams-postgres' gate (loams-gate.yml, G3) dispatches
nf1-candidate.yml here with the candidate's pin document: release-notes.py
pin-doc's output for the images it has just built and not yet tagged.
`apply` checks it and writes it over this checkout's pin file and every
consumer (`pins.py set --from-release FILE`), so the jobs that follow run on
the candidate's images. Nothing is committed: the change lives in the job's
checkout.

The document is refused unless:
- pins.py's own checks pass (validate());
- every image is either built from the candidate's commit (source
  `ostrium-labs/loams-postgres@<fork commit>`, a ghcr.io/ostrium-labs/loams-postgres-*
  repository), or exactly the entry Loams pins today (a partial release keeps
  the other images). So a candidate can never bring in a mirror, Neon's own
  images, or another repository.

With --env, the variables the jobs need are appended to FILE ($GITHUB_ENV):
PG_HEADERS_IMAGE (the storage image, which pg2.yml and pg2-e2e.yml read the
server headers from). The candidate's majors (supported, maintenance and beta)
are printed as `majors=<JSON list>` for $GITHUB_OUTPUT.

Exit codes: 0 applied, 1 refused, 2 usage. Stdlib only.
"""

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("pins", HERE / "pins.py")
pins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pins)

COMMIT = re.compile(r"[0-9a-f]{40}")


class Refused(Exception):
    pass


def check(doc, current, fork_commit, root):
    """Every reason `doc` is not a candidate of `fork_commit`, as messages."""
    errs = pins.validate(doc, root)
    if errs:
        return errs
    ours = f"ostrium-labs/loams-postgres@{fork_commit}"
    pinned = (current or {}).get("images", {})
    for key, img in doc["images"].items():
        if img.get("source") == ours and img["repository"].startswith(pins.FORK_REPOSITORY_PREFIX):
            continue
        if pinned.get(key) == img:
            continue
        errs.append(f"images.{key}: neither built from {ours} nor the image Loams pins today")
    return errs


def majors(doc):
    m = doc["majors"]
    return sorted(set(m["supported"]) | set(m["maintenance"]) | set(m["beta"]))


def apply(root, doc_path, fork_commit, env_path=None):
    if not COMMIT.fullmatch(fork_commit):
        raise Refused(f"--fork-commit must be 40 hex characters, not {fork_commit!r}")
    try:
        text = Path(doc_path).read_text(encoding="utf-8")
    except OSError as e:
        raise Refused(f"{doc_path}: {e}") from None
    try:
        doc = pins.load_toml(text, doc_path)
        current = pins.load_toml(pins.read(root, pins.PIN_FILE), pins.PIN_FILE)
    except pins.Refused as e:
        raise Refused(str(e)) from None
    errs = check(doc, current, fork_commit, root)
    if errs:
        raise Refused("; ".join(errs))
    if pins.cmd_set(root, str(Path(doc_path).resolve())) != 0:
        raise Refused("pins.py set refused the document")
    storage = doc["images"]["storage"]
    if env_path:
        with open(env_path, "a", encoding="utf-8") as f:
            f.write(f"PG_HEADERS_IMAGE={storage['repository']}@{storage['digest']}\n")
    return majors(doc)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=str(HERE.parents[1]), help="repository root")
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("apply")
    a.add_argument("--doc", required=True)
    a.add_argument("--fork-commit", required=True)
    a.add_argument("--env")
    args = ap.parse_args(argv)
    try:
        m = apply(args.root, args.doc, args.fork_commit, args.env)
    except Refused as e:
        print(f"candidate.py: refused: {e}", file=sys.stderr)
        return 1
    print(f"majors={json.dumps(m)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
