#!/usr/bin/env python3
"""Check and list Loams Flow's connector profiles (CN2 plan Task 7).

Sources of truth:

  connectors/schema/profile.schema.json   the profile shape, the family modules and
                                          the target / http-api rule (R-CN2-2)
  connectors/profiles/<id>.yaml           one target or HTTP-API profile each
  connectors/registry/<id>.yaml           the manifests whose runtime.profile names one

Modes:

  --check   (default) check every profile and every manifest's runtime.profile, print
            every problem and exit non-zero on any. The schema is applied with the
            `jsonschema` module when it is installed; the rules below run either way.
  --list    print one line per profile: id, kind, module, the real system and its service.

The rules mirror `fabric/crates/loams-flow/src/profile.rs`, which is the authority at
run time:

  * the id is the file name;
  * a target's module is a family module and it has no `http` block; an http-api
    profile's module is `http` and it has one;
  * no key anywhere names a credential (`token_url` is a URL), no fixed header
    carries one, and `http.base_url` has no user information (Review Focus 8);
  * every `narrows` path names a capability, and a manifest that uses the profile
    explains each with a note that starts `narrows: <path>`;
  * a manifest's runtime.profile names an existing profile.

A refusal names the key, never the value.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - the CI image has PyYAML
    print("profiles: PyYAML is required", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
CONNECTORS = ROOT / "connectors"
SCHEMA = CONNECTORS / "schema" / "profile.schema.json"
PROFILES = CONNECTORS / "profiles"
REGISTRY = CONNECTORS / "registry"

SECRET_KEY_PARTS = (
    "password", "passwd", "secret", "token", "api_key", "apikey", "private_key",
    "access_key", "credential", "authorization", "cookie", "session",
)
NOT_SECRET_KEYS = {"token_url"}
SOURCE_FLAGS = {"streaming", "batch", "cdc", "webhook", "resumable"}
SINK_FLAGS = {"streaming", "batch", "transactional", "upsert", "delete", "idempotent", "reply"}


def is_secret_key(key: str) -> bool:
    k = key.lower().replace("-", "_")
    return k not in NOT_SECRET_KEYS and any(p in k for p in SECRET_KEY_PARTS)


def secret_problems(value, at: str = "") -> list[str]:
    problems: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            here = f"{at}.{key}" if at else str(key)
            if at != "http.resources" and is_secret_key(str(key)):
                problems.append(f"{here}: a profile never holds a credential (Review Focus 8)")
                continue
            problems.extend(secret_problems(child, here))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            problems.extend(secret_problems(child, f"{at}.{i}"))
    return problems


def narrow_problem(path: str, formats: set[str]) -> str | None:
    head, _, tail = path.partition(".")
    ok = (
        (head in ("source", "sink") and not tail)
        or (head == "source" and tail in SOURCE_FLAGS)
        or (head == "sink" and tail in SINK_FLAGS)
        or (head == "bulk" and tail == "arrow")
        or (head == "formats" and tail in formats)
    )
    return None if ok else f"narrows: {path!r} names no capability"


def check_profile(path: Path, schema: dict, validator, formats: set[str]) -> tuple[dict | None, list[str]]:
    try:
        doc = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as error:
        return None, [f"cannot read: {error}"]
    if not isinstance(doc, dict):
        return None, ["not a YAML mapping"]
    problems: list[str] = []
    if validator is not None:
        for error in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path)):
            where = ".".join(str(p) for p in error.absolute_path) or "profile"
            # The validator's message may quote the value; report the keyword only.
            problems.append(f"{where}: fails the schema's {error.validator!r}")
    modules = schema["properties"]["module"]["enum"]
    for key in ("apiVersion", "kind", "id", "profile_kind", "module", "system"):
        if key not in doc:
            problems.append(f"{key}: required")
    if doc.get("id") != path.stem:
        problems.append(f"id: {doc.get('id')!r} is not the file name ({path.name})")
    kind, module = doc.get("profile_kind"), doc.get("module")
    if kind not in ("target", "http-api"):
        problems.append(f"profile_kind: {kind!r} is not target or http-api")
    if module not in modules:
        problems.append(f"module: {module!r} is not a family module")
    if kind == "http-api" and (module != "http" or "http" not in doc):
        problems.append("module: an http-api profile runs on `http` and has an `http` block")
    if kind == "target" and "http" in doc:
        problems.append("http: only an http-api profile has an `http` block")
    problems.extend(secret_problems(doc))
    base_url = ((doc.get("http") or {}).get("base_url") or "")
    if "@" in base_url.split("://", 1)[-1].split("/", 1)[0]:
        problems.append("http.base_url: a URL with user information holds a credential")
    for narrowed in doc.get("narrows") or []:
        problem = narrow_problem(str(narrowed), formats)
        if problem:
            problems.append(problem)
    return doc, problems


def manifest_formats() -> set[str]:
    manifest_schema = json.loads((CONNECTORS / "schema" / "connector.schema.json").read_text())
    found: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            if "enum" in node and "cloudevents-binary" in node["enum"]:
                found.update(node["enum"])
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(manifest_schema)
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="check (the default)")
    parser.add_argument("--list", action="store_true", help="list the profiles")
    args = parser.parse_args()

    schema = json.loads(SCHEMA.read_text())
    try:
        import jsonschema  # type: ignore

        validator = jsonschema.Draft202012Validator(schema)
    except ImportError:
        validator = None
        if not args.list:
            print("profiles: the `jsonschema` module is not installed; rules-only check")
    formats = manifest_formats()

    profiles: dict[str, dict] = {}
    failures = 0
    for path in sorted(PROFILES.glob("*.yaml")):
        doc, problems = check_profile(path, schema, validator, formats)
        for problem in problems:
            print(f"{path.relative_to(ROOT)}: {problem}")
        failures += len(problems)
        if doc is not None and not problems:
            profiles[doc["id"]] = doc

    if args.list:
        for pid, doc in profiles.items():
            system = doc["system"]
            print(f"{pid}\t{doc['profile_kind']}\t{doc['module']}\t{system['name']}\t{json.dumps(system['service'])}")
        return 1 if failures else 0

    for path in sorted(REGISTRY.glob("*.yaml")):
        manifest = yaml.safe_load(path.read_text()) or {}
        pid = (manifest.get("runtime") or {}).get("profile")
        if not pid:
            continue
        where = path.relative_to(ROOT)
        if pid not in profiles:
            print(f"{where}: runtime.profile {pid!r} is not a valid profile under connectors/profiles/")
            failures += 1
            continue
        notes = manifest.get("notes") or []
        for narrowed in profiles[pid].get("narrows") or []:
            prefix = f"narrows: {narrowed}"
            if not any(re.match(re.escape(prefix) + r"($|[\s:,;])", n) for n in notes):
                print(f"{where}: the profile {pid!r} narrows {narrowed} and no `{prefix} …` note says why")
                failures += 1

    if failures:
        print(f"profiles: {failures} problem(s)")
        return 1
    print(f"profiles: {len(profiles)} profile(s) ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
