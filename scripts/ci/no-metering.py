#!/usr/bin/env python3
"""Enforce D552 without printing file contents or scanning untracked secrets."""
from fnmatch import fnmatchcase
from pathlib import Path
import re
import subprocess
import sys
import tomllib


# Compose these names so the guard also checks its own source and fixtures.
MARKERS = (b"loams." + b"meter", b"meter." + b"sock", b"Host" + b"Report",
           b"x-loams-" + b"usage", b"loams_" + b"meter_")
ALLOWLIST = ("docs/design/13-decision-log.md", "docs/design/_pending/*.md",
             "docs/design/27-usage-hooks.md", "docs/design/41-*.md",
             "docs/open-core.md", "docs/plans/2026-10-0*-rn1-*.md",
             "docs/plans/2026-10-02-mt4-*.md", "CHANGELOG.md")
PREFIX = "loam-platform"

# PG2 (D548, D550; PG2 Task 9): in Loams Postgres's control plane and its
# chart, no field, metric, table or endpoint is named for billing. Names are
# read from code and strings with the comments taken out, so prose may still
# say "the plan".
NAME_SCOPES = ("crates/loams-pg-control/", "deploy/helm/loams-postgres/")
NAME_SUFFIXES = (".rs", ".toml", ".yaml", ".yml", ".json", ".tpl", ".sql", ".proto")
BILLING_WORDS = frozenset(w + s for w in ("plan", "price", "invoice", "credit", "meter",
                                           "billable", "usage") for s in ("", "s"))
# Names of other crates that only look like billing words.
ALLOWED_NAMES = frozenset({
    "FaultPlan",  # loams-kv's fault injection
    # X.509 key usage (PG2 Task 10's compute certificates): rcgen's and
    # x509-parser's own names, not billing.
    "KeyUsagePurpose", "ExtendedKeyUsagePurpose", "key_usages", "extended_key_usages",
    "extended_key_usage",
    # A Postgres GUC name (PG2 Task 11's settable parameters).
    "plan_cache_mode",
})
TOKEN = re.compile(r"[A-Za-z0-9_]+")
CAMEL = re.compile(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|[0-9]+")


def strip_rust_comments(text):
    """`text` with `//` and `/* */` comments blanked (newlines kept), strings kept."""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    if text[i] == "\n":
                        out.append("\n")
                    i += 1
        elif c == "r" and re.match(r'r#*"', text[i:]):
            hashes = len(re.match(r'r(#*)"', text[i:]).group(1))
            end = text.find('"' + "#" * hashes, i + 2 + hashes)
            end = n if end < 0 else end + 1 + hashes
            out.append(text[i:end])
            i = end
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            out.append(text[i:j + 1])
            i = j + 1
        elif c == "'" and (text.startswith("'\\", i) or (i + 2 < n and text[i + 2] == "'")):
            # A char literal ('"', '\''); anything else is a lifetime.
            j = text.find("'", i + 2 if text[i + 1] != "\\" else i + 3)
            j = n if j < 0 else j
            out.append(text[i:j + 1])
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def strip_hash_comments(text):
    """`text` with `#` comments (outside quotes) blanked, newlines kept."""
    lines = []
    for line in text.split("\n"):
        quote = None
        for k, c in enumerate(line):
            if quote:
                quote = None if c == quote else quote
            elif c in "'\"":
                quote = c
            elif c == "#" and (k == 0 or line[k - 1] in " \t"):
                line = line[:k]
                break
        lines.append(line)
    return "\n".join(lines)


def billing_names(name, data):
    """(line, word) for every name in `data` with a billing word in it."""
    try:
        text = data.decode()
    except UnicodeError:
        return [(1, "undecodable file")]
    if name.endswith(".rs"):
        text = strip_rust_comments(text)
    elif not name.endswith(".json"):
        text = strip_hash_comments(text)
    found = []
    for match in TOKEN.finditer(text):
        if match.group() in ALLOWED_NAMES:
            continue
        for part in match.group().split("_"):
            for word in CAMEL.findall(part):
                if word.lower() in BILLING_WORDS:
                    found.append((text.count("\n", 0, match.start()) + 1, word.lower()))
    return found


def private_package(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key.startswith(PREFIX):
                return True
            if key in ("name", "package") and isinstance(child, str) and child.startswith(PREFIX):
                return True
            if private_package(child):
                return True
    elif isinstance(value, list):
        return any(private_package(child) for child in value)
    return False


def main():
    root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip())
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).split(b"\0")
    failed = False
    for raw_name in filter(None, names):
        name = raw_name.decode(errors="surrogateescape")
        path = root / name
        if not path.exists() and not path.is_symlink():
            # A tracked deletion is not part of the candidate tree.
            continue
        if path.is_symlink() and path.name in ("Cargo.toml", "Cargo.lock"):
            print(f"{name}: symlinked Cargo manifest/lockfile is forbidden", file=sys.stderr)
            failed = True
            continue
        if path.is_symlink():
            # Scan the committed symlink target, never the file it points to.
            data = path.readlink().as_posix().encode(errors="surrogateescape")
        else:
            data = path.read_bytes()
        if path.name in ("Cargo.toml", "Cargo.lock"):
            try:
                document = tomllib.loads(data.decode())
            except (UnicodeError, tomllib.TOMLDecodeError):
                print(f"{name}: cannot parse Cargo manifest/lockfile", file=sys.stderr)
                failed = True
            else:
                if private_package(document):
                    print(f"{name}: private platform package is forbidden", file=sys.stderr)
                    failed = True
        if any(len(name.split("/")) == len(allowed.split("/"))
               and all(fnmatchcase(part, pattern) for part, pattern in
                       zip(name.split("/"), allowed.split("/")))
               for allowed in ALLOWLIST):
            continue
        if name.startswith(NAME_SCOPES) and name.endswith(NAME_SUFFIXES):
            for line, word in billing_names(name, data):
                print(f"{name}:{line}: billing name {word!r} (D548, D550; PG2 Task 9)", file=sys.stderr)
                failed = True
        for marker in MARKERS:
            start = data.find(marker)
            if start >= 0:
                line = data[:start].count(b"\n") + 1
                print(f"{name}:{line}: forbidden billing-grade name {marker.decode()}", file=sys.stderr)
                failed = True
    if failed:
        return 1
    print("no-metering: tracked files and Cargo packages satisfy the open-core boundary")
    return 0


if __name__ == "__main__":
    sys.exit(main())
