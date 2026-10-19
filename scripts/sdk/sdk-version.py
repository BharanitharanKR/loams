#!/usr/bin/env python3
"""The published SDKs' versions: one SemVer, spelled the way each registry wants it.

`.github/workflows/sdk-release.yml` publishes seven SDKs from one release tag,
`vX.Y.Z[-pre.N]`. The tag is the version; every manifest carries the same version
in its ecosystem's spelling, and this script is what proves it. A release whose
manifests disagree with its tag must fail before anything is uploaded, because a
registry never forgets a version once it has one.

    SemVer (the tag)      0.1.0-preview.2
    npm                   0.1.0-preview.2      dist-tag `preview`, never `latest`
    crates.io             0.1.0-preview.2
    NuGet                 0.1.0-preview.2
    PyPI (PEP 440)        0.1.0a2              `preview` and `alpha` map to `a`
    Go                    tag sdks/go/v0.1.0-preview.2 (no manifest version)
    SwiftPM               the release tag itself (no manifest version)
    C++ (CMake)           project(VERSION 0.1.0) + LOAMS_SDK_VERSION "0.1.0-preview.2"
    RubyGems              0.1.0.pre.2          a letter makes it a prerelease
    Packagist (Composer)  tag v0.1.0-alpha.2   on the PHP split repository; Composer
                                               has no `preview` stability, so a
                                               `preview` tag would be ignored
    pub.dev               0.1.0-preview.2
    Maven Central         0.1.0-preview.2      dev.loams:loams, dev.loams:loams-kotlin
    CocoaPods             0.1.0-preview.2      pod `Loams`

Usage:
    sdk-version.py show
    sdk-version.py check [VERSION|vTAG] [--strict-root]
    sdk-version.py set VERSION
    sdk-version.py spell VERSION [--github-output]
    sdk-version.py --self-test

`check` with no version checks that the SDK manifests agree with each other.
`--strict-root` also requires the root Cargo workspace (which publishes
`loams-proto` and `loams-live-proto`, the Rust SDK's dependencies) to be at the
same version; a release tag needs that, a pull request does not.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

SEMVER = re.compile(
    r"^(?P<core>(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*))"
    r"(?:-(?P<pre>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)

# SemVer prerelease label -> PEP 440 prerelease segment.
PEP440_LABELS = {"preview": "a", "alpha": "a", "a": "a", "beta": "b", "b": "b", "rc": "rc"}
# SemVer prerelease label -> RubyGems prerelease segment (`0.1.0.pre.2`).
GEM_LABELS = {"preview": "pre", "alpha": "alpha", "a": "alpha", "beta": "beta", "b": "beta", "rc": "rc"}
# SemVer prerelease label -> Composer stability (alpha, beta, RC are all it knows).
COMPOSER_LABELS = {"preview": "alpha", "alpha": "alpha", "a": "alpha", "beta": "beta", "b": "beta", "rc": "RC"}

# The manifests `set` rewrites and `check` reads. Each entry is
# (label, path, ecosystem, regex with one group around the version).
MANIFESTS = [
    ("typescript @loams/live", "sdks/typescript/packages/live/package.json", "semver",
     r'^  "version": "([^"]+)",$'),
    ("typescript @loams/client", "sdks/typescript/packages/client/package.json", "semver",
     r'^  "version": "([^"]+)",$'),
    ("python loams", "sdks/python/pyproject.toml", "pep440",
     r'^version = "([^"]+)"$'),
    ("python uv.lock", "sdks/python/uv.lock", "pep440",
     r'^name = "loams"\nversion = "([^"]+)"$'),
    ("rust loams", "sdks/rust/Cargo.toml", "semver",
     r'^\[workspace\.package\]\nversion = "([^"]+)"$'),
    ("rust Cargo.lock", "sdks/rust/Cargo.lock", "semver",
     r'^name = "loams"\nversion = "([^"]+)"$'),
    # The Rust SDK's requirement on the two proto crates follows the root
    # workspace's version (ecosystem "root"), so it can be no newer than what the
    # path dependency is; `check --strict-root` then pins both to the release.
    ("rust dep loams-proto", "sdks/rust/Cargo.toml", "root",
     r'^loams-proto = \{ path = "\.\./\.\./crates/loams-proto", version = "([^"]+)"'),
    ("rust dep loams-live-proto", "sdks/rust/Cargo.toml", "root",
     r'^loams-live-proto = \{ path = "\.\./\.\./crates/loams-live-proto", version = "([^"]+)"'),
    # The lock file records the path dependencies' versions too, and
    # `--locked` fails when they drift from the root workspace.
    ("rust Cargo.lock loams-proto", "sdks/rust/Cargo.lock", "root",
     r'^name = "loams-proto"\nversion = "([^"]+)"$'),
    ("rust Cargo.lock loams-live-proto", "sdks/rust/Cargo.lock", "root",
     r'^name = "loams-live-proto"\nversion = "([^"]+)"$'),
    ("csharp Loams", "sdks/csharp/src/Loams/Loams.csproj", "semver",
     r"^    <Version>([^<]+)</Version>$"),
    ("cpp loams (LOAMS_SDK_VERSION)", "sdks/cpp/CMakeLists.txt", "semver",
     r'^set\(LOAMS_SDK_VERSION "([^"]+)"\)$'),
    ("cpp loams (project VERSION)", "sdks/cpp/CMakeLists.txt", "core",
     r"^project\(loams-cpp VERSION (\S+) LANGUAGES CXX\)$"),
    ("ruby loams (gem)", "sdks/ruby/lib/loams/version.rb", "gem",
     r'^  VERSION = "([^"]+)"$'),
    ("dart loams (pub)", "sdks/dart/pubspec.yaml", "semver",
     r"^version: (\S+)$"),
    # The project's own <version>, two spaces in; dependencies' are deeper.
    ("java dev.loams:loams", "sdks/java/pom.xml", "semver",
     r"^  <version>([^<]+)</version>$"),
    ("kotlin dev.loams:loams-kotlin", "sdks/kotlin/pom.xml", "semver",
     r"^  <version>([^<]+)</version>$"),
    ("objc Loams (podspec)", "Loams.podspec", "semver",
     r"^  s\.version += '([^']+)'$"),
]

# Read, never written: the root workspace is not this script's to bump.
ROOT_WORKSPACE = ("root workspace (loams-proto, loams-live-proto)", "Cargo.toml")


class VersionError(Exception):
    pass


def parse(version: str) -> re.Match[str]:
    version = version.removeprefix("v")
    match = SEMVER.match(version)
    if not match:
        raise VersionError(f"{version!r} is not a SemVer version (X.Y.Z or X.Y.Z-label.N)")
    return match


def spell(version: str) -> dict[str, str]:
    """Every ecosystem's spelling of one SemVer version."""
    match = parse(version)
    core, pre = match["core"], match["pre"]
    semver = match.group(0)
    if pre is None:
        pep440, npm_tag, gem, composer = core, "latest", core, core
    else:
        parts = pre.split(".")
        if len(parts) != 2 or parts[0] not in PEP440_LABELS or not parts[1].isdigit():
            raise VersionError(
                f"prerelease {pre!r} has no PEP 440 spelling; use one of "
                f"{sorted(PEP440_LABELS)} followed by .N (for example 0.1.0-preview.2)"
            )
        pep440 = f"{core}{PEP440_LABELS[parts[0]]}{int(parts[1])}"
        gem = f"{core}.{GEM_LABELS[parts[0]]}.{int(parts[1])}"
        composer = f"{core}-{COMPOSER_LABELS[parts[0]]}.{int(parts[1])}"
        # A prerelease is never `latest`: `npm install @loams/client` must not
        # pick a preview up by accident.
        npm_tag = "preview" if parts[0] == "preview" else "next"
    return {
        "semver": semver,
        "core": core,
        "pep440": pep440,
        "npm_tag": npm_tag,
        "prerelease": "true" if pre else "false",
        "go_tag": f"sdks/go/v{semver}",
        "gem": gem,
        # The tag on the PHP split repository, which Packagist reads.
        "composer_tag": f"v{composer}",
        "tag": f"v{semver}",
    }


def expected(spelling: dict[str, str], ecosystem: str, root: str) -> str:
    return {
        "semver": spelling["semver"],
        "pep440": spelling["pep440"],
        "core": spelling["core"],
        "gem": spelling["gem"],
        "root": root,
    }[ecosystem]


def read_all(root: Path) -> list[tuple[str, str, str, str | None]]:
    """(label, path, ecosystem, version or None when the pattern is missing)."""
    found = []
    for label, path, ecosystem, pattern in MANIFESTS:
        text = (root / path).read_text()
        match = re.search(pattern, text, re.MULTILINE)
        found.append((label, path, ecosystem, match.group(1) if match else None))
    return found


def root_version(root: Path) -> str:
    with (root / ROOT_WORKSPACE[1]).open("rb") as handle:
        return tomllib.load(handle)["workspace"]["package"]["version"]


def check(root: Path, version: str | None, strict_root: bool) -> list[str]:
    found = read_all(root)
    problems = []
    for label, path, _, value in found:
        if value is None:
            problems.append(f"{path}: no version found for {label} (the pattern in sdk-version.py is stale)")
    if problems:
        return problems
    if version is None:
        # No tag: the manifests must agree with the TypeScript client's version.
        version = next(value for label, _, _, value in found if label == "typescript @loams/client")
        assert version is not None
    spelling = spell(version)
    have = root_version(root)
    for label, path, ecosystem, value in found:
        want = expected(spelling, ecosystem, have)
        if value != want:
            problems.append(f"{path}: {label} is {value}, the release wants {want}")
    if have != spelling["semver"]:
        message = (
            f"Cargo.toml: {ROOT_WORKSPACE[0]} is {have}, the release wants {spelling['semver']} "
            "(the Rust SDK depends on both crates, so they publish at the release version)"
        )
        if strict_root:
            problems.append(message)
        else:
            print(f"note: {message}", file=sys.stderr)
    return problems


def set_all(root: Path, version: str) -> None:
    spelling = spell(version)
    have = root_version(root)
    for label, path, ecosystem, pattern in MANIFESTS:
        file = root / path
        text = file.read_text()
        regex = re.compile(pattern, re.MULTILINE)
        match = regex.search(text)
        if not match:
            raise VersionError(f"{path}: no version found for {label}")
        start, end = match.span(1)
        text = text[:start] + expected(spelling, ecosystem, have) + text[end:]
        file.write_text(text)
        print(f"{path}: {label} -> {expected(spelling, ecosystem, have)}")
    if have != spelling["semver"]:
        print(
            f"note: the root Cargo workspace is {have}; bump [workspace.package] version to "
            f"{spelling['semver']} in the release PR too (loams-proto, loams-live-proto)",
            file=sys.stderr,
        )


def self_test() -> None:
    cases = {
        "0.1.0-preview.2": ("0.1.0a2", "preview"),
        "v0.1.0-preview.12": ("0.1.0a12", "preview"),
        "1.2.3": ("1.2.3", "latest"),
        "1.0.0-rc.1": ("1.0.0rc1", "next"),
        "1.0.0-beta.3": ("1.0.0b3", "next"),
    }
    for version, (pep440, tag) in cases.items():
        got = spell(version)
        assert (got["pep440"], got["npm_tag"]) == (pep440, tag), (version, got)
    assert spell("0.1.0-preview.2")["go_tag"] == "sdks/go/v0.1.0-preview.2"
    for version, (gem, composer_tag) in {
        "0.1.0-preview.2": ("0.1.0.pre.2", "v0.1.0-alpha.2"),
        "1.0.0-rc.1": ("1.0.0.rc.1", "v1.0.0-RC.1"),
        "1.0.0-beta.3": ("1.0.0.beta.3", "v1.0.0-beta.3"),
        "1.2.3": ("1.2.3", "v1.2.3"),
    }.items():
        got = spell(version)
        assert (got["gem"], got["composer_tag"]) == (gem, composer_tag), (version, got)
    for bad in ("0.1", "01.0.0", "0.1.0-preview", "0.1.0-nightly.1", "0.1.0-preview.x"):
        try:
            spell(bad)
        except VersionError:
            continue
        raise AssertionError(f"{bad} was accepted")
    print("sdk-version: self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT)
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("show")
    p_check = sub.add_parser("check")
    p_check.add_argument("version", nargs="?")
    p_check.add_argument("--strict-root", action="store_true")
    p_set = sub.add_parser("set")
    p_set.add_argument("version")
    p_spell = sub.add_parser("spell")
    p_spell.add_argument("version")
    p_spell.add_argument("--github-output", action="store_true")
    args = parser.parse_args()

    try:
        if args.self_test:
            self_test()
            return 0
        if args.command == "show":
            for label, path, _, value in read_all(args.root):
                print(f"{value or '<missing>':20} {label:34} {path}")
            print(f"{root_version(args.root):20} {ROOT_WORKSPACE[0]:34} {ROOT_WORKSPACE[1]}")
            return 0
        if args.command == "check":
            problems = check(args.root, args.version, args.strict_root)
            for problem in problems:
                print(f"::error::{problem}" if os.environ.get("GITHUB_ACTIONS") else problem)
            if problems:
                print("fix: python3 scripts/sdk/sdk-version.py set <version>, and commit", file=sys.stderr)
                return 1
            print("sdk-version: every SDK manifest is at the release version")
            return 0
        if args.command == "set":
            set_all(args.root, args.version)
            return 0
        if args.command == "spell":
            spelling = spell(args.version)
            if args.github_output:
                with open(os.environ["GITHUB_OUTPUT"], "a") as out:
                    for key, value in spelling.items():
                        out.write(f"{key}={value}\n")
            print(json.dumps(spelling, indent=2))
            return 0
    except VersionError as error:
        print(f"sdk-version: {error}", file=sys.stderr)
        return 1
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
