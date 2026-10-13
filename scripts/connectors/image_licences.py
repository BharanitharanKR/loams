#!/usr/bin/env python3
"""The images Loams builds and the default compose profile carry no CI-only or user-supplied component.

CN2 plan Task 3 (design §33 D359; Owner ruling OR-3; CN1 Ruling 8). `connectors/licences.toml`
has two kinds besides runtime, driver, library and service:

  * `ci-service` — a proprietary or source-available image (Redpanda, MongoDB, Oracle Database
    Free, ...) that runs only in a compose profile named `connectors-*` (never the default profile
    and never the bare `connectors` profile) or in a CI workflow. It may carry a refused id such as
    BUSL-1.1, the D60 precedent, because it is never shipped.
  * `user-supplied` — a driver the operator mounts (Oracle's `ojdbc` and Instant Client). It is
    only ever a documented mount at the path its stanza names.

This script lists each image Loams builds (`loams-fabric`, `loams-connect`, any Debezium wrapper:
the Containerfiles and Dockerfiles under deploy/ whose path names one of them) and the compose
files that carry the default profile (deploy/release/compose.yaml) and the connector stack
(deploy/fabric/compose.yaml, once CN2 Task 6 adds it), and fails on any `ci-service` or
`user-supplied` component found where it may not be.

The same rules are `loams_flow::validate::LicenceGate::{scan_image, scan_compose}`, tested by
fabric/crates/loams-flow/tests/licence_gate.rs; `--self-test` runs this script over the same
fixtures so the two cannot drift.

Usage:
  python3 scripts/connectors/image_licences.py              # the repository
  python3 scripts/connectors/image_licences.py --self-test  # the shared fixtures, then the repository
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LICENCES_PATH = REPO_ROOT / "connectors" / "licences.toml"
FIXTURES = REPO_ROOT / "fabric" / "crates" / "loams-flow" / "tests" / "fixtures" / "licences"

# The images Loams builds and the gate covers (CN2 Task 3), by the name a Containerfile's path
# carries. `loams-fabric` exists today; the other two arrive with CN1 Task 12 and CN2 Task 22.
BUILT_IMAGE_NAMES = ("loams-fabric", "loams-connect", "debezium")
REQUIRED_IMAGES = ("loams-fabric",)
# The compose files the gate reads: the shipped default profile, and the connector stack.
COMPOSE_FILES = ("deploy/release/compose.yaml", "deploy/fabric/compose.yaml")

KINDS = ("runtime", "driver", "library", "service", "ci-service", "user-supplied")
SHIPPED_KINDS = ("runtime", "driver", "library")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
EULA_REF = re.compile(r"^LicenseRef-[A-Za-z0-9.-]+-EULA$")
SPDX_ID = re.compile(r"^[A-Za-z0-9.+-]+$")

CI_SERVICE_RULE = (
    "a ci-service runs only in a connectors-* compose profile or a CI workflow, never in an image "
    "or the default profile (OR-3)"
)
USER_SUPPLIED_RULE = (
    "a user-supplied driver is only ever mounted at its documented path, never shipped in an image "
    "or the default profile (CN1 Ruling 8)"
)


@dataclass
class Component:
    key: str
    spdx: str
    kind: str
    image: str = ""
    digest: str = ""
    reason: str = ""
    profiles: list[str] = field(default_factory=list)
    driver: str = ""
    mount: str = ""


def is_ci_profile(name: str) -> bool:
    """`connectors-<something>`; the bare `connectors` profile is the default connector stack."""
    return name.startswith("connectors-") and len(name) > len("connectors-")


def read_components(path: Path) -> tuple[list[Component], list[str]]:
    """Every [components.*] stanza, with the problems of the two CN2 Task 3 kinds' fields."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    components: list[Component] = []
    problems: list[str] = []
    for key, stanza in data.get("components", {}).items():
        kind = str(stanza.get("kind", ""))
        component = Component(
            key=key,
            spdx=str(stanza.get("spdx", "")),
            kind=kind,
            image=str(stanza.get("image", "")),
            digest=str(stanza.get("digest", "")),
            reason=str(stanza.get("reason", "")),
            profiles=[str(p) for p in stanza.get("profiles", [])],
            driver=str(stanza.get("driver", "")),
            mount=str(stanza.get("mount", "")),
        )
        where = f"{path.name}: components.{key}"
        if kind not in KINDS:
            problems.append(f"{where}: kind {kind!r} is not one of {'|'.join(KINDS)}")
        if kind == "ci-service":
            last = component.image.rsplit("/", 1)[-1]
            if not component.image or "@" in component.image or ":" in last:
                problems.append(f"{where}: image {component.image!r} must be a reference without tag or digest")
            if not DIGEST.match(component.digest):
                problems.append(f"{where}: digest {component.digest!r} is not sha256:<64 hex>")
            if not component.reason:
                problems.append(f"{where}: a ci-service gives the reason it is needed")
            if not (EULA_REF.match(component.spdx) or (SPDX_ID.match(component.spdx) and not component.spdx.startswith("LicenseRef-"))):
                problems.append(f"{where}: spdx {component.spdx!r} is neither an SPDX id nor LicenseRef-<vendor>-EULA")
            if not component.profiles or not all(is_ci_profile(p) for p in component.profiles):
                problems.append(f"{where}: profiles {component.profiles!r} must be non-empty connectors-* names (OR-3)")
        if kind == "user-supplied":
            if not component.driver:
                problems.append(f"{where}: a user-supplied component names its driver file")
            if not component.mount.startswith("/"):
                problems.append(f"{where}: mount {component.mount!r} is not an absolute path")
            if component.image or component.profiles:
                problems.append(f"{where}: a user-supplied driver has no image or profiles")
        components.append(component)
    return components, problems


def normalise_image(word: str) -> str:
    word = word.removeprefix("--from=")
    word = word.removeprefix("docker.io/")
    return word.removeprefix("library/")


def names_image(text: str, reference: str) -> bool:
    reference = normalise_image(reference.lower())
    for word in text.lower().split():
        word = normalise_image(word)
        if word.startswith(reference):
            rest = word[len(reference):]
            if rest == "" or rest[0] in ":@":
                return True
    return False


def names_driver(component: Component, text: str) -> bool:
    stem = re.split(r"[*?\[]", component.driver, maxsplit=1)[0].lower()
    by_name = len(stem) >= 3 and stem in text.lower()
    by_mount = bool(component.mount) and component.mount.lower() in text.lower()
    return by_name or by_mount


def placement_allowed(component: Component, placement: tuple[str, object]) -> bool:
    """Mirrors `LicenceComponent::check_placement`. Placement is (kind, value)."""
    where, value = placement
    if component.kind == "ci-service":
        if where == "workflow":
            return True
        if where == "compose":
            return bool(value) and all(is_ci_profile(p) for p in value)  # type: ignore[union-attr]
        return False
    if component.kind == "user-supplied":
        if where == "mount" and component.mount:
            path = str(value)
            return path == component.mount or path.startswith(component.mount + "/")
        return False
    return True


def describe(placement: tuple[str, object]) -> str:
    where, value = placement
    if where == "image":
        return f"image {value}"
    if where == "compose":
        return f"compose profiles [{', '.join(value)}]" if value else "the default compose profile"  # type: ignore[arg-type]
    if where == "mount":
        return f"mount {value}"
    return f"CI workflow {value}"


def misplaced(origin: str, component: Component, placement: tuple[str, object]) -> str:
    rule = CI_SERVICE_RULE if component.kind == "ci-service" else USER_SUPPLIED_RULE
    return f"{origin}: components.{component.key} ({component.kind}) is in {describe(placement)}: {rule}"


def scan_image(components: list[Component], image: str, text: str) -> list[str]:
    problems: list[str] = []
    placement = ("image", image)
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for component in components:
            if component.kind == "ci-service":
                named = bool(component.image) and names_image(line, component.image)
            elif component.kind == "user-supplied":
                named = names_driver(component, line)
            else:
                named = False
            if named and not placement_allowed(component, placement):
                problems.append(misplaced(f"image {image}", component, placement))
    return list(dict.fromkeys(problems))


def expand_defaults(text: str) -> str:
    """`${NAME:-default}` and `${NAME-default}` as their defaults; a bare `${NAME}` as nothing."""

    def default(match: re.Match[str]) -> str:
        inner = match.group(1)
        if ":-" in inner:
            return inner.split(":-", 1)[1]
        if "-" in inner:
            return inner.split("-", 1)[1]
        return ""

    return re.sub(r"\$\{([^}]*)\}", default, text)


def volume_spec(volume: object) -> str | None:
    if isinstance(volume, str):
        return expand_defaults(volume)
    if isinstance(volume, dict) and volume.get("target"):
        return f"{expand_defaults(str(volume.get('source', '')))}:{volume['target']}"
    return None


def volume_target(volume: str) -> str:
    parts = volume.split(":")
    return parts[1] if len(parts) > 1 else parts[0]


def scan_compose(components: list[Component], origin: str, text: str) -> list[str]:
    document = yaml.safe_load(text) or {}
    problems: list[str] = []
    for name, service in (document.get("services") or {}).items():
        service = service or {}
        profiles = [str(p) for p in service.get("profiles") or []]
        where = f"{origin} service {name}"
        image = expand_defaults(str(service.get("image", ""))).lower()
        volumes = [v for v in (volume_spec(v) for v in service.get("volumes") or []) if v]
        for component in components:
            if component.kind == "ci-service":
                if image and component.image and names_image(image, component.image):
                    placement: tuple[str, object] = ("compose", profiles)
                    if not placement_allowed(component, placement):
                        problems.append(misplaced(where, component, placement))
            elif component.kind == "user-supplied":
                for volume in volumes:
                    target = volume_target(volume)
                    at_mount = bool(component.mount) and (
                        target == component.mount or target.startswith(component.mount + "/")
                    )
                    if not (at_mount or names_driver(component, volume)):
                        continue
                    placement = ("compose", profiles) if not profiles else ("mount", target)
                    if not placement_allowed(component, placement):
                        problems.append(misplaced(where, component, placement))
    return list(dict.fromkeys(problems))


def built_images(root: Path) -> list[tuple[str, Path]]:
    found: list[tuple[str, Path]] = []
    for path in sorted((root / "deploy").rglob("*")):
        if not path.is_file():
            continue
        name = path.name.lower()
        if not (name.startswith("containerfile") or name.startswith("dockerfile")):
            continue
        whole = str(path.relative_to(root)).lower()
        for image in BUILT_IMAGE_NAMES:
            if image in whole:
                found.append((image, path))
                break
    return found


def check_repository(root: Path, extra: list[Component]) -> list[str]:
    components, problems = read_components(root / "connectors" / "licences.toml")
    components = components + extra
    images = built_images(root)
    for required in REQUIRED_IMAGES:
        if not any(name == required for name, _ in images):
            problems.append(f"no Containerfile for the {required} image under deploy/")
    for name, path in images:
        print(f"image-licences: image {name}: {path.relative_to(root)}")
        problems += scan_image(components, name, path.read_text(encoding="utf-8"))
    for relative in COMPOSE_FILES:
        path = root / relative
        if not path.exists():
            print(f"image-licences: {relative} is not there yet; skipped")
            continue
        print(f"image-licences: compose {relative}")
        problems += scan_compose(components, relative, path.read_text(encoding="utf-8"))
    kinds = {c.kind for c in components}
    print(
        "image-licences: "
        f"{sum(c.kind == 'ci-service' for c in components)} ci-service and "
        f"{sum(c.kind == 'user-supplied' for c in components)} user-supplied component(s) checked"
        + ("" if kinds & {"ci-service", "user-supplied"} else " (none listed yet)")
    )
    return problems


def self_test() -> int:
    """The cases tests/licence_gate.rs asserts, over the same fixtures."""
    failures: list[str] = []
    components, problems = read_components(FIXTURES / "licences.toml")
    failures += [f"fixture licences.toml should read cleanly: {p}" for p in problems]

    leaks = scan_image(components, "loams-connect", (FIXTURES / "Containerfile.leaks").read_text(encoding="utf-8"))
    for key in ("oracle-database-free", "ojdbc"):
        if not any(f"components.{key} " in p for p in leaks):
            failures.append(f"Containerfile.leaks: components.{key} should be refused: {leaks}")

    compose = scan_compose(components, "fixture compose.yaml", (FIXTURES / "compose.yaml").read_text(encoding="utf-8"))
    expected = {
        "bad-redpanda-default": "redpanda",
        "bad-redpanda-connectors": "redpanda",
        "bad-ojdbc-default": "ojdbc",
        "bad-ojdbc-elsewhere": "ojdbc",
    }
    for service, key in expected.items():
        if not any(f"service {service}:" in p and f"components.{key} " in p for p in compose):
            failures.append(f"compose fixture: {service} should be refused for {key}: {compose}")
    if any("service ok-" in p for p in compose):
        failures.append(f"compose fixture: an ok-* service was refused: {compose}")
    if len(compose) != len(expected):
        failures.append(f"compose fixture: expected {len(expected)} refusals, got {compose}")

    # The repository, with the fixture's proprietary stanzas added so the scan is not vacuous.
    extra = [c for c in components if c.kind not in SHIPPED_KINDS]
    failures += check_repository(REPO_ROOT, extra)

    for failure in failures:
        print(f"image-licences: self-test FAIL: {failure}")
    print(f"image-licences: self-test {len(failures)} failure(s)")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--self-test", action="store_true", help="run the shared fixtures, then the repository")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    problems = check_repository(REPO_ROOT, [])
    for problem in problems:
        print(f"image-licences: FAIL: {problem}")
    print(f"image-licences: {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
