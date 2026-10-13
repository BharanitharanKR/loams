#!/usr/bin/env python3
"""The compose services a connector family's suites need in one stack.

CN2 plan Task 6: `conformance/connectors/systems.toml` names, per manifest,
its family crate and the services that stand for its real system;
`deploy/fabric/compose.yaml` puts each service in a profile
(`connectors-p2` for open images, `connectors-ci-only` for `ci-service`
images, OR-3). The CI shards of `.github/workflows/fabric.yml` ask this
script which services to bring up, so the mapping lives in one place:

    compose_services.py --profile connectors-p2 --family brokers
    # -> "activemq artemis mosquitto nats pulsar rabbitmq"

The compose file is read as JSON (`docker compose config --format json`, or
`--compose-json FILE`), so the script needs only the standard library.
Extra targets that are compose services are included (they run after the
real system); Loams modules are not compose services here and are skipped.
An empty answer means the family has nothing to start in that stack.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def compose_services(compose_json: str | None) -> dict:
    if compose_json:
        doc = json.loads(Path(compose_json).read_text())
    else:
        out = subprocess.run(
            [
                "docker", "compose", "-f", str(ROOT / "deploy/fabric/compose.yaml"),
                "--profile", "connectors-p2", "--profile", "connectors-ci-only",
                "config", "--format", "json",
            ],
            check=True, capture_output=True, text=True,
        ).stdout
        doc = json.loads(out)
    return doc.get("services", {})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", required=True, choices=["connectors-p2", "connectors-ci-only"])
    ap.add_argument("--family", required=True)
    ap.add_argument("--compose-json", help="the output of `docker compose config --format json`")
    args = ap.parse_args()

    systems = tomllib.loads((ROOT / "conformance/connectors/systems.toml").read_text())["systems"]
    services = compose_services(args.compose_json)
    wanted: set[str] = set()
    for entry in systems.values():
        if entry["family"] != args.family:
            continue
        for svc in entry.get("services", []) + entry.get("extra_targets", []):
            name = svc.get("compose")
            profiles = services.get(name, {}).get("profiles", []) if name else []
            # A service in both stacks (Azurite) runs in the p2 shard; in the
            # CI-only stack it starts only as another service's dependency.
            if args.profile == "connectors-ci-only" and "connectors-p2" in profiles:
                continue
            if args.profile in profiles:
                wanted.add(name)
    print(" ".join(sorted(wanted)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
