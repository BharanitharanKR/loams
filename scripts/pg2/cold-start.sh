#!/usr/bin/env bash
# PG2 Task 17: cold start, suspended endpoint -> StartEndpoint -> first
# SELECT 1 through the endpoint's address, RUNS times (50), on compose.
#
#   scripts/pg2/cold-start.sh            # writes bench/results/<date>-<sha>-coldstart.json
#
# It brings up deploy/loams-postgres-dev's storage (rustfs, broker,
# pageserver, safekeeper1) as its own compose project (COMPOSE_PROJECT,
# default pg2-coldstart), runs `it_cold_start_samples` (crates/loams-pg-control/
# tests/it_endpoint.rs: the API service, the endpoint reconciler and the
# compose runtime on real computes), and summarizes the samples with
# coldstart_stats.py (p50, p90, p99). The stack is torn down on exit
# (KEEP=1 keeps it).
#
# Variants: compose with the warm pool off. Compose has no warm pool (R14.2);
# Kubernetes (kind, pool on and off) needs storage in the cluster, which
# Task 48's chart brings, and the measurement through PgDog is Task 26's.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
RUNS=${RUNS:-50}
PROJECT=${COMPOSE_PROJECT:-pg2-coldstart}
OUT_DIR=${OUT_DIR:-$ROOT/bench/results}
log() { printf '%s cold-start: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

case "$RUNS" in '' | *[!0-9]*) fail "RUNS must be a number" ;; esac

if [ -z "${DOCKER_HOST:-}" ] && [ -S "/run/user/$(id -u)/podman/podman.sock" ]; then
  DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
  export DOCKER_HOST
fi
COMPOSE=(docker-compose)
command -v docker-compose >/dev/null || COMPOSE=(docker compose)
# The computes reach the upcall listener on the host.
case ${DOCKER_HOST:-} in
  *podman*) UPCALL_HOST=host.containers.internal ;;
  *) UPCALL_HOST=host.docker.internal; export IT_EXTRA_HOSTS=${IT_EXTRA_HOSTS:-host.docker.internal:host-gateway} ;;
esac

# The storage stack publishes these on the host.
for port in 9000 9898 7676; do
  if ss -ltn | awk '{print $4}' | grep -qE "[:.]$port\$"; then
    fail "port $port is in use (another storage stack?)"
  fi
done

work=$(mktemp -d "${TMPDIR:-/tmp}/loams-coldstart.XXXXXX")
teardown() {
  if [ "${KEEP:-0}" != 1 ]; then
    (cd "$ROOT/deploy/loams-postgres-dev" && "${COMPOSE[@]}" -p "$PROJECT" down -v >/dev/null 2>&1) || true
  fi
  rm -rf "$work"
}
trap teardown EXIT

log "storage stack $PROJECT"
(cd "$ROOT/deploy/loams-postgres-dev" &&
  "${COMPOSE[@]}" -p "$PROJECT" up -d rustfs create-bucket storage_broker pageserver safekeeper1) >&2
for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:9898/v1/status >/dev/null && break
  sleep 1
done
curl -sf http://127.0.0.1:9898/v1/status >/dev/null || fail "the pageserver did not come up"

log "$RUNS cold starts (compose, pool off)"
(cd "$ROOT" &&
  COLDSTART_RUNS=$RUNS COLDSTART_OUT=$work/compose-off.json \
  IT_COMPOSE_NETWORK=${PROJECT}_default IT_UPCALL_HOST=$UPCALL_HOST \
  cargo test -p loams-pg-control --locked --test it_endpoint -- --ignored --exact it_cold_start_samples)

sha=$(git -C "$ROOT" rev-parse --short=12 HEAD)
date=$(date -u +%Y%m%d)
mkdir -p "$OUT_DIR"
out="$OUT_DIR/$date-$sha-coldstart.json"
python3 "$ROOT/scripts/pg2/coldstart_stats.py" --sha "$sha" --date "$date" --host "$(uname -sm), $(nproc) CPUs" \
  "$work/compose-off.json" >"$out"
log "wrote $out"
python3 - "$out" <<'PY'
import json
import sys

for r in json.load(open(sys.argv[1]))["runs"]:
    print(f"{r['runtime']}/pool {r['pool']}: n={r['n']} p50={r['p50_ms']} p90={r['p90_ms']} "
          f"p99={r['p99_ms']} ms (target met: {r.get('meets_target')})")
PY
