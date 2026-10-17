#!/usr/bin/env bash
# Builds the four release images' binaries the way their Containerfiles'
# build stages do (rust:1.97.1-slim-trixie, `cargo build --locked --release`
# with the same packages and features), into $E2E_HOME/bin.
#
# Why not deploy/release/build.sh: it keeps one podman cache mount per image
# (four ~10 GB targets) in the container store, and this machine has no room
# for that. Why not the host's cargo: a host build links against the host's
# glibc (2.44 here), whose libm symbols trixie's 2.41 lacks, so it cannot run
# in the release runtime image.
#
#   scripts/local-e2e/build-binaries.sh [engine|pg-control|agentd|fabric ...]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
jobs="${E2E_JOBS:-5}"
builder="$E2E_REGISTRY/builder:rust-1.97.1-trixie"
mkdir -p "$E2E_HOME/bin" "$E2E_HOME/cargo-target" "$E2E_HOME/cargo-target-fabric" "$E2E_HOME/cargo-registry" \
  "$root/target" "$root/fabric/target"
epodman image exists "$builder" || epodman build -t "$builder" -f "$here/images/Containerfile.builder" "$here/images"
what=("$@"); ((${#what[@]})) || what=(engine pg-control agentd fabric)

run() {
  epodman run --rm --name "loams-e2e-build-$1" --network host \
    -v "$root:/src:ro" -v "$E2E_HOME/cargo-target:/src/target" \
    -v "$E2E_HOME/cargo-target-fabric:/src/fabric/target" \
    -v "$E2E_HOME/cargo-registry:/usr/local/cargo/registry" \
    -v "$E2E_HOME/bin:/out" -e CARGO_BUILD_JOBS="$jobs" "$builder" bash -ec "$2"
}

for w in "${what[@]}"; do
  echo "== $w ($(date -Is))"
  case "$w" in
    engine) run engine 'cargo build --locked --release -p loams --bin loams \
        --features live,live-tikv,durable,graph,tikv,pgwire,mysql-wire,stream-grpc
      install -m 0755 target/release/loams /out/loams-engine' ;;
    pg-control) run pg-control 'cargo build --locked --release -p loams --bin loams --features postgres,tikv
      install -m 0755 target/release/loams /out/loams-pg-control' ;;
    agentd) run agentd 'cargo build --locked --release -p loams-agentd --bin loams-agentd
      install -m 0755 target/release/loams-agentd /out/loams-agentd' ;;
    fabric) run fabric 'cd fabric
      cargo build --locked --release -p loams-fabric --bin loams-fabric
      cargo build --locked --release -p loams-house-worker --bin loams-house-worker
      install -m 0755 target/release/loams-fabric target/release/loams-house-worker /out/
      lib="$(ls -t target/release/build/loams-chdb-sys-*/out/libchdb/libchdb.so | head -1)"
      install -m 0644 "$lib" /out/libchdb.so' ;;
    *) echo "unknown: $w" >&2; exit 2 ;;
  esac
done
echo "== done ($(date -Is))"; ls -la "$E2E_HOME/bin"
