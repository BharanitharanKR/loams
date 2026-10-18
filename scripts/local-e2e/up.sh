#!/usr/bin/env bash
# Starts the local-e2e stack: the release compose bundle from this checkout,
# copied to $E2E_HOME/stack (as byoc/install.sh copies it to a VM), with
# compose.override.yaml from this directory, on the local-e2e Podman store.
#
#   scripts/local-e2e/up.sh            # house, postgres-control and console profiles
#   scripts/local-e2e/up.sh --no-pg    # without the Postgres control plane
#   scripts/local-e2e/up.sh infra      # only the stack directory and the services
#                                      # that need no Loams image (pd, tikv, tidb,
#                                      # rustfs, the Neon storage)
#
# Idempotent: an existing .env, secrets/ and volumes are kept.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
profiles=(--profile house --profile postgres-control --profile console)
mode="${1:-all}"
[[ "$mode" == "--no-pg" ]] && profiles=(--profile house --profile console)

# 1. The Podman API on the e2e store (a transient user unit).
if ! systemctl --user is-active -q loams-e2e-podman; then
  mkdir -p "$E2E_PODMAN_ROOT"
  systemd-run --user --unit=loams-e2e-podman --description='Loams local-e2e Podman API' \
    podman --root "$E2E_PODMAN_ROOT" --runroot "$E2E_PODMAN_RUNROOT" system service --time=0 \
    "unix://$E2E_PODMAN_SOCK"
  for _ in $(seq 50); do [[ -S "$E2E_PODMAN_SOCK" ]] && break; sleep 0.2; done
fi
# `podman system service` makes the socket 0600; pg-runner (uid 10001, group 0
# = your user inside rootless Podman, CONTAINER_SOCKET_GID=0) needs group
# access, as podman.socket's SocketMode=0660 gives it.
chmod 0660 "$E2E_PODMAN_SOCK"
export DOCKER_HOST="unix://$E2E_PODMAN_SOCK"

# 2. The images (build-binaries.sh, then build-images.sh, when missing).
if [[ "$mode" != infra ]]; then
  for image in loams loams-fabric loams-agentd loams-pg-control; do
    if ! epodman image exists "$E2E_REGISTRY/$image:$E2E_VERSION"; then
      [[ -x "$E2E_HOME/bin/$image" || -x "$E2E_HOME/bin/loams-engine" ]] || "$here/build-binaries.sh"
      "$here/build-images.sh" "$image"
    fi
  done
fi

# 3. The stack directory: this checkout's bundle, plus the override.
stack="$E2E_HOME/stack"
mkdir -p "$stack"
rsync -a --delete --exclude .env --exclude secrets/ --exclude compose.override.yaml \
  "$root/deploy/release/" "$stack/release/"
rsync -a --delete "$root/deploy/loams-postgres-dev/" "$stack/loams-postgres-dev/"
cp "$here/compose.override.yaml" "$stack/release/compose.override.yaml"
mkdir -p "$stack/release/secrets"
cd "$stack/release"
if [[ ! -f .env ]]; then
  cp .env.example .env
  cat >> .env <<ENV

# --- local-e2e (scripts/local-e2e/up.sh) ---------------------------------------
LOAMS_REGISTRY=$E2E_REGISTRY
LOAMS_VERSION=$E2E_VERSION
LOAMS_AUTH_MODE=off
LOAMS_CORS_ORIGINS=http://127.0.0.1:5190,http://localhost:5190,http://127.0.0.1:5192,http://localhost:5192,http://127.0.0.1:3000,http://localhost:3000
# RustFS (from the Postgres stack), with AWS_* in secrets/engine.env (B1, fixed by PR #755).
LOAMS_BUCKET=s3://loams-engine/stack
LOAMS_HOUSE_ENDPOINT=https://house:8443
HOUSE_PRINCIPALS=dev-loopback=read+write+ddl+admin
CONTAINER_SOCKET=$E2E_PODMAN_SOCK
CONTAINER_SOCKET_GID=0
# The compose console (--profile console) on http://127.0.0.1:3000, from the console build.
LOAMS_ENGINE_URL=http://127.0.0.1:18080
LOAMS_CONSOLE_DIR=$root/web/apps/console/dist
ENV
fi
# A stack made before B1 was fixed kept its bucket on the loams-data volume.
if grep -q '^# s3://loams-engine/stack does not work' .env; then
  sed -i 's|^# s3://loams-engine/stack does not work.*|# RustFS (from the Postgres stack), with AWS_* in secrets/engine.env (B1, fixed by PR #755).|' .env
  python3 -I - .env <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().splitlines(keepends=True)
i = max(n for n, l in enumerate(lines) if l.startswith("LOAMS_BUCKET="))
lines[i] = "LOAMS_BUCKET=s3://loams-engine/stack\n"
open(p, "w").write("".join(lines))
PY
  echo "up.sh: the engine's bucket is now s3://loams-engine/stack (re-seed: data on the old bucket is not moved)"
fi
sed -i 's/^AWS_ENDPOINT=/AWS_ENDPOINT_URL=/' secrets/engine.env 2>/dev/null || true
if [[ ! -f secrets/engine.env ]]; then
  cat > secrets/engine.env <<ENV
# The engine's S3 settings for LOAMS_BUCKET (RustFS from the Postgres stack).
AWS_ACCESS_KEY_ID=rustfsadmin
AWS_SECRET_ACCESS_KEY=rustfsadmin
AWS_REGION=us-east-1
AWS_ENDPOINT_URL=http://rustfs:9000
AWS_ALLOW_HTTP=true
ENV
  chmod 0644 secrets/engine.env
fi

if [[ "$mode" == infra ]]; then
  ecompose up -d pd tikv tidb rustfs e2e-bucket create-bucket storage_broker pageserver safekeeper1
  ecompose ps
  exit 0
fi

# 4. pg-control's API key (README "Postgres control plane"), once.
if [[ ! -s secrets/pg-api-keys.json ]]; then
  echo '{"keys": []}' > secrets/pg-api-keys.json
  out="$(ecompose run --rm --no-deps -T pg-control pg-api-key \
          --subject e2e@localhost --grant admin:namespace:default)"
  python3 -I -c 'import json,sys; d=json.loads(sys.argv[1]); print(json.dumps({"keys":[d["entry"]]}))' "$out" \
    > secrets/pg-api-keys.json
  python3 -I -c 'import json,sys; print(json.loads(sys.argv[1])["api_key"])' "$out" > "$E2E_HOME/pg-api-key"
  chmod 0644 secrets/pg-api-keys.json; chmod 0600 "$E2E_HOME/pg-api-key"
fi

# 5. Up.
ecompose "${profiles[@]}" up -d
ecompose "${profiles[@]}" ps
