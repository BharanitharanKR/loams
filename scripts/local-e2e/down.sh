#!/usr/bin/env bash
# Stops what the local-e2e scripts started: the desktop app, the console dev
# server, the agentd daemons and the compose stack (its containers; volumes
# and images stay), then the e2e Podman API unit.
#
#   down.sh            stop everything, keep the data
#   down.sh --purge    also delete the stack's volumes, images and $E2E_HOME
#                      (everything local-e2e made; asks first)
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
systemctl --user stop loams-e2e-desktop loams-e2e-console loams-e2e-agentd-desktop loams-e2e-agentd-web 2>/dev/null
if systemctl --user is-active -q loams-e2e-podman; then
  # pg-runner's computes are compose projects of their own (loams-pg-<ns>).
  for p in $(epodman ps -a --format '{{index .Labels "com.docker.compose.project"}}' | sort -u | grep '^loams-pg-'); do
    DOCKER_HOST="unix://$E2E_PODMAN_SOCK" docker-compose -p "$p" down
  done
  if [[ "${1:-}" == --purge ]]; then
    read -r -p "Delete the local-e2e stack's volumes and $E2E_HOME? [y/N] " yes
    [[ "$yes" == y ]] || exit 1
    ecompose --profile '*' down --volumes
  else
    ecompose --profile '*' down
  fi
  systemctl --user stop loams-e2e-podman
fi
# Files in the store belong to subordinate uids: remove them inside the user namespace.
[[ "${1:-}" == --purge ]] && podman unshare rm -rf "$E2E_HOME"
echo stopped
