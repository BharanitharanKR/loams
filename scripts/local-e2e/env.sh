# Sourced by every local-e2e script. Everything the local stack keeps (its own
# Podman store, volumes, downloads, logs) lives under E2E_HOME on /mnt/Projects,
# away from the default Podman store on /home.
export E2E_HOME="${E2E_HOME:-/mnt/Projects/loams-e2e}"
export E2E_DATA="${E2E_DATA:-$E2E_HOME/data}"
export E2E_PODMAN_ROOT="$E2E_HOME/podman/storage"
export E2E_PODMAN_RUNROOT="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/loams-e2e-podman"
export E2E_PODMAN_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/loams-e2e-podman.sock"
export E2E_VERSION="${E2E_VERSION:-0.1.0}"
export E2E_REGISTRY="${E2E_REGISTRY:-localhost/loams-e2e}"
# `epodman ...` is podman on the e2e store; `ecompose ...` is compose on it.
epodman() { podman --root "$E2E_PODMAN_ROOT" --runroot "$E2E_PODMAN_RUNROOT" "$@"; }
# Compose on the e2e store, in the stack directory (docker-compose v2+ CLI;
# `podman compose` hands off to the same binary).
ecompose() { DOCKER_HOST="unix://$E2E_PODMAN_SOCK" docker-compose --project-directory "$E2E_HOME/stack/release" "$@"; }
