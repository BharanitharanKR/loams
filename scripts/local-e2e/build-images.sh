#!/usr/bin/env bash
# Packages the binaries from build-binaries.sh into the four release images,
# tagged $E2E_REGISTRY/<image>:$E2E_VERSION (localhost/loams-e2e/...:0.1.0),
# in the local-e2e Podman store. linux/amd64 only.
#
#   scripts/local-e2e/build-images.sh [loams|loams-fabric|loams-agentd|loams-pg-control ...]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
revision="$(git -C "$root" rev-parse HEAD)"
images=("$@"); ((${#images[@]})) || images=(loams loams-fabric loams-agentd loams-pg-control)
for image in "${images[@]}"; do
  ref="$E2E_REGISTRY/$image:$E2E_VERSION"
  echo "build-images: $ref"
  epodman build --format docker --platform linux/amd64 \
    --build-arg "VERSION=$E2E_VERSION" --build-arg "REVISION=$revision" \
    -t "$ref" -f "$here/images/Containerfile.$image" "$E2E_HOME/bin"
done
epodman images --filter "reference=$E2E_REGISTRY/*"
