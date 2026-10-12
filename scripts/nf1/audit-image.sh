#!/usr/bin/env bash
# NF1 Task 1: audit the Rust packages in every platform of an image, by digest.
#
#   audit-image.sh <ref@sha256:digest> > deploy/pins/audit/sha256-<hex>.json
#
# Copies the index, every platform and its layers, once, into an OCI image
# layout (`skopeo copy --all --preserve-digests ... oci:`; not into a local image
# store) under AUDIT_WORKDIR, then runs audit_image.py on each platform: every
# binary with a `.dep-v0` section is read with rust-audit-info.
# Prints {"<platform>:<path>": [{name, version, source}]} on stdout; findings
# go to stderr.
#
# Environment: SKOPEO (a local skopeo; default: the pinned image through podman,
# as mirror-images.sh), REGISTRY_AUTH_FILE, AUDIT_WORKDIR (default: a new
# directory under ${TMPDIR:-/var/tmp}; images are large, keep it off a tmpfs),
# AUDIT_KEEP_LAYOUT (a directory: stage the layout there and keep it, so
# mirror-images.sh can push the same bytes with MIRROR_FROM instead of
# downloading them again),
# AUDIT_LICENCES (crates.io licence cache, kept between runs), AUDIT_OFFLINE=1,
# MIRROR_TLS_VERIFY=false (tests only).
#
# Exit: 0 clean; 3 a forbidden licence (CDDL, `inferno`) or a private
# `neondatabase/subzero` source; 2 usage (a tag instead of a digest); 1 errors.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
SKOPEO_IMAGE=$(sed -n 's/^SKOPEO_IMAGE=//p' "$here/mirror-images.sh")

[[ $# -eq 1 ]] || { echo "usage: $0 <ref@sha256:digest>" >&2; exit 2; }
ref=$1
if [[ ! $ref =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]]; then
  echo "error: image must be pinned by digest (<repo>@sha256:<64 hex>), got: $ref" >&2
  exit 2
fi
repo=${ref%@*}
tls=${MIRROR_TLS_VERIFY:-true}

base=${AUDIT_WORKDIR:-${TMPDIR:-/var/tmp}}
mkdir -p "$base"
work=$(mktemp -d "$base/nf1-audit.XXXXXX")
trap 'rm -rf "$work"' EXIT

layout=$work/layout
if [[ -n ${AUDIT_KEEP_LAYOUT:-} ]]; then
  layout=$(mkdir -p "$AUDIT_KEEP_LAYOUT" && cd "$AUDIT_KEEP_LAYOUT" && pwd)
fi

if [[ -n ${SKOPEO:-} ]]; then
  skopeo_cmd=("$SKOPEO")
else
  skopeo_cmd=(podman run --rm --network host -v "$work:$work:Z")
  [[ $layout == "$work"/* ]] || skopeo_cmd+=(-v "$layout:$layout:Z")
  if [[ -n ${REGISTRY_AUTH_FILE:-} ]]; then
    skopeo_cmd+=(-v "$REGISTRY_AUTH_FILE:/auth.json:ro,Z" -e REGISTRY_AUTH_FILE=/auth.json)
  fi
  skopeo_cmd+=("$SKOPEO_IMAGE")
fi

"${skopeo_cmd[@]}" inspect --raw --retry-times 3 "--tls-verify=$tls" "docker://$ref" >"$work/top.json"
got="sha256:$(sha256sum "$work/top.json" | cut -d' ' -f1)"
[[ $got == "${ref#*@}" ]] || { echo "error: $ref hashes to $got" >&2; exit 1; }

# One line per image platform: <digest> <os/arch[/variant]>. Attestation
# manifests (platform unknown/unknown) carry no binaries and are skipped.
if jq -e '.manifests' "$work/top.json" >/dev/null; then
  plats=$(jq -r '.manifests[] | select(.platform.os != "unknown")
    | "\(.digest) \(.platform.os)/\(.platform.architecture)\(if .platform.variant then "/" + .platform.variant else "" end)"' \
    "$work/top.json")
else
  plats="${ref#*@} single"
fi

echo "audit: copying $ref" >&2
"${skopeo_cmd[@]}" copy --all --preserve-digests --quiet --retry-times 3 "--src-tls-verify=$tls" \
  "docker://$ref" "oci:$layout:${ref#*@sha256:}" >&2

args=()
while read -r digest platform; do
  [[ -n $digest ]] || continue
  args+=("$platform=$layout@$digest")
done <<<"$plats"

py_args=()
[[ ${AUDIT_OFFLINE:-} == 1 ]] && py_args+=(--offline)
[[ -n ${AUDIT_LICENCES:-} ]] && py_args+=(--licences "$AUDIT_LICENCES")

python3 -I "$here/audit_image.py" "${py_args[@]}" "${args[@]}"
