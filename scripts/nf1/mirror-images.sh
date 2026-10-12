#!/usr/bin/env bash
# NF1 Task 1: copy an image, by digest, from one registry to another, then prove
# the copy is byte-identical.
#
#   mirror-images.sh [--dry-run | --verify-only] <src-ref@sha256:digest> <dst-repo> <tag>
#
# Copies with `skopeo copy --all --preserve-digests` (every platform, attestation
# manifests included), then compares `skopeo inspect --raw` of the source and the
# destination: the index, and every child manifest it lists, must be byte-identical
# and hash to its digest. Nothing is pulled into a local image store.
#
# skopeo runs from a pinned image through podman, unless SKOPEO names a local
# binary (SKOPEO=skopeo). Credentials come from REGISTRY_AUTH_FILE.
# MIRROR_TLS_VERIFY=false allows a plain-HTTP registry (tests only).
# MIRROR_FROM=oci:<layout>:<ref> copies from a local OCI layout that
# `audit-image.sh` staged (AUDIT_KEEP_LAYOUT) instead of downloading the source
# again; the verification still compares the destination with <src-ref@digest>.
#
# Exit: 0 copied and identical; 1 copy failed or a digest differs; 2 usage
# (including a source given by tag instead of digest).
set -euo pipefail

# quay.io/skopeo/stable:v1.21.0 (2026-03-06), multi-arch index.
SKOPEO_IMAGE=quay.io/skopeo/stable@sha256:a585e4a3b8a045baa87c7f1b2f940d6d299ebede85ab3f2419d52d2264eefc93

usage() {
  echo "usage: $0 [--dry-run | --verify-only] <src-ref@sha256:digest> <dst-repo> <tag>" >&2
  exit 2
}

mode=copy
case "${1:-}" in
  --dry-run) mode=dry; shift ;;
  --verify-only) mode=verify; shift ;;
esac
[[ $# -eq 3 ]] || usage
src=$1 dst_repo=$2 tag=$3

if [[ ! $src =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]]; then
  echo "error: source must be pinned by digest (<repo>@sha256:<64 hex>), got: $src" >&2
  exit 2
fi
src_repo=${src%@*}
src_digest=${src#*@}
if [[ $dst_repo == *@* || ${dst_repo##*/} == *:* || -z $dst_repo ]]; then
  echo "error: destination must be a repository without tag or digest, got: $dst_repo" >&2
  exit 2
fi
if [[ ! $tag =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "error: invalid tag: $tag" >&2
  exit 2
fi
dst="$dst_repo:$tag"

if [[ -n ${MIRROR_FROM:-} && ! ${MIRROR_FROM} =~ ^oci:/[^:]+:[^:]+$ ]]; then
  echo "error: MIRROR_FROM must be oci:<absolute layout path>:<ref>, got: $MIRROR_FROM" >&2
  exit 2
fi

tls=${MIRROR_TLS_VERIFY:-true}
[[ $tls == true || $tls == false ]] || { echo "error: MIRROR_TLS_VERIFY must be true or false" >&2; exit 2; }

if [[ -n ${SKOPEO:-} ]]; then
  skopeo_cmd=("$SKOPEO")
else
  skopeo_cmd=(podman run --rm --network host)
  if [[ -n ${MIRROR_FROM:-} ]]; then
    from_dir=${MIRROR_FROM#oci:}
    from_dir=${from_dir%:*}
    skopeo_cmd+=(-v "$from_dir:$from_dir:ro,Z")
  fi
  if [[ -n ${REGISTRY_AUTH_FILE:-} ]]; then
    skopeo_cmd+=(-v "$REGISTRY_AUTH_FILE:/auth.json:ro,Z" -e REGISTRY_AUTH_FILE=/auth.json)
  fi
  skopeo_cmd+=("$SKOPEO_IMAGE")
fi

copy_args=(copy --all --preserve-digests --retry-times 3
  "--src-tls-verify=$tls" "--dest-tls-verify=$tls"
  "${MIRROR_FROM:-docker://$src}" "docker://$dst")

if [[ $mode == dry ]]; then
  echo "${skopeo_cmd[*]} ${copy_args[*]}"
  exit 0
fi

if [[ $mode == copy ]]; then
  echo "copying $src -> $dst" >&2
  "${skopeo_cmd[@]}" "${copy_args[@]}" >&2 || { echo "error: copy failed" >&2; exit 1; }
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# raw <ref> <file>: the manifest bytes exactly as the registry serves them.
raw() {
  "${skopeo_cmd[@]}" inspect --raw --retry-times 3 "--tls-verify=$tls" "docker://$1" >"$2"
}
digest_of() { echo "sha256:$(sha256sum "$1" | cut -d' ' -f1)"; }

fail=0
# same <label> <expected digest> <src ref> <dst ref>
same() {
  local label=$1 want=$2 a=$work/a.json b=$work/b.json
  raw "$3" "$a" || { echo "error: cannot read $3" >&2; fail=1; return 1; }
  raw "$4" "$b" || { echo "error: cannot read $4" >&2; fail=1; return 1; }
  local da db
  da=$(digest_of "$a") db=$(digest_of "$b")
  if [[ $da != "$want" ]]; then
    echo "error: $label: source $3 hashes to $da, expected $want" >&2
    fail=1
  elif ! cmp -s "$a" "$b"; then
    echo "error: $label: destination $4 differs from source (got $db, want $want)" >&2
    fail=1
  else
    return 0
  fi
  return 1
}

if same index "$src_digest" "$src" "$dst"; then
  echo "index $src_digest identical"
  cp "$work/a.json" "$work/index.json"
  # A single-platform manifest has no children; an index lists them.
  children=$(jq -r '.manifests[]? | [.digest,
      ((.platform.os // "?") + "/" + (.platform.architecture // "?")
       + (if .platform.variant then "/" + .platform.variant else "" end)
       + (if .annotations["vnd.docker.reference.type"] then "+" + .annotations["vnd.docker.reference.type"] else "" end))]
      | @tsv' "$work/index.json")
  while IFS=$'\t' read -r child platform; do
    [[ -n $child ]] || continue
    if same "child $child" "$child" "$src_repo@$child" "$dst_repo@$child"; then
      echo "child $child $platform identical"
    fi
  done <<<"$children"
fi

exit "$fail"
