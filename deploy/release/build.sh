#!/usr/bin/env bash
# Builds the Loams release images from deploy/release/Containerfile.<name>,
# for linux/amd64 and linux/arm64, tagged :<version> and :preview.
#
#   deploy/release/build.sh [options] [image...]
#
#   image            loams, loams-fabric, loams-agentd [default: all three]
#   --version V      the version tag [default: the tag on HEAD, else 0.0.0-dev.<sha>]
#   --registry R     [default: ghcr.io/ostrium-labs, or $LOAMS_REGISTRY]
#   --platforms P    comma-separated [default: linux/amd64,linux/arm64]
#   --push           push both tags (CI on a release tag); without it the
#                    images stay local
#   --jobs N         CARGO_BUILD_JOBS inside the build stage
#
# Uses podman (one manifest list per image, `podman manifest push --all`) when
# it is installed, else `docker buildx` (which needs --push, or a single
# platform, to keep the result: a multi-platform build cannot be --load-ed).
# Building a foreign platform runs under QEMU binfmt emulation, which is slow;
# CI builds each platform on a native runner by passing --platforms.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
registry="${LOAMS_REGISTRY:-ghcr.io/ostrium-labs}"
platforms="linux/amd64,linux/arm64"
version=""
push=0
jobs=""
images=()

die() { echo "build.sh: $*" >&2; exit 1; }

while (($#)); do
  case "$1" in
    --version) version="${2:?--version needs a value}"; shift 2 ;;
    --registry) registry="${2:?--registry needs a value}"; shift 2 ;;
    --platforms) platforms="${2:?--platforms needs a value}"; shift 2 ;;
    --push) push=1; shift ;;
    --jobs) jobs="${2:?--jobs needs a value}"; shift 2 ;;
    -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    loams|loams-fabric|loams-agentd) images+=("$1"); shift ;;
    *) die "unknown argument: $1" ;;
  esac
done
((${#images[@]})) || images=(loams loams-fabric loams-agentd)
[[ -z "$jobs" || "$jobs" =~ ^[0-9]+$ ]] || die "--jobs must be a number"

revision="$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -z "$version" ]]; then
  version="$(git -C "$root" describe --tags --exact-match HEAD 2>/dev/null || true)"
  [[ -n "$version" ]] || version="0.0.0-dev.${revision:0:12}"
fi
version="${version#v}"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "version $version is not semver"

if command -v podman >/dev/null 2>&1; then
  engine=podman
elif command -v docker >/dev/null 2>&1 && docker buildx version >/dev/null 2>&1; then
  engine=buildx
else
  die "needs podman, or docker with buildx"
fi

build_args=(--build-arg "VERSION=$version" --build-arg "REVISION=$revision")
[[ -n "$jobs" ]] && build_args+=(--build-arg "CARGO_BUILD_JOBS=$jobs")

for image in "${images[@]}"; do
  file="$root/deploy/release/Containerfile.$image"
  [[ -f "$file" ]] || die "no $file"
  ref="$registry/$image"
  echo "build.sh: $ref:$version and :preview for $platforms ($engine)"
  case "$engine" in
    podman)
      # A manifest list per image; --format docker keeps the HEALTHCHECK.
      podman manifest rm "$ref:$version" >/dev/null 2>&1 || true
      podman build --format docker --platform "$platforms" --manifest "$ref:$version" \
        "${build_args[@]}" -f "$file" "$root"
      podman tag "$ref:$version" "$ref:preview"
      if ((push)); then
        podman manifest push --all "$ref:$version" "docker://$ref:$version"
        podman manifest push --all "$ref:$version" "docker://$ref:preview"
      fi
      ;;
    buildx)
      out=(--push)
      if ((!push)); then
        [[ "$platforms" == *,* ]] && die "docker buildx keeps a multi-platform build only with --push; pass --platforms linux/amd64 to build locally"
        out=(--load)
      fi
      docker buildx build --platform "$platforms" "${build_args[@]}" \
        -t "$ref:$version" -t "$ref:preview" "${out[@]}" -f "$file" "$root"
      ;;
  esac
done
