#!/usr/bin/env bash
# The release channel of a version: what a release of it is called on GitHub
# and which image tags it moves.
#
#   scripts/release/channel.sh <version|vversion>    print key=value lines
#   scripts/release/channel.sh --self-test
#
# A version with no `-` (0.1.0) is a stable release: a normal GitHub release
# (the repository's "Latest"), images :<version>, :<major>.<minor> and :latest.
# A version with a `-` (0.1.0-preview.3, 0.1.0-rc.1) is a prerelease: a GitHub
# pre-release that is never "Latest", images :<version> and the rolling
# :preview, and never :latest or :<major>.<minor>.
#
# Keys: stable, prerelease, latest_flag (the `gh release` flag), title,
# image_tags (space separated, :<version> first), alias_note (the release
# notes' "(also ...)" text).
set -euo pipefail

channel() {
  local version=${1#v}
  [[ "$version" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] \
    || { echo "channel.sh: '$1' is not X.Y.Z or X.Y.Z-pre" >&2; return 1; }
  if [[ "$version" == *-* ]]; then
    echo "stable=false"
    echo "prerelease=true"
    echo "latest_flag=--latest=false"
    echo "title=Loams $version (preview)"
    echo "image_tags=$version preview"
    echo "alias_note=also \`:preview\`"
  else
    local minor="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
    echo "stable=true"
    echo "prerelease=false"
    echo "latest_flag=--latest"
    echo "title=Loams $version"
    echo "image_tags=$version $minor latest"
    echo "alias_note=also \`:$minor\` and \`:latest\`"
  fi
}

self_test() {
  local failed=0
  expect() {
    local version=$1 key=$2 want=$3 got
    got=$(channel "$version" | sed -n "s/^$key=//p")
    if [[ "$got" != "$want" ]]; then
      echo "FAIL $version $key: got '$got', want '$want'" >&2
      failed=1
    fi
  }
  expect 0.1.0 prerelease false
  expect 0.1.0 latest_flag --latest
  expect 0.1.0 title "Loams 0.1.0"
  expect v0.1.0 image_tags "0.1.0 0.1 latest"
  expect 1.12.3 image_tags "1.12.3 1.12 latest"
  expect 0.1.0-preview.3 prerelease true
  expect 0.1.0-preview.3 latest_flag --latest=false
  expect 0.1.0-preview.3 title "Loams 0.1.0-preview.3 (preview)"
  expect 0.1.0-preview.3 image_tags "0.1.0-preview.3 preview"
  expect 0.1.0-rc.1 prerelease true
  expect 0.1.0-rc.1 latest_flag --latest=false
  expect 0.1.0-rc.1 image_tags "0.1.0-rc.1 preview"
  if channel 0.1 >/dev/null 2>&1 || channel latest >/dev/null 2>&1; then
    echo "FAIL: a malformed version was accepted" >&2
    failed=1
  fi
  ((failed == 0)) && echo "channel.sh: self-test ok"
  return "$failed"
}

if [[ "${1:-}" == --self-test ]]; then
  self_test
else
  [[ $# -eq 1 ]] || { echo "usage: channel.sh <version> | --self-test" >&2; exit 2; }
  channel "$1"
fi
