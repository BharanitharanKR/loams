#!/usr/bin/env bash
# Upgrades a BYOC stack (one byoc/install.sh made) over SSH (design §54 §10.5,
# Q832; UP1 Task 22). On the host it fetches the target release's `loams` CLI
# (its Linux package, checked against the release's SHA256SUMS) and runs
# `loams upgrade` there, streaming its output and returning its exit code
# (see docs/release/upgrades.md). Nothing listens on the host beyond SSH.
#
#   deploy/release/byoc/upgrade.sh [options] user@host --to VERSION
#
#   --to V                  the target release (one step from the stack's)
#   --check                 run `loams upgrade check` only
#   --rollback              run `loams upgrade rollback` instead
#   -p, --port N            SSH port [default: 22]
#   -i, --identity FILE     SSH private key
#   --dir DIR               the install directory on the host [default: loams]
#   -y, --yes               do not ask on the host (`--yes`)
#   --dry-run               print the SSH command and the remote script; connect to nothing
set -euo pipefail

port=22
identity=""
dir="loams"
to=""
mode="upgrade"
yes=0
dry_run=0
target=""

die() { echo "upgrade.sh: $*" >&2; exit 2; }

while (($#)); do
  case "$1" in
    --to) to="${2:?}"; shift 2 ;;
    --check) mode="check"; shift ;;
    --rollback) mode="rollback"; shift ;;
    -p|--port) port="${2:?}"; shift 2 ;;
    -i|--identity) identity="${2:?}"; shift 2 ;;
    --dir) dir="${2:?}"; shift 2 ;;
    -y|--yes) yes=1; shift ;;
    --dry-run) dry_run=1; shift ;;
    -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
    -*) die "unknown option $1" ;;
    *) [[ -z "$target" ]] || die "one host only"; target="$1"; shift ;;
  esac
done
[[ -n "$target" ]] || die "name the host: user@host"
[[ -n "$to" ]] || die "name the target release: --to VERSION"
[[ "$to" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || die "--to $to is not a release version"

# The remote side: fetch the target's CLI once, then run it in the stack.
# shellcheck disable=SC2016 # expanded on the host
script='
set -euo pipefail
dir="$1"; version="$2"; mode="$3"; yes="$4"
case "$dir" in /*) ;; *) dir="$HOME/$dir" ;; esac
stack="$dir/release"
test -f "$stack/compose.yaml" || { echo "no Loams stack in $stack" >&2; exit 4; }
case "$(uname -m)" in
  x86_64|amd64) deb="loams_${version}_amd64.deb" ;;
  *) echo "release $version has no Linux package for $(uname -m)" >&2; exit 6 ;;
esac
bin="$dir/.loams-upgrade/bin/loams-$version"
if [ ! -x "$bin" ]; then
  tmp="$(mktemp -d)"
  trap "rm -rf \"$tmp\"" EXIT
  base="https://github.com/ostrium-labs/loams/releases/download/v$version"
  curl -fsSL --retry 3 -o "$tmp/$deb" "$base/$deb"
  curl -fsSL --retry 3 -o "$tmp/SHA256SUMS" "$base/SHA256SUMS"
  (cd "$tmp" && grep " [*]\?$deb\$" SHA256SUMS | sha256sum -c --quiet -) \
    || { echo "$deb does not match the release SHA256SUMS" >&2; exit 9; }
  mkdir -p "$tmp/x"
  if command -v dpkg-deb >/dev/null; then
    dpkg-deb -x "$tmp/$deb" "$tmp/x"
  else
    (cd "$tmp" && ar x "$deb" && tar -xf data.tar.* -C x)
  fi
  install -D -m 0755 "$tmp/x/usr/bin/loams" "$bin"
fi
args=(upgrade)
case "$mode" in
  check) args+=(check --to "$version") ;;
  rollback) args+=(rollback) ;;
  upgrade) args+=(--to "$version") ;;
esac
args+=(--dir "$stack" --output text)
[ "$yes" = 1 ] && args+=(--yes)
exec "$bin" "${args[@]}"
'

ssh_opts=(-p "$port")
[[ -n "$identity" ]] && ssh_opts+=(-i "$identity")
# A TTY lets `loams upgrade` ask for its confirmation on the host.
((yes)) || ssh_opts+=(-t)
remote_cmd="bash -c $(printf '%q' "$script") loams-upgrade $(printf '%q ' "$dir" "$to" "$mode" "$yes")"

if ((dry_run)); then
  printf 'ssh'; printf ' %q' "${ssh_opts[@]}" "$target"; printf ' <remote script>\n'
  echo "remote: dir=$dir version=$to mode=$mode yes=$yes"
  echo "remote script:"
  printf '%s\n' "$script"
  exit 0
fi
exec ssh "${ssh_opts[@]}" "$target" "$remote_cmd"
