#!/usr/bin/env bash
# Stops the Loams preview stack that byoc/install.sh put on a host.
#
#   deploy/release/byoc/uninstall.sh [-p port] [-i key] [--dir DIR] [--purge] user@host
#
# Without --purge the containers go and the data volumes and the install
# directory stay, so install.sh brings the same data back. --purge also deletes
# the volumes (every database, bucket and tailnet identity on the host) and
# the directory. Docker itself is left installed.
set -euo pipefail

port=22; identity=""; dir="loams"; purge=0; target=""
die() { echo "uninstall.sh: $*" >&2; exit 1; }
while (($#)); do
  case "$1" in
    -p|--port) port="${2:?}"; shift 2 ;;
    -i|--identity) identity="${2:?}"; shift 2 ;;
    --dir) dir="${2:?}"; shift 2 ;;
    --purge) purge=1; shift ;;
    -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown option: $1" ;;
    *) [[ -z "$target" ]] || die "one target only"; target="$1"; shift ;;
  esac
done
[[ -n "$target" ]] || die "usage: uninstall.sh [options] user@host"
[[ "$dir" =~ ^[A-Za-z0-9._/-]+$ ]] || die "--dir may hold only letters, digits and ._/-"
[[ "$dir" != *..* && "$dir" != "." && "$dir" != "./" && "${dir%/}" != "" ]] || die "--dir $dir is not an install directory"
ssh_opts=(-p "$port"); [[ -n "$identity" ]] && ssh_opts+=(-i "$identity")

# shellcheck disable=SC2016 # expanded on the host
script='
set -euo pipefail
cd "$1/release" || { echo "no Loams install in $1" >&2; exit 1; }
sudo=""; [ "$(id -u)" -eq 0 ] || sudo="sudo"
dc="docker compose"; docker info >/dev/null 2>&1 || dc="$sudo docker compose"
if [ "$2" = 1 ]; then
  $dc --profile "*" down --volumes --remove-orphans
  cd ~ && rm -rf -- "$1"
  echo "removed the stack, its volumes and $1"
else
  $dc --profile "*" down --remove-orphans
  echo "stopped the stack; data volumes and $1 kept (--purge removes them)"
fi
'
ssh -t "${ssh_opts[@]}" "$target" "bash -c $(printf '%q' "$script") loams-uninstall $(printf '%q ' "$dir" "$purge")"
