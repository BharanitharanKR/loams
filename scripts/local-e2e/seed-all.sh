#!/usr/bin/env bash
# Seeds every product of the running local-e2e stack (up.sh) from the data
# fetch-data.sh downloaded. Each seed is idempotent and writes its checks to
# $E2E_HOME/logs/seed-<name>.json; this script goes on when one fails.
#
#   scripts/local-e2e/seed-all.sh [collections streams graph durable house relational live git ...]
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
py="$E2E_HOME/venv/bin/python"
if [[ ! -x "$py" ]]; then
  UV_CACHE_DIR="$E2E_HOME/uv-cache" uv venv -q -p 3.12 "$E2E_HOME/venv"
  UV_CACHE_DIR="$E2E_HOME/uv-cache" uv pip install -q -p "$py" -r "$here/requirements.txt"
fi
tsx="$(ls -d "$root"/node_modules/.pnpm/tsx@*/node_modules/tsx/dist/cli.mjs 2>/dev/null | head -1)"
what=("$@"); ((${#what[@]})) || what=(relational collections streams graph durable house live git)
status=0
for w in "${what[@]}"; do
  echo "=== seed $w"
  case "$w" in
    house)
      E2E_HOUSE_PASSWORD="$(ecompose exec -T house cat /var/lib/loams-house/house.key)" \
        "$py" -I "$here/seed/house.py" || status=1 ;;
    live) node "$tsx" "$here/seed/live.ts" || status=1 ;;
    git) "$here/seed/git.sh" || status=1 ;;
    *) "$py" -I "$here/seed/$w.py" || status=1 ;;
  esac
done
exit $status
