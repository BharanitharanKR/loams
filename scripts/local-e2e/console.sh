#!/usr/bin/env bash
# The console's Vite dev server on http://127.0.0.1:5190/, proxying the API to
# the stack's engine (LOAMS_API=http://127.0.0.1:18080, same origin for the
# browser), as a transient user unit `loams-e2e-console`.
#
#   console.sh [start|stop|logs]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
case "${1:-start}" in
  start)
    systemctl --user stop loams-e2e-console 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-console --description='Loams local-e2e console dev server (:5190)' \
      -p WorkingDirectory="$root/web/apps/console" -E PATH="$PATH" -E HOME="$HOME" \
      -E LOAMS_API="${E2E_ENGINE:-http://127.0.0.1:18080}" -E LOAMS_UI_BASE=/ \
      node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5190 --strictPort
    for _ in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:5190/ && break; sleep 1; done
    echo "console: http://127.0.0.1:5190/" ;;
  stop) systemctl --user stop loams-e2e-console ;;
  logs) journalctl --user -u loams-e2e-console --no-pager | tail -50 ;;
esac
