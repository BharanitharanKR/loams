#!/usr/bin/env bash
# The console's Vite dev server on http://127.0.0.1:5190/, proxying the API to
# the stack's engine (LOAMS_API=http://127.0.0.1:18080, same origin for the
# browser), as a transient user unit `loams-e2e-console`.
#
# `console.sh hosted` also stages the hosted web edition (deploy/stage.mjs, the
# build console.loams.dev serves, with engineUrl http://127.0.0.1:18080) and
# serves it on http://127.0.0.1:5192/ (unit `loams-e2e-console-hosted`). The dev
# server is the engine-served edition (base.yml only: no Graph, House or Live
# pages); the hosted one adds web.yml's pages.
#
#   console.sh [start|hosted|stop|logs]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
case "${1:-start}" in
  start)
    systemctl --user stop loams-e2e-console 2>/dev/null || true; systemctl --user reset-failed loams-e2e-console 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-console --description='Loams local-e2e console dev server (:5190)' \
      -p WorkingDirectory="$root/web/apps/console" -E PATH="$PATH" -E HOME="$HOME" \
      -E LOAMS_API="${E2E_ENGINE:-http://127.0.0.1:18080}" -E LOAMS_UI_BASE=/ \
      node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5190 --strictPort
    for _ in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:5190/ && break; sleep 1; done
    echo "console: http://127.0.0.1:5190/" ;;
  hosted)
    (cd "$root/web/apps/console" && { [[ -f dist/index.html ]] || pnpm build; } \
      && LOAMS_ENGINE_URL="${E2E_ENGINE:-http://127.0.0.1:18080}" LOAMS_AGENTD_ORIGINS=https://127.0.0.1:17443 node deploy/stage.mjs)
    systemctl --user stop loams-e2e-console-hosted 2>/dev/null || true; systemctl --user reset-failed loams-e2e-console-hosted 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-console-hosted --description='Loams local-e2e hosted console (:5192)' \
      -E PATH="$PATH" node "$here/hosted-console.mjs" "$root/web/apps/console/dist-cloudflare" 5192
    sleep 1; echo "hosted console: http://127.0.0.1:5192/" ;;
  stop) systemctl --user stop loams-e2e-console loams-e2e-console-hosted ;;
  logs) journalctl --user -u loams-e2e-console --no-pager | tail -50 ;;
esac
