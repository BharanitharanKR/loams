#!/usr/bin/env bash
# Loams Desktop (apps/desktop-electron), run with `pnpm dev` (or `pnpm preview`,
# the production build) as a transient user unit `loams-e2e-desktop`, with its
# own userData ($E2E_HOME/desktop-userdata, so your real desktop settings are
# untouched):
# - its local engine (`loams dev`, which the app supervises) is the release
#   engine binary from build-binaries.sh (LOAMS_BIN);
# - Studio's daemon is the release loams-agentd (LOAMS_AGENTD_BIN), which the
#   app starts or attaches to itself, with the Mock harness (no API keys);
# - a server "Local compose stack (e2e)" at http://127.0.0.1:18080, made the
#   active one on first start (servers.json);
# - the Postgres page reaches pg-control (127.0.0.1:7680) with the API key
#   up.sh minted into $E2E_HOME/pg-api-key (LOAMS_PG_CONTROL_API_KEY_FILE;
#   main reads it, the renderer never sees it).
# Both scripts fetch Electron's binary first (install-electron).
#
#   desktop.sh [start [dev|preview]|stop|logs]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
ud="$E2E_HOME/desktop-userdata"
case "${1:-start}" in
  start)
    mkdir -p "$ud"
    run="${2:-dev}"; [[ "$run" == dev || "$run" == preview ]] || { echo "desktop.sh start [dev|preview]" >&2; exit 2; }
    if [[ ! -f "$ud/servers.json" ]]; then
      cat > "$ud/servers.json" <<JSON
{"v":1,"servers":[{"id":"e2e-stack","name":"Local compose stack (e2e)","kind":"remote","url":"${E2E_ENGINE:-http://127.0.0.1:18080}"}],"activeId":"e2e-stack"}
JSON
    fi
    systemctl --user stop loams-e2e-desktop 2>/dev/null || true; systemctl --user reset-failed loams-e2e-desktop 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-desktop --description='Loams local-e2e desktop app' \
      -p WorkingDirectory="$root/apps/desktop-electron" -E PATH="$PATH" -E HOME="$HOME" \
      -E DISPLAY="${DISPLAY:-}" -E WAYLAND_DISPLAY="${WAYLAND_DISPLAY:-}" -E XDG_RUNTIME_DIR="$XDG_RUNTIME_DIR" \
      -E DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-}" -E XDG_SESSION_TYPE="${XDG_SESSION_TYPE:-}" \
      -E LOAMS_DESKTOP_USER_DATA="$ud" -E LOAMS_BIN="$E2E_HOME/bin/loams-engine" \
      -E LOAMS_AGENTD_BIN="$E2E_HOME/bin/loams-agentd" -E LOAMS_DESKTOP_HARNESS=mock \
      -E LOAMS_DESKTOP_NO_UPDATE_CHECK=1 \
      -E LOAMS_PG_CONTROL_API_KEY_FILE="$E2E_HOME/pg-api-key" \
      pnpm run "$run"
    echo "desktop: started, pnpm $run (journalctl --user -u loams-e2e-desktop -f)" ;;
  stop) systemctl --user stop loams-e2e-desktop ;;
  logs) journalctl --user -u loams-e2e-desktop --no-pager | tail -80 ;;
esac
