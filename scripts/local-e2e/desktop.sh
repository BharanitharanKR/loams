#!/usr/bin/env bash
# Loams Desktop (apps/desktop-electron), built and run with `electron-vite preview`, as a transient user unit
# `loams-e2e-desktop`, with its own userData ($E2E_HOME/desktop-userdata, so
# your real desktop settings are untouched):
# - its local engine (`loams dev`, which the app supervises) is the release
#   engine binary from build-binaries.sh (LOAMS_BIN);
# - a server "Local compose stack (e2e)" at http://127.0.0.1:18080, made the
#   active one on first start (servers.json);
# - Studio talks to `agentd.sh desktop`'s daemon in that userData.
# Not `pnpm dev`: in dev mode the main bundle throws at load, "Could not resolve
# "pg-native" imported by "pg"" (Vite's optional-peer stub; bug in the local-e2e
# report). The production build has no such stub.
# Needs the console build (web/apps/console/dist): `pnpm --filter @loams/console build`.
#
#   desktop.sh [start|stop|logs]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
ud="$E2E_HOME/desktop-userdata"
case "${1:-start}" in
  start)
    mkdir -p "$ud"
    [[ -d "$root/web/apps/console/dist" ]] || pnpm -C "$root" --filter @loams/console build
    # pnpm does not run electron's postinstall: fetch its binary (from ~/.cache/electron when there).
    edir="$(ls -d "$root"/node_modules/.pnpm/electron@*/node_modules/electron | head -1)"
    [[ -f "$edir/path.txt" ]] || (cd "$edir" && node install.js)
    if [[ ! -f "$ud/servers.json" ]]; then
      cat > "$ud/servers.json" <<JSON
{"v":1,"servers":[{"id":"e2e-stack","name":"Local compose stack (e2e)","kind":"remote","url":"${E2E_ENGINE:-http://127.0.0.1:18080}"}],"activeId":"e2e-stack"}
JSON
    fi
    systemctl --user stop loams-e2e-desktop 2>/dev/null || true; systemctl --user reset-failed loams-e2e-desktop 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-desktop --description='Loams local-e2e desktop app (electron-vite preview)' \
      -p WorkingDirectory="$root/apps/desktop-electron" -E PATH="$PATH" -E HOME="$HOME" \
      -E DISPLAY="${DISPLAY:-}" -E WAYLAND_DISPLAY="${WAYLAND_DISPLAY:-}" -E XDG_RUNTIME_DIR="$XDG_RUNTIME_DIR" \
      -E DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-}" -E XDG_SESSION_TYPE="${XDG_SESSION_TYPE:-}" \
      -E LOAMS_DESKTOP_USER_DATA="$ud" -E LOAMS_BIN="$E2E_HOME/bin/loams-engine" \
      -E LOAMS_DESKTOP_NO_UPDATE_CHECK=1 \
      sh -c 'pnpm build && exec pnpm exec electron-vite preview --skipBuild'
    echo "desktop: started (journalctl --user -u loams-e2e-desktop -f)" ;;
  stop) systemctl --user stop loams-e2e-desktop ;;
  logs) journalctl --user -u loams-e2e-desktop --no-pager | tail -80 ;;
esac
