#!/usr/bin/env bash
# Runs loams-agentd on this host for Studio, as a transient user unit, with the
# Mock harness (LOAMS_DESKTOP_HARNESS=mock: deterministic turns, no API keys)
# and its own HOME under $E2E_HOME/agentd (it never reads your home
# directory). Two daemons, one per edition:
#
#   agentd.sh desktop   `run --child` in the desktop's userData
#                       ($E2E_HOME/desktop-userdata/agentd), where Electron main
#                       finds agentd.json and agentd.token. The release binary
#                       from build-binaries.sh. Unit: loams-e2e-agentd-desktop.
#   agentd.sh web       `serve --remote` on https://127.0.0.1:17443 (self-signed)
#                       for the web console's Settings > Agentd hosts, with the
#                       test issuer (LOAMS_AGENTD_TEST_ISSUER, a local JWKS):
#                       that needs a DEBUG build (a release daemon ignores it),
#                       so this builds `cargo build -p loams-agentd` on the host.
#                       Writes the access token (7 days) to
#                       $E2E_HOME/agentd/web/token. Unit: loams-e2e-agentd-web.
#   agentd.sh stop      stops both.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/env.sh"
root="$(cd "$here/../.." && pwd)"
mode="${1:?usage: agentd.sh desktop|web|stop}"
base="$E2E_HOME/agentd"
home="$base/home"
mkdir -p "$home" "$base/worktrees"

if [[ "$mode" == stop ]]; then
  systemctl --user stop loams-e2e-agentd-desktop loams-e2e-agentd-web 2>/dev/null || true
  exit 0
fi

# The project Studio opens: a local copy of the git seed's repository (or a new one).
project="$home/projects/e2e-sample"
if [[ ! -d "$project/.git" ]]; then
  mkdir -p "$home/projects"
  if [[ -d "$E2E_HOME/git-src/e2e-sample/.git" ]]; then
    git clone -q "$E2E_HOME/git-src/e2e-sample" "$project"
  else
    mkdir -p "$project" && git -C "$project" init -q -b main && echo '# e2e-sample' > "$project/README.md"
    git -C "$project" add -A && git -C "$project" -c user.name=e2e -c user.email=e2e@localhost commit -q -m init
  fi
fi
printf '[user]\n\tname = Loams e2e\n\temail = e2e@localhost\n[commit]\n\tgpgsign = false\n' > "$home/.gitconfig"

envs=(-E HOME="$home" -E PATH="/usr/bin:/bin" -E LOAMS_DESKTOP_HARNESS=mock
      -E LOAMS_DESKTOP_WORKTREES_DIR="$base/worktrees" -E LOAMS_DESKTOP_NO_LOGIN_SHELL=1
      -E LOAMS_DESKTOP_DEVICE_NAME=local-e2e -E LOAMS_DESKTOP_MOCK_EDIT=notes.txt -E RUST_LOG=info)

case "$mode" in
  desktop)
    dir="$E2E_HOME/desktop-userdata/agentd"
    mkdir -p "$dir"; chmod 0700 "$dir"
    [[ -f "$dir/config.toml" ]] || : > "$dir/config.toml"
    systemctl --user stop loams-e2e-agentd-desktop 2>/dev/null || true; systemctl --user reset-failed loams-e2e-agentd-desktop 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-agentd-desktop --description='Loams local-e2e agentd (desktop, child mode)' \
      -p WorkingDirectory="$home" "${envs[@]}" \
      "$E2E_HOME/bin/loams-agentd" run --child --config "$dir/config.toml"
    for _ in $(seq 60); do [[ -s "$dir/agentd.json" ]] && break; sleep 0.5; done
    cat "$dir/agentd.json"; echo
    port="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["connectPort"])' "$dir/agentd.json")"
    curl -s -X POST "http://127.0.0.1:$port/loams.agentd.v1.ProjectService/AddRepo" -H "authorization: Bearer $(cat "$dir/agentd.token")" \
      -H 'content-type: application/json' -H 'connect-protocol-version: 1' -d "{\"path\": \"$project\"}"; echo
    ;;
  web)
    target="$(cd "$root" && cargo metadata --format-version 1 --no-deps | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
    touch "$root/crates/loams-agentd/src/main.rs"
    (cd "$root" && nice cargo build -j 4 -p loams-agentd --bin loams-agentd)
    install -m 0755 "$target/debug/loams-agentd" "$base/loams-agentd-debug"
    web="$base/web"; mkdir -p "$web/tls" "$web/dir"; chmod 0700 "$web/dir"
    [[ -f "$web/dir/config.toml" ]] || : > "$web/dir/config.toml"
    if [[ ! -s "$web/tls/cert.pem" ]]; then
      openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout "$web/tls/key.pem" \
        -out "$web/tls/cert.pem" -days 30 -subj /CN=127.0.0.1 -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' 2>/dev/null
    fi
    node "$here/agentd-token.mjs" "$web"   # writes jwks.json and token (sub local-e2e, 7 days)
    systemctl --user stop loams-e2e-agentd-web 2>/dev/null || true; systemctl --user reset-failed loams-e2e-agentd-web 2>/dev/null || true
    systemd-run --user --unit=loams-e2e-agentd-web --description='Loams local-e2e agentd (web, remote mode)' \
      -p WorkingDirectory="$home" "${envs[@]}" -E LOAMS_AGENTD_TEST_ISSUER="$web/jwks.json" \
      "$base/loams-agentd-debug" serve --remote --listen 127.0.0.1:17443 \
      --tls-cert "$web/tls/cert.pem" --tls-key "$web/tls/key.pem" \
      --issuer http://127.0.0.1:9/application/o/loams-e2e/ --audience loams-agentd --subject local-e2e \
      --allow-origin http://127.0.0.1:5190 --allow-origin http://localhost:5190 \
      --allow-origin http://127.0.0.1:5192 --allow-origin http://localhost:5192 \
      --config "$web/dir/config.toml"
    for _ in $(seq 30); do curl -sk -o /dev/null https://127.0.0.1:17443/ && break; sleep 1; done
    # Register the project, as the desktop's folder picker would (the web edition adds only git URLs).
    curl -sk -X POST https://127.0.0.1:17443/loams.agentd.v1.ProjectService/AddRepo -H "authorization: Bearer $(cat "$web/token")" \
      -H 'content-type: application/json' -H 'connect-protocol-version: 1' -d "{\"path\": \"$project\"}"; echo
    systemctl --user --no-pager status loams-e2e-agentd-web | head -5
    echo "token: $web/token (paste into the console's Settings > Agentd hosts with https://127.0.0.1:17443)"
    ;;
  *) echo "usage: agentd.sh desktop|web|stop" >&2; exit 2 ;;
esac
