#!/usr/bin/env bash
# Sends one statement (argument or stdin) to the spike House over ClickHouse HTTP.
set -euo pipefail
sql=${1:-$(cat)}
podman exec -i he0v3-house sh -c 'curl -sS -u "default:$(cat /var/lib/loams-house/house.key)" \
  --data-binary @- "http://127.0.0.1:18123/?$0"' "${CH_QS:-}" <<<"$sql"
