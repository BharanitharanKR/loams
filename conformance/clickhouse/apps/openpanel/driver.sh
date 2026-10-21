#!/usr/bin/env bash
# HE0 driver for OpenPanel: synthetic traffic only. Task 1 checks that the
# stack answers through its proxy; Task 4 adds the event-ingest driver (through
# the HTTP API, so the Redis buffers run) and the dashboard, funnel and
# retention reads.
set -euo pipefail

URL=${HE0_OPENPANEL_URL:-http://127.0.0.1:${HE0_OPENPANEL_PORT:-3200}}
for _ in $(seq 1 120); do
  curl -fsS "$URL/api/healthcheck" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$URL/api/healthcheck" >/dev/null
echo "driver: openpanel answers at $URL"
