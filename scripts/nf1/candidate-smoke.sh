#!/usr/bin/env bash
# NF1 Task 16, G3's extension smoke: deploy/loams-postgres-dev on the images in
# this checkout's pin file (a candidate's, after scripts/nf1/candidate.py
# apply), for one Postgres major:
#
#   candidate-smoke.sh MAJOR
#
# 1. the storage services, a tenant and a timeline of MAJOR (pageserver API);
# 2. compute1 on images.compute-v<MAJOR>: `SHOW server_version` is the pin's
#    pg_minor, then scripts/nf1/candidate-extensions.sql;
# 3. a branch of that timeline, and compute2 on it reads the rows written
#    before the branch point.
# The compose project is torn down (down -v, this project only) on exit.
# Run by nf1-candidate.yml on a hosted runner; it uses the stack's fixed
# loopback ports.
set -euo pipefail
major=${1:?usage: candidate-smoke.sh MAJOR}
[[ $major =~ ^[0-9]+$ ]] || { echo "MAJOR must be a number"; exit 2; }
root=$(cd "$(dirname "$0")/../.." && pwd)
stack=$root/deploy/loams-postgres-dev
project=${PROJECT:-nf1-candidate-$major}

read -r image minor < <(python3 -I - "$root/deploy/pins/loams-postgres-images.toml" "$major" <<'PY'
import sys, tomllib
pins = tomllib.load(open(sys.argv[1], "rb"))
img = pins["images"][f"compute-v{sys.argv[2]}"]
print(f'{img["repository"]}@{img["digest"]}', img["pg_minor"])
PY
)
export COMPUTE_IMAGE=$image
dc() { docker compose -p "$project" --project-directory "$stack" -f "$stack/compose.yaml" "$@"; }
trap 'dc --profile branch down -v > /dev/null 2>&1 || true' EXIT

echo "compute-v$major: $image (Postgres $minor)"
for i in 1 2 3; do
  dc --profile branch pull -q rustfs create-bucket storage_broker pageserver safekeeper1 compute1 compute2 && break
  [ "$i" -lt 3 ] || exit 1
  sleep $((i * 15))
done
dc up -d --pull never rustfs create-bucket storage_broker pageserver safekeeper1
for _ in $(seq 1 120); do curl -sf localhost:9898/v1/status > /dev/null && break; sleep 1; done
curl -sf localhost:9898/v1/status > /dev/null

TENANT_ID=$(openssl rand -hex 16) TIMELINE_ID=$(openssl rand -hex 16)
export TENANT_ID TIMELINE_ID
curl -sf -X PUT -H 'Content-Type: application/json' \
  -d '{"mode":"AttachedSingle","generation":1,"tenant_conf":{}}' \
  "localhost:9898/v1/tenant/$TENANT_ID/location_config" > /dev/null
curl -sf -X POST -H 'Content-Type: application/json' \
  -d "{\"new_timeline_id\":\"$TIMELINE_ID\",\"pg_version\":$major}" \
  "localhost:9898/v1/tenant/$TENANT_ID/timeline/" > /dev/null

psql_on() { # service, then psql arguments
  local svc=$1; shift
  dc exec -T "$svc" psql -X -q -v ON_ERROR_STOP=1 -h localhost -p 55433 -U cloud_admin -d postgres "$@"
}
ready() {
  for _ in $(seq 1 120); do psql_on "$1" -tAc 'SELECT 1' > /dev/null 2>&1 && return 0; sleep 1; done
  dc logs --tail 100 "$1"; return 1
}

dc up -d --pull never compute1
ready compute1
version=$(psql_on compute1 -tAc 'SHOW server_version')
echo "server_version: $version"
[[ $version == "$minor" || $version == "$minor "* ]] || { echo "expected Postgres $minor, got $version"; exit 1; }
psql_on compute1 < "$root/scripts/nf1/candidate-extensions.sql"
echo "extensions: ok"

# The branch point is after the smoke's rows (a checkpoint makes the LSN durable on the pageserver).
lsn=$(psql_on compute1 -tAc 'SELECT pg_current_wal_flush_lsn()')
for _ in $(seq 1 60); do
  last=$(curl -sf "localhost:9898/v1/tenant/$TENANT_ID/timeline/$TIMELINE_ID" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["last_record_lsn"])')
  python3 -I -c 'import sys; f=lambda s: int(s.split("/")[0],16)<<32|int(s.split("/")[1],16); sys.exit(0 if f(sys.argv[1])>=f(sys.argv[2]) else 1)' "$last" "$lsn" && break
  sleep 1
done
BRANCH_TIMELINE_ID=$(openssl rand -hex 16)
export BRANCH_TIMELINE_ID
curl -sf -X POST -H 'Content-Type: application/json' \
  -d "{\"new_timeline_id\":\"$BRANCH_TIMELINE_ID\",\"ancestor_timeline_id\":\"$TIMELINE_ID\",\"ancestor_start_lsn\":\"$lsn\"}" \
  "localhost:9898/v1/tenant/$TENANT_ID/timeline/" > /dev/null
dc --profile branch up -d --pull never compute2
ready compute2
rows=$(psql_on compute2 -tAc 'SELECT count(*) FROM candidate_items')
[ "$rows" = 3 ] || { echo "the branch sees $rows rows, expected 3"; exit 1; }
echo "branch: ok ($rows rows)"
