#!/usr/bin/env bash
# Runs git-wal-bench (plan GT1 Task 11): the in-memory latency model at PUT
# delays of 20, 50, 100 and 150 ms, then, when LOAMS_GIT_BENCH_S3 is set
# (scripts/git/rustfs.sh start prints the endpoint), the same store over S3.
# One JSON line per run goes to stdout and to bench/results/git-wal/.
#
#   scripts/git/bench.sh                     # in-memory only
#   LOAMS_GIT_BENCH_S3=http://127.0.0.1:9100/loams-git scripts/git/bench.sh
#
# Knobs: DURATION (s, default 20), PUSHERS (32), PACK_BYTES (4096),
# LOAMS_GIT_BENCH_KEY / LOAMS_GIT_BENCH_SECRET (rustfsadmin).
set -euo pipefail

cd "$(dirname "$0")/../.."
DURATION=${DURATION:-20}
PUSHERS=${PUSHERS:-32}
PACK_BYTES=${PACK_BYTES:-4096}
OUT=bench/results/git-wal
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)

cargo build -p git-wal-bench --locked
BIN=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/debug/git-wal-bench

run() {
  local label=$1
  shift
  "$BIN" --repos 1 --pushers "$PUSHERS" --duration "$DURATION" --pack-bytes "$PACK_BYTES" "$@" \
    | tee -a "$OUT/$STAMP-$label.jsonl"
}

for delay in 20 50 100 150; do
  run memory --store memory:/// --sorted-listing --put-delay-ms "$delay"
done

if [[ -n "${LOAMS_GIT_BENCH_S3:-}" ]]; then
  endpoint=${LOAMS_GIT_BENCH_S3%/*}
  bucket=${LOAMS_GIT_BENCH_S3##*/}
  s3=(--store "s3://$bucket" --opt "aws_endpoint=$endpoint"
      --opt "aws_access_key_id=${LOAMS_GIT_BENCH_KEY:-rustfsadmin}"
      --opt "aws_secret_access_key=${LOAMS_GIT_BENCH_SECRET:-rustfsadmin}"
      --opt aws_region=us-east-1 --opt aws_allow_http=true
      --opt aws_virtual_hosted_style_request=false)
  run s3 "${s3[@]}" --put-delay-ms 0
  run s3 "${s3[@]}" --put-delay-ms 100
fi
