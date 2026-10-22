#!/usr/bin/env bash
# HE0 G3 (unmerged): iceberg-rust's RowDelta commits deletion vectors (v3) and position
# deletes (v2) to tables Spark wrote; Spark, Trino 483, DuckDB 2.0.0 and iceberg-rust read
# them back. Needs `../stack.sh up` (and `../stack.sh trino` for the Trino reads).
#
#   run.sh   # setup.sql, before.sql, commit, then every reader
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SPIKE=$(dirname "$HERE")
LK=http://127.0.0.1:18181/catalog
OUT="$HERE/results"
PROBE="cargo run -q --manifest-path $HERE/Cargo.toml --"
mkdir -p "$OUT"

"$SPIKE/stack.sh" spark "$HERE/setup.sql" >/dev/null
"$SPIKE/stack.sh" spark "$HERE/before.sql" | grep '^expect' | tee "$OUT/before.txt"
: > "$OUT/commit.txt"
for t in dv dv_part upgraded pos_v2; do
  echo "## rdw.$t" | tee -a "$OUT/commit.txt"
  $PROBE "$LK" commit spark "rdw.$t" 2>/dev/null | tee -a "$OUT/commit.txt"
done
"$SPIKE/stack.sh" spark "$HERE/after.sql" | grep '^spark' | tee "$OUT/spark-after.txt"
: > "$OUT/rust-after.txt"
for t in dv:500 dv_part:1000 upgraded:500 pos_v2:500; do
  name=${t%%:*}; min=${t##*:}
  printf 'rust %s\t' "$name" | tee -a "$OUT/rust-after.txt"
  $PROBE "$LK" read spark "rdw.$name" 2>/dev/null | tee -a "$OUT/rust-after.txt"
  printf 'rust %s>=%s\t' "$name" "$min" | tee -a "$OUT/rust-after.txt"
  $PROBE "$LK" read spark "rdw.$name" "$min" 2>/dev/null | tee -a "$OUT/rust-after.txt"
done
"$SPIKE/stack.sh" trino-sql "$HERE/trino-after.sql" | grep '^trino' | tee "$OUT/trino-after.txt"
uv run -q --no-project --with duckdb==2.0.0 python -I "$HERE/duckdb_after.py" | tee "$OUT/duckdb-after.txt"
