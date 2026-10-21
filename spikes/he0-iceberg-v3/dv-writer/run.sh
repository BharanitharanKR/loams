#!/usr/bin/env bash
# HE0 G2 (unmerged): Spark-written v3 tables get deletion vectors written by iceberg-rust's
# DeletionVectorWriter, committed by Iceberg Java's RowDelta, and read back by Spark, Trino
# and iceberg-rust. Needs `../stack.sh up`; setup.sql (re)creates the tables.
#
#   run.sh              # before.sql, write, commit, after.sql, the iceberg-rust read
#   run.sh commit <t>   # commit .work/dvs-<t>.tsv again
#   run.sh trino        # the same reads on Trino (after `../stack.sh trino`)
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SPIKE=$(dirname "$HERE")
LK=http://127.0.0.1:18181/catalog
S3=http://127.0.0.1:19000
OUT="$HERE/results"
WORK="$HERE/.work"
PROBE="cargo run -q --manifest-path $HERE/Cargo.toml --"
TABLES=(dv dv_part upgraded)
mkdir -p "$OUT" "$WORK"

spark_shell() { # table
  echo ":load /scripts/commit.scala" | podman run -i --rm --pod he0v3 --name he0v3-spark-shell -e HOME=/tmp \
    -v "$SPIKE/.cache/jars:/opt/iceberg:ro" -v "$HERE:/scripts:ro" -v "$WORK:/work:ro" \
    docker.io/apache/spark:4.0.4 /opt/spark/bin/spark-shell --master 'local[2]' --driver-memory 1g \
    --jars /opt/iceberg/iceberg-spark-runtime-4.0_2.13-1.12.0.jar,/opt/iceberg/iceberg-aws-bundle-1.12.0.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.ui.enabled=false \
    --conf spark.sql.catalog.spark=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark.type=rest --conf "spark.sql.catalog.spark.uri=$LK" \
    --conf spark.sql.catalog.spark.warehouse=spark \
    --conf spark.sql.catalog.spark.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf "spark.sql.catalog.spark.s3.endpoint=$S3" --conf spark.sql.catalog.spark.s3.path-style-access=true \
    --conf spark.sql.catalog.spark.s3.access-key-id=spikeadmin \
    --conf spark.sql.catalog.spark.s3.secret-access-key=spikeadmin-secret \
    --conf spark.sql.catalog.spark.client.region=us-east-1 \
    --conf "spark.he0.table=dvw.$1" 2>&1 \
    | grep -E "^committed|^[A-Za-z.]+(Exception|Error):" | grep -v AccessDeniedException || true
}

case "${1:-}" in
  "")
    "$SPIKE/stack.sh" spark "$HERE/setup.sql" >/dev/null
    "$SPIKE/stack.sh" spark "$HERE/before.sql" | grep '^expect' | tee "$OUT/before.txt"
    : > "$OUT/write.txt"
    for t in "${TABLES[@]}"; do
      echo "## dvw.$t" | tee -a "$OUT/write.txt"
      $PROBE "$LK" write spark "dvw.$t" "$WORK/dvs-$t.tsv" | tee -a "$OUT/write.txt"
      spark_shell "$t" | tee -a "$OUT/write.txt"
    done
    "$SPIKE/stack.sh" spark "$HERE/after.sql" | grep '^spark' | tee "$OUT/spark-after.txt"
    : > "$OUT/rust-after.txt"
    for t in dv:500 dv_part:1000 upgraded:500; do
      name=${t%%:*}; min=${t##*:}
      printf 'rust %s\t' "$name" | tee -a "$OUT/rust-after.txt"
      $PROBE "$LK" read spark "dvw.$name" | tee -a "$OUT/rust-after.txt"
      printf 'rust %s>=%s\t' "$name" "$min" | tee -a "$OUT/rust-after.txt"
      $PROBE "$LK" read spark "dvw.$name" "$min" | tee -a "$OUT/rust-after.txt"
    done
    ;;
  commit) # table: commit an existing .work/dvs-<table>.tsv
    spark_shell "$2"
    ;;
  trino)
    "$SPIKE/stack.sh" trino-sql "$HERE/trino-after.sql" | grep trino | tee "$OUT/trino-after.txt"
    ;;
  *) echo "usage: $0 [trino]" >&2; exit 2 ;;
esac
