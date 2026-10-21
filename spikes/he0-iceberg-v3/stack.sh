#!/usr/bin/env bash
# HE0 Task 11 spike stack (plan HE1, unmerged branch): RustFS, Postgres,
# Lakekeeper, and on demand the House (loams-fabric 0.1.0), Spark 4.0.4 with
# Iceberg 1.12.0 and Trino 483, all in one podman pod so every service sees the
# others on 127.0.0.1 and the host sees the same ports.
#
#   stack.sh up            # pod + rustfs + postgres + lakekeeper, bucket, warehouses
#   stack.sh house         # the House on the "lakehouse" warehouse (bucket house)
#   stack.sh spark <file>  # runs a spark-sql file against the catalogs
#   stack.sh trino         # starts Trino with both catalogs
#   stack.sh trino-sql <f> # runs a SQL file on Trino
#   stack.sh down          # removes every container and the pod (no volumes are made)
#
# Nothing here keeps data outside the containers: `down` removes all of it.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
POD=he0v3
KEY=spikeadmin
SECRET=spikeadmin-secret
S3=http://127.0.0.1:19000
LK=http://127.0.0.1:18181
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

RUSTFS=docker.io/rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff
POSTGRES=docker.io/library/postgres:17
LAKEKEEPER=quay.io/lakekeeper/catalog:v0.13.6@sha256:d6829722cac0d00dfc5665b0955766387b93ccadd8b1e70e679b49619bc30ea5
HOUSE=ghcr.io/ostrium-labs/loams-fabric:0.1.0
SPARK=docker.io/apache/spark:4.0.4
TRINO=docker.io/trinodb/trino:483@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534

wait_for() { # url
  for _ in $(seq 1 90); do curl -sf -o /dev/null "$1" && return 0; sleep 1; done
  echo "timed out waiting for $1" >&2; return 1
}

warehouse() { # name key-prefix bucket
  local kp=null; [ -n "$2" ] && kp="\"$2\""
  curl -sf -X POST "$LK/management/v1/warehouse" -H 'content-type: application/json' -d @- <<JSON
{"warehouse-name": "$1", "project-id": "00000000-0000-0000-0000-000000000000",
 "storage-profile": {"type": "s3", "bucket": "$3", "key-prefix": $kp, "endpoint": "$S3/",
   "region": "us-east-1", "path-style-access": true, "flavor": "s3-compat", "sts-enabled": false,
   "remote-signing-enabled": false},
 "storage-credential": {"type": "s3", "credential-type": "access-key",
   "aws-access-key-id": "$KEY", "aws-secret-access-key": "$SECRET"}}
JSON
  echo
}

case "${1:-}" in
  up)
    podman pod create --name "$POD" -p 127.0.0.1:19000:19000 -p 127.0.0.1:18181:18181 \
      -p 127.0.0.1:18123:18123 -p 127.0.0.1:18080:8080 >/dev/null
    podman run -d --pod "$POD" --name he0v3-rustfs -e RUSTFS_VOLUMES=/data \
      -e RUSTFS_ADDRESS=0.0.0.0:19000 -e RUSTFS_CONSOLE_ENABLE=false \
      -e RUSTFS_ACCESS_KEY="$KEY" -e RUSTFS_SECRET_KEY="$SECRET" "$RUSTFS" >/dev/null
    podman run -d --pod "$POD" --name he0v3-postgres -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_DB=lakekeeper "$POSTGRES" >/dev/null
    for _ in $(seq 1 60); do
      podman exec he0v3-postgres pg_isready -U postgres -d lakekeeper >/dev/null 2>&1 && break; sleep 1
    done
    sleep 2
    LKENV=(-e LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@127.0.0.1:5432/lakekeeper
           -e LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@127.0.0.1:5432/lakekeeper
           -e LAKEKEEPER__PG_ENCRYPTION_KEY=spike-not-a-secret-spike-not-a-secret
           -e LAKEKEEPER__LISTEN_PORT=18181 -e LAKEKEEPER__BASE_URI=$LK
           -e RUST_LOG=info)
    podman run --rm --pod "$POD" "${LKENV[@]}" "$LAKEKEEPER" migrate
    podman run -d --pod "$POD" --name he0v3-lakekeeper "${LKENV[@]}" "$LAKEKEEPER" serve >/dev/null
    for _ in $(seq 1 60); do
      curl -sf -o /dev/null -X PUT -H "x-amz-content-sha256: ${EMPTY_SHA}" \
        --aws-sigv4 aws:amz:us-east-1:s3 --user "${KEY}:${SECRET}" "$S3/spike" && break
      curl -sf -o /dev/null -I -H "x-amz-content-sha256: ${EMPTY_SHA}" \
        --aws-sigv4 aws:amz:us-east-1:s3 --user "${KEY}:${SECRET}" "$S3/spike" && break
      sleep 1
    done
    wait_for "$LK/health"
    curl -sf -X POST "$LK/management/v1/bootstrap" -H 'content-type: application/json' \
      -d '{"accept-terms-of-use": true}'
    # No STS and no remote signing: every client brings its own keys. With
    # Lakekeeper's default remote signing, Spark's ranged reads of a deletion
    # vector's Puffin file fail with SignatureDoesNotMatch against RustFS.
    warehouse spark spark spike
    # The House's store must be a bucket root: with s3://bucket/prefix its
    # writes work but chDB's reads through house-cache are refused (403), see
    # the report's side findings.
    curl -sf -o /dev/null -X PUT -H "x-amz-content-sha256: ${EMPTY_SHA}" \
      --aws-sigv4 aws:amz:us-east-1:s3 --user "${KEY}:${SECRET}" "$S3/house"
    warehouse lakehouse "" house
    ;;
  house)
    podman run -d --pod "$POD" --name he0v3-house \
      -e AWS_ENDPOINT_URL="$S3" -e AWS_ACCESS_KEY_ID="$KEY" -e AWS_SECRET_ACCESS_KEY="$SECRET" \
      -e AWS_REGION=us-east-1 -e RUST_LOG=info "$HOUSE" house --single-node \
      --data-dir=/var/lib/loams-house --house-listen=127.0.0.1:18123 \
      --sandbox=none --unsafe-no-sandbox \
      --catalog "rest:$LK/catalog?warehouse=lakehouse" --store s3://house >/dev/null
    ;;
  spark)
    file=$(realpath "$2"); shift 2
    podman run --rm --pod "$POD" --name he0v3-spark \
      -v "$HERE/.cache/jars:/opt/iceberg:ro" -v "$(dirname "$file"):/work:ro" "$SPARK" \
      /opt/spark/bin/spark-sql --master 'local[4]' --driver-memory 2g \
      --jars /opt/iceberg/iceberg-spark-runtime-4.0_2.13-1.12.0.jar,/opt/iceberg/iceberg-aws-bundle-1.12.0.jar \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=4 \
      --conf spark.sql.catalog.spark=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.spark.type=rest --conf "spark.sql.catalog.spark.uri=$LK/catalog" \
      --conf spark.sql.catalog.spark.warehouse=spark \
      --conf spark.sql.catalog.house=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.house.type=rest --conf "spark.sql.catalog.house.uri=$LK/catalog" \
      --conf spark.sql.catalog.house.warehouse=lakehouse \
      $(for c in spark house; do printf -- '--conf spark.sql.catalog.%s.%s ' \
          "$c" "io-impl=org.apache.iceberg.aws.s3.S3FileIO" "$c" "s3.endpoint=$S3" \
          "$c" "s3.path-style-access=true" "$c" "s3.access-key-id=$KEY" \
          "$c" "s3.secret-access-key=$SECRET" "$c" "client.region=us-east-1"; done) \
      --conf spark.sql.defaultCatalog=spark "$@" -f "/work/$(basename "$file")" 2>&1 \
      | grep -vE '^(WARNING|[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (WARN|INFO))'
    ;;
  trino)
    podman run -d --pod "$POD" --name he0v3-trino \
      -v "$HERE/trino/catalog:/etc/trino/catalog:ro" \
      -v "$HERE/trino/jvm.config:/etc/trino/jvm.config:ro" "$TRINO" >/dev/null
    for _ in $(seq 1 120); do
      podman exec he0v3-trino trino --execute 'SELECT 1' >/dev/null 2>&1 && break; sleep 2
    done
    ;;
  trino-sql)
    podman exec -i he0v3-trino trino --output-format=TSV_HEADER --file /dev/stdin < "$2" 2>&1
    ;;
  down)
    for c in he0v3-trino he0v3-spark he0v3-house he0v3-lakekeeper he0v3-postgres he0v3-rustfs; do
      podman rm -f -t 2 "$c" >/dev/null 2>&1 || true
    done
    podman pod rm -f "$POD" >/dev/null 2>&1 || true
    ;;
  *) echo "usage: $0 up|house|spark <file>|trino|down" >&2; exit 2 ;;
esac
