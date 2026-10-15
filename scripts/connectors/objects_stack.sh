#!/usr/bin/env bash
# The `objects` family's CI preparation (CN2 Task 13, on Task 6's estate).
#
# Run after `docker compose -f deploy/fabric/compose.yaml --profile connectors-p2
# up -d --wait rustfs fake-gcs azurite sftp`. It waits for the four systems,
# creates the bucket and container the suites write to, and exports the
# endpoints, the fixed test credentials of deploy/fabric/compose.yaml and the
# SFTP host key to the steps that follow (`$GITHUB_ENV`), or prints them when
# run by hand.
set -euo pipefail

out="${GITHUB_ENV:-/dev/stdout}"
export LOAMS_RUSTFS_ENDPOINT=http://127.0.0.1:9000
export LOAMS_RUSTFS_ACCESS_KEY=loams
export LOAMS_RUSTFS_SECRET_KEY=loams-test-secret
export LOAMS_RUSTFS_BUCKET=loams
export LOAMS_FAKE_GCS_ENDPOINT=http://127.0.0.1:4443
export LOAMS_FAKE_GCS_BUCKET=loams
export LOAMS_AZURITE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1
export LOAMS_AZURITE_ACCOUNT=devstoreaccount1
# Azurite's well-known development key, not a secret.
export LOAMS_AZURITE_ACCESS_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
export LOAMS_AZURITE_CONTAINER=loams
export LOAMS_SFTP_HOST=127.0.0.1
export LOAMS_SFTP_PORT=2222
export LOAMS_SFTP_USER=loams
export LOAMS_SFTP_PASSWORD=loams-test
export LOAMS_SFTP_ROOT=upload

for _ in $(seq 1 60); do
  curl -s -o /dev/null "$LOAMS_RUSTFS_ENDPOINT" \
    && curl -s -o /dev/null "$LOAMS_FAKE_GCS_ENDPOINT/storage/v1/b" \
    && curl -s -o /dev/null http://127.0.0.1:10000 \
    && ssh-keyscan -p "$LOAMS_SFTP_PORT" "$LOAMS_SFTP_HOST" >/dev/null 2>&1 \
    && break
  sleep 2
done
AWS_ACCESS_KEY_ID="$LOAMS_RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$LOAMS_RUSTFS_SECRET_KEY" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "$LOAMS_RUSTFS_ENDPOINT" s3 mb "s3://$LOAMS_RUSTFS_BUCKET"
curl -sSf -X POST -H 'Content-Type: application/json' \
  -d "{\"name\":\"$LOAMS_FAKE_GCS_BUCKET\"}" "$LOAMS_FAKE_GCS_ENDPOINT/storage/v1/b?project=loams"
az storage container create --name "$LOAMS_AZURITE_CONTAINER" \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=$LOAMS_AZURITE_ACCOUNT;AccountKey=$LOAMS_AZURITE_ACCESS_KEY;BlobEndpoint=$LOAMS_AZURITE_ENDPOINT;"

{
  for v in RUSTFS_ENDPOINT RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY RUSTFS_BUCKET FAKE_GCS_ENDPOINT FAKE_GCS_BUCKET \
    AZURITE_ENDPOINT AZURITE_ACCOUNT AZURITE_ACCESS_KEY AZURITE_CONTAINER \
    SFTP_HOST SFTP_PORT SFTP_USER SFTP_PASSWORD SFTP_ROOT; do
    name="LOAMS_$v"
    echo "$name=${!name}"
  done
  # Newer OpenSSH prints its `# host:port SSH-2.0-...` banner comment on stdout.
  echo "LOAMS_SFTP_HOST_KEY=$(ssh-keyscan -p "$LOAMS_SFTP_PORT" -t ed25519 "$LOAMS_SFTP_HOST" 2>/dev/null | grep -v '^#' | head -n1)"
} >> "$out"
