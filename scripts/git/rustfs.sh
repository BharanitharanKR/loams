#!/usr/bin/env bash
# A throwaway RustFS for the loams-git suites and the WAL bench (plan GT1
# Task 11, T0-28). Uses docker or podman, whichever is present.
#
#   scripts/git/rustfs.sh start   # starts it, creates the bucket, prints the env
#   scripts/git/rustfs.sh env     # prints the env again
#   scripts/git/rustfs.sh stop    # stops it (the container is --rm: nothing is left)
#
# The container keeps its data inside itself: no named volume is created, so
# stopping it removes everything it made. Override the port, name or bucket
# with LOAMS_GIT_RUSTFS_PORT, LOAMS_GIT_RUSTFS_NAME and LOAMS_GIT_RUSTFS_BUCKET.
set -euo pipefail

IMAGE=docker.io/rustfs/rustfs:1.0.0
PORT=${LOAMS_GIT_RUSTFS_PORT:-9100}
NAME=${LOAMS_GIT_RUSTFS_NAME:-loams-git-rustfs}
BUCKET=${LOAMS_GIT_RUSTFS_BUCKET:-loams-git}
KEY=rustfsadmin
SECRET=rustfsadmin
ENDPOINT=http://127.0.0.1:${PORT}
# RustFS requires x-amz-content-sha256; this is the empty body's hash.
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

runtime() {
  if command -v docker >/dev/null 2>&1; then echo docker
  elif command -v podman >/dev/null 2>&1; then echo podman
  else echo "neither docker nor podman is installed" >&2; exit 1
  fi
}

print_env() {
  cat <<ENV
export LOAMS_TEST_S3_URL=${ENDPOINT}/${BUCKET}
export LOAMS_TEST_S3_KEY=${KEY}
export LOAMS_TEST_S3_SECRET=${SECRET}
export LOAMS_GIT_Q385_S3=${ENDPOINT}/${BUCKET}
export LOAMS_GIT_Q385_KEY=${KEY}
export LOAMS_GIT_Q385_SECRET=${SECRET}
# git-wal-bench --store s3://${BUCKET} --opt aws_endpoint=${ENDPOINT} --opt aws_access_key_id=${KEY} \\
#   --opt aws_secret_access_key=${SECRET} --opt aws_region=us-east-1 --opt aws_allow_http=true \\
#   --opt aws_virtual_hosted_style_request=false
ENV
}

case "${1:-}" in
  start)
    rt=$(runtime)
    "$rt" run -d --rm --name "$NAME" -p "127.0.0.1:${PORT}:9000" \
      -e RUSTFS_VOLUMES=/data -e RUSTFS_ADDRESS=0.0.0.0:9000 -e RUSTFS_CONSOLE_ENABLE=false \
      -e RUSTFS_ACCESS_KEY="$KEY" -e RUSTFS_SECRET_KEY="$SECRET" "$IMAGE" >/dev/null
    for _ in $(seq 1 60); do
      if curl -sf -o /dev/null -X PUT -H "x-amz-content-sha256: ${EMPTY_SHA}" \
           --aws-sigv4 aws:amz:us-east-1:s3 --user "${KEY}:${SECRET}" "${ENDPOINT}/${BUCKET}" \
         || curl -sf -o /dev/null -I -H "x-amz-content-sha256: ${EMPTY_SHA}" \
           --aws-sigv4 aws:amz:us-east-1:s3 --user "${KEY}:${SECRET}" "${ENDPOINT}/${BUCKET}"; then
        print_env
        exit 0
      fi
      sleep 1
    done
    echo "RustFS did not come up on ${ENDPOINT}" >&2
    exit 1
    ;;
  env) print_env ;;
  stop) "$(runtime)" stop "$NAME" >/dev/null ;;
  *) echo "usage: $0 start|env|stop" >&2; exit 2 ;;
esac
