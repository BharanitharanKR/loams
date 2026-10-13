#!/usr/bin/env bash
# NF1 Task 12: verify the signatures, attestations and sources of every image
# Loams pins that Loams Postgres built. See scripts/nf1/verify_pins.py.
#
#   verify-pins.sh [pin file]   (default deploy/pins/loams-postgres-images.toml)
#
# Needs cosign, gh (authenticated: GH_TOKEN) and oras, or COSIGN, GH and ORAS.
# Test: scripts/nf1/tests/test_verify_pins.sh.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
exec python3 -I "$here/verify_pins.py" ${1:+--pins "$1"}
