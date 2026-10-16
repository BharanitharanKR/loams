#!/usr/bin/env bash
# Checks release/upgrade-path.toml against the compiled migrations and the
# pin files (design §54 §11.1, UP1 Task 11). `--self-test` runs the checker's
# own tests (planted: an unlisted migration, a broken chain, a pin mismatch).
set -euo pipefail
exec python3 "$(dirname "$0")/upgrade_path.py" "${@:-check}"
