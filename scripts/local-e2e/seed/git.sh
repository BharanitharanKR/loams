#!/usr/bin/env bash
# Loams Git: a small repository made here (no clone), registered with the
# engine's RepoService and pushed with git-remote-loams straight to the
# stack's bucket (RustFS on 127.0.0.1:19000), then fetched back into a fresh
# clone. git-remote-loams is built on the host (the shared cargo target) if
# it is not on PATH.
#
#   scripts/local-e2e/seed/git.sh
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/../env.sh"
root="$(cd "$here/../../.." && pwd)"
engine="${E2E_ENGINE:-http://127.0.0.1:18080}"
log="$E2E_HOME/logs/seed-git.txt"
: > "$log"
say() { echo "$(date +%T) $*" | tee -a "$log"; }

helper="$(command -v git-remote-loams || true)"
if [[ -z "$helper" ]]; then
  target="$(cd "$root" && cargo metadata --format-version 1 --no-deps | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
  helper="$target/debug/git-remote-loams"
  touch "$root/crates/loams-git-remote/src/main.rs"
  (cd "$root" && nice cargo build -j 4 -p loams-git-remote --bin git-remote-loams) >> "$log" 2>&1 || { say "FAIL build git-remote-loams"; exit 1; }
fi
bindir="$E2E_HOME/git-bin"; mkdir -p "$bindir"; ln -sf "$helper" "$bindir/git-remote-loams"
export PATH="$bindir:$PATH"

src="$E2E_HOME/git-src/e2e-sample"
if [[ ! -d "$src/.git" ]]; then
  mkdir -p "$src"
  git -C "$src" init -q -b main
  cat > "$src/README.md" <<'MD'
# e2e-sample

A small repository the local-e2e seed pushes to Loams Git with git-remote-loams.
MD
  cp "$root/scripts/local-e2e/seed/"*.py "$src/"
  git -C "$src" add -A
  git -C "$src" -c user.name='Loams e2e' -c user.email=e2e@localhost commit -q -m 'Initial commit: seed scripts'
  printf '\n## History\n\nA second commit, so the log has two entries.\n' >> "$src/README.md"
  git -C "$src" -c user.name='Loams e2e' -c user.email=e2e@localhost commit -q -am 'README: history'
  git -C "$src" checkout -q -b feature/live-check
  echo 'print("live")' > "$src/live_check.py"
  git -C "$src" add -A && git -C "$src" -c user.name='Loams e2e' -c user.email=e2e@localhost commit -q -m 'Add live_check'
  git -C "$src" checkout -q main
fi

# The catalog entry (RepoService needs the engine's `git` feature).
reply="$(curl -s -X POST "$engine/loams.repos.v1.RepoService/CreateRepo" -H 'content-type: application/json' \
  -H 'connect-protocol-version: 1' -d '{"namespace": "e2e", "name": "e2e-sample", "idempotencyKey": "e2e-sample"}')"
say "CreateRepo: ${reply:0:300}"
reply="$(curl -s -X POST "$engine/loams.repos.v1.RepoService/ListRepos" -H 'content-type: application/json' \
  -H 'connect-protocol-version: 1' -d '{"namespace": "e2e"}')"
say "ListRepos: ${reply:0:300}"

export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin AWS_REGION=us-east-1 \
  AWS_ENDPOINT=http://127.0.0.1:19000 AWS_ALLOW_HTTP=true
remote="loams::s3://loams-engine/stack/ns/e2e/repos/e2e-sample"
git -C "$src" remote remove loams 2>/dev/null
git -C "$src" remote add loams "$remote"
status=0
if git -C "$src" push loams main feature/live-check >> "$log" 2>&1; then say "PASS push main feature/live-check"; else say "FAIL push (see $log)"; status=1; fi
clone="$E2E_HOME/git-src/e2e-sample-clone"; rm -rf "$clone"
if git clone -q "$remote" "$clone" >> "$log" 2>&1; then
  say "PASS clone: $(git -C "$clone" log --oneline | wc -l) commits on $(git -C "$clone" branch --show-current)"
else say "FAIL clone (see $log)"; status=1; fi
exit $status
