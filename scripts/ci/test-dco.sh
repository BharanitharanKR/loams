#!/usr/bin/env bash
set -euo pipefail
checker="$(cd "$(dirname "$0")" && pwd)/check-dco.sh"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/loams-agents"
mkdir -p "$cache"
fixture=$(mktemp -d "$cache/dco-fixture.XXXXXX")
trap 'python3 -c "import shutil, sys; shutil.rmtree(sys.argv[1])" "$fixture"' EXIT
git init -q "$fixture"
cd "$fixture"
git config user.name 'DCO fixture'
git config user.email fixture@example.com
git -c commit.gpgsign=false commit -q --allow-empty -m base
base=$(git rev-parse HEAD)
check() {
  local name=$1 email=$2 signoff=$3 expected=$4
  git -c commit.gpgsign=false commit -q --allow-empty --author "$name <$email>" -m fixture ${signoff:+-m "$signoff"}
  local head result=0
  head=$(git rev-parse HEAD)
  BASE=$base HEAD=$head "$checker" > "$fixture/result" 2>&1 || result=$?
  if [[ $expected == pass && $result != 0 || $expected == fail && $result == 0 ]]; then
    cat "$fixture/result"
    echo "unexpected DCO result for $name <$email>: $expected" >&2
    exit 1
  fi
  base=$head
}
check 'Human' human@example.com 'Signed-off-by: Human <human@example.com>' pass
check 'Human' human@example.com '' fail
check 'dependabot[bot]' '49699333+dependabot[bot]@users.noreply.github.com' 'Signed-off-by: dependabot[bot] <support@github.com>' pass
check 'dependabot[bot]' '49699333+dependabot[bot]@users.noreply.github.com' '' fail
check 'Human' human@example.com 'Signed-off-by: dependabot[bot] <support@github.com>' fail
check 'dependabot[bot]' impostor@example.com 'Signed-off-by: dependabot[bot] <support@github.com>' fail
remediate() { # by_name by_email signoff expected: remediates an unsigned Human commit
  local by_name=$1 by_email=$2 signoff=$3 expected=$4 target head result=0
  git -c commit.gpgsign=false commit -q --allow-empty --author 'Human <human@example.com>' -m fixture
  target=$(git rev-parse HEAD)
  git -c commit.gpgsign=false commit -q --allow-empty --author "$by_name <$by_email>" -m remediation \
    -m "I, $by_name <$by_email>, hereby add my Signed-off-by to this commit: $target" ${signoff:+-m "$signoff"}
  head=$(git rev-parse HEAD)
  BASE=$base HEAD=$head "$checker" > "$fixture/result" 2>&1 || result=$?
  if [[ $expected == pass && $result != 0 || $expected == fail && $result == 0 ]]; then
    cat "$fixture/result"
    echo "unexpected DCO result for a remediation by $by_name <$by_email>: $expected" >&2
    exit 1
  fi
  base=$head
}
remediate 'Human' human@example.com 'Signed-off-by: Human <human@example.com>' pass
remediate 'Human' human@example.com '' fail
remediate 'Other' other@example.com 'Signed-off-by: Other <other@example.com>' fail
echo 'DCO: signed humans/bot accepted; unsigned bot and mismatched identities rejected; same-author remediation accepted'
