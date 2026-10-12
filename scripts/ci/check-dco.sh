#!/usr/bin/env bash
set -euo pipefail
status=0
signed() { # sha email: the commit carries a Signed-off-by for email
  git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$1" | grep -qiF "<$2>"
}
# Retroactive sign-off (the DCO bot's remediation format): a signed-off
# commit in the range whose body has a line
#   I, Name <email>, hereby add my Signed-off-by to this commit: <sha>
# signs off <sha> when <email> is both its own and <sha>'s author email.
remediated=$(mktemp)
trap 'rm -f "$remediated"' EXIT
for sha in $(git rev-list --no-merges "$BASE..$HEAD"); do
  email=$(git log -1 --format='%ae' "$sha")
  signed "$sha" "$email" || continue
  git log -1 --format='%b' "$sha" |
    sed -n 's/^I, .* <\([^>]*\)>, hereby add my Signed-off-by to this commit: \([0-9a-f]\{40\}\)$/\2 \1/p' |
    while read -r target who; do
      if [[ "${who,,}" == "${email,,}" ]]; then echo "$target ${who,,}"; fi
    done >> "$remediated"
done
# --no-merges skips merge commits.
for sha in $(git rev-list --no-merges "$BASE..$HEAD"); do
  name=$(git log -1 --format='%an' "$sha")
  email=$(git log -1 --format='%ae' "$sha")
  # GitHub's Dependabot commits use its support address in DCO.
  if [[ "$name" == 'dependabot[bot]' && "$email" == '49699333+dependabot[bot]@users.noreply.github.com' ]] &&
      git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$sha" |
      grep -Fxq 'dependabot[bot] <support@github.com>'; then
    continue
  fi
  if ! signed "$sha" "$email" && ! grep -Fxq "$sha ${email,,}" "$remediated"; then
    echo "::error::commit $sha is missing 'Signed-off-by: ... <$email>' (use git commit -s)"
    status=1
  fi
done
exit $status
