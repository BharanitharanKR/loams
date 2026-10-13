#!/usr/bin/env bash
# API2 Task 19: a nightly failure of a wave 2 or 3 SDK job opens one issue per
# language, or comments on the one already open. ci.yml calls this from those
# jobs with `if: failure() && github.event_name == 'schedule'`, and gives only
# them `issues: write`. GH_TOKEN is the workflow's GITHUB_TOKEN.
#
# Usage: sdk-nightly-issue.sh <lang> (the sdks/<lang> directory name)
set -euo pipefail

lang="${1:?usage: sdk-nightly-issue.sh <lang>}"
: "${GH_TOKEN:?GH_TOKEN must be set}"
: "${GITHUB_REPOSITORY:?}" "${GITHUB_SERVER_URL:?}" "${GITHUB_RUN_ID:?}"

title="Nightly CI: sdk-${lang} fails"
run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
body="The nightly \`sdk-${lang}\` job failed on ${GITHUB_SHA:-the default branch}: ${run_url}"

# --search matches words, not the exact title, so filter on the title itself.
number="$(gh issue list --repo "$GITHUB_REPOSITORY" --state open \
  --search "in:title sdk-${lang}" --json number,title \
  --jq "map(select(.title == \"${title}\")) | .[0].number // empty")"

if [[ -n "$number" ]]; then
  gh issue comment "$number" --repo "$GITHUB_REPOSITORY" --body "$body"
else
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" \
    --body "${body}

This issue is opened and updated by \`scripts/ci/sdk-nightly-issue.sh\`. Close it when the job passes again; the next nightly failure opens a new one."
fi
