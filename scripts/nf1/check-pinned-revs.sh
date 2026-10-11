#!/usr/bin/env bash
# Fails if a fork revision that Loams pins could be garbage-collected (NF1
# Task 3, Review Focus 4).
#
#   scripts/nf1/check-pinned-revs.sh [root]
#
# Finds every `git = "https://github.com/ostrium-labs/<repo>"` dependency with
# a `rev` in the Cargo.toml files under root (tracked files when root is a git
# checkout; target/ is skipped). A rev passes when it is reachable from the
# fork's protected branch, which no push can rewind or delete (rulesets, Task
# 3), or when it is the target of a `loams-*` tag, which no push can move or
# delete:
#
#   loams-postgres (also its old name, neon)   main
#   rust-postgres, azure-sdk-for-rust          loams/neon
#   framed-websockets, tokio-epoll-uring,
#   pg_session_jwt, autoscaling                loams/main
#
# Reachable means `gh api repos/ostrium-labs/<repo>/compare/<branch>...<rev>`
# reports `behind` or `identical` (the rev is the branch or an ancestor of it).
# Other ostrium-labs repositories are not NF1 forks; they are listed as `skip`.
# Any gh error fails the rev. GH overrides the gh binary (the tests use it).
set -euo pipefail

root=${1:-.}
gh=${GH:-gh}

branch_of() {
  case $1 in
    loams-postgres) echo main ;;
    rust-postgres | azure-sdk-for-rust) echo loams/neon ;;
    framed-websockets | tokio-epoll-uring | pg_session_jwt | autoscaling) echo loams/main ;;
    *) return 1 ;;
  esac
}

if git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  list() { git -C "$root" ls-files -z -- 'Cargo.toml' '*/Cargo.toml'; }
else
  list() { (cd "$root" && find . -name Cargo.toml -not -path '*/target/*' -not -path './.git/*' -printf '%P\0'); }
fi

# "<repo> <rev>" for each pinned dependency: an inline table on one line, or
# the git and rev keys of one [dependencies.x] style table.
pins=$(list | { grep -zv -e '^target/' -e '/target/' || true; } | (cd "$root" && xargs -0r awk '
  function flush() { if (sgit != "" && srev != "") print sgit, srev; sgit = ""; srev = "" }
  function repo(s) { if (match(s, /git *= *"https:\/\/github\.com\/ostrium-labs\/[A-Za-z0-9_.-]+"/)) {
      s = substr(s, RSTART, RLENGTH); sub(/.*ostrium-labs\//, "", s); sub(/"$/, "", s); sub(/\.git$/, "", s); return s }
    return "" }
  function rev(s) { if (match(s, /rev *= *"[0-9a-f]+"/)) { s = substr(s, RSTART, RLENGTH); sub(/.*= *"/, "", s); sub(/"$/, "", s); return s }
    return "" }
  FNR == 1 { flush() }
  /^[ \t]*\[/ { flush(); next }
  { g = repo($0); r = rev($0)
    if (g != "" && r != "") print g, r
    else if (/^[ \t]*git[ \t]*=/) sgit = g
    else if (/^[ \t]*rev[ \t]*=/) srev = r }
  END { flush() }
') | sed 's/^neon /loams-postgres /' | sort -u)

declare -A tags_of
tag_targets() { # "<commit sha> <tag name>" for each loams-* tag of $1
  local repo=$1 type sha ref out
  out=$("$gh" api "repos/ostrium-labs/$repo/git/matching-refs/tags/loams-" --paginate \
    -q '.[] | .object.type + " " + .object.sha + " " + .ref') || return 1
  while read -r type sha ref; do
    [ -n "$sha" ] || continue
    while [ "$type" = tag ]; do
      read -r type sha < <("$gh" api "repos/ostrium-labs/$repo/git/tags/$sha" -q '.object.type + " " + .object.sha') || return 1
    done
    echo "$sha ${ref#refs/tags/}"
  done <<<"$out"
}

fail=0
while read -r repo rev; do
  [ -n "$repo" ] || continue
  if ! branch=$(branch_of "$repo"); then
    echo "skip $repo $rev: not an NF1 fork"
    continue
  fi
  if status=$("$gh" api "repos/ostrium-labs/$repo/compare/$branch...$rev" -q .status 2>/dev/null); then
    case $status in
      behind | identical)
        echo "ok   $repo $rev ($status $branch)"
        continue
        ;;
    esac
  else
    status="compare failed"
  fi
  if [ -z "${tags_of[$repo]+x}" ]; then
    tags_of[$repo]=$(tag_targets "$repo") || tags_of[$repo]=""
  fi
  tag=$(awk -v r="$rev" 'index($1, r) == 1 { print $2; exit }' <<<"${tags_of[$repo]}")
  if [ -n "$tag" ]; then
    echo "ok   $repo $rev (tag $tag)"
  else
    echo "FAIL $repo $rev: $status against $branch, and no loams-* tag points at it"
    fail=1
  fi
done <<<"$pins"

exit $fail
