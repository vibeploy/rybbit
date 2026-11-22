#!/usr/bin/env bash
# Rebuild the vibeploy patch set on top of the newest upstream release tag.
#
# The current fork branch is the highest `vibeploy/vX.Y.Z` branch on ORIGIN;
# its patches are the commits between upstream tag `vX.Y.Z` and the branch tip.
# On success HEAD is a detached commit with every patch cherry-picked onto the
# target tag. This script never pushes and never sees a token; the caller does
# that after reading the outputs.
#
# Outputs (GITHUB_OUTPUT): status = up_to_date | pending_review | ready | failed,
# plus target_tag, target_sha, base_tag, fork_branch, new_branch, sync_branch.
set -euo pipefail

ORIGIN=${ORIGIN_REMOTE:-origin}
UPSTREAM=${UPSTREAM_REMOTE:-upstream}
TARGET_TAG=${TARGET_TAG:-}
OUT=${GITHUB_OUTPUT:-/dev/null}
SUMMARY=${GITHUB_STEP_SUMMARY:-/dev/stderr}
REPORT=${REPORT_FILE:-/dev/null}

release_re='^v[0-9]+\.[0-9]+\.[0-9]+$'

out() { printf '%s=%s\n' "$1" "$2" >>"$OUT"; }
note() { printf '%s\n' "$*" >>"$SUMMARY"; }
fail() {
  note "**Sync stopped:** $*"
  out status failed
  echo "error: $*" >&2
  exit 1
}
newer() { # newer A B: true when A sorts strictly after B
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]
}

git fetch --quiet --prune --no-tags "$ORIGIN" \
  "+refs/heads/vibeploy/*:refs/remotes/$ORIGIN/vibeploy/*" \
  "+refs/heads/sync/*:refs/remotes/$ORIGIN/sync/*"

fork_branch=$(git for-each-ref --format='%(refname:lstrip=3)' "refs/remotes/$ORIGIN/vibeploy/" |
  grep -E '^vibeploy/v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n1 || true)
[ -n "$fork_branch" ] || fail "no vibeploy/vX.Y.Z branch on $ORIGIN"
base_tag=${fork_branch#vibeploy/}
fork_ref="refs/remotes/$ORIGIN/$fork_branch"
out fork_branch "$fork_branch"
out base_tag "$base_tag"

# Release tags as upstream publishes them right now; never trust a local tag.
upstream_tags=$(git ls-remote --tags --refs "$UPSTREAM" | sed 's#^\([0-9a-f]*\)[[:space:]]*refs/tags/#\1 #' |
  awk -v re="$release_re" '$2 ~ re')
[ -n "$upstream_tags" ] || fail "could not list release tags on $UPSTREAM"

tag_sha() { printf '%s\n' "$upstream_tags" | awk -v t="$1" '$2 == t { print $1 }'; }

if [ -n "$TARGET_TAG" ]; then
  [[ "$TARGET_TAG" =~ $release_re ]] || fail "target tag '$TARGET_TAG' is not a vX.Y.Z release tag"
  target=$TARGET_TAG
  [ -n "$(tag_sha "$target")" ] || fail "upstream has no tag $target"
else
  target=$(printf '%s\n' "$upstream_tags" | awk '{ print $2 }' | sort -V | tail -n1)
fi
out target_tag "$target"

if ! newer "$target" "$base_tag"; then
  note "Up to date: \`$fork_branch\` is based on $base_tag; upstream offers $target."
  out status up_to_date
  exit 0
fi

new_branch="vibeploy/$target"
sync_branch="sync/vibeploy-$target"
out new_branch "$new_branch"
out sync_branch "$sync_branch"

# Never overwrite a sync branch a person may already be fixing up.
if git show-ref --verify --quiet "refs/remotes/$ORIGIN/$sync_branch"; then
  note "Pending review: \`$sync_branch\` already exists; merge or delete it to sync again."
  out status pending_review
  exit 0
fi
if git ls-remote --exit-code --heads "$ORIGIN" "$new_branch" >/dev/null; then
  fail "\`$new_branch\` exists on $ORIGIN without \`$sync_branch\`; it should have been picked as the fork branch"
fi

git fetch --quiet --no-tags "$UPSTREAM" "+refs/tags/$base_tag:refs/tags/$base_tag" "+refs/tags/$target:refs/tags/$target"
target_sha=$(git rev-parse "refs/tags/$target^{commit}")
announced=$(tag_sha "$target")
# An annotated tag lists the tag object; compare commits, not objects.
[ "$(git rev-parse "$announced^{commit}" 2>/dev/null || true)" = "$target_sha" ] ||
  fail "fetched $target does not match what $UPSTREAM announced"
out target_sha "$target_sha"

git merge-base --is-ancestor "refs/tags/$base_tag" "$fork_ref" ||
  fail "\`$fork_branch\` does not contain upstream $base_tag; cannot tell which commits are ours"

# A PR merge onto the untouched base is fine (its tree equals the PR head).
# Anything else carries changes no cherry-pick would replay.
for m in $(git rev-list --merges "refs/tags/$base_tag..$fork_ref"); do
  [ "$(git rev-parse "$m^{tree}")" = "$(git rev-parse "$m^2^{tree}")" ] ||
    fail "merge commit $(git rev-parse --short "$m") on \`$fork_branch\` carries its own changes; rebase it into plain commits first"
done

patches=$(git rev-list --reverse --no-merges "refs/tags/$base_tag..$fork_ref")
[ -n "$patches" ] || fail "\`$fork_branch\` has no commits on top of $base_tag"

git -c advice.detachedHead=false checkout --quiet --detach "$target_sha"

applied=() dropped=()
for c in $patches; do
  subject=$(git log -1 --format='%h %s' "$c")
  if git cherry-pick -x "$c" >/dev/null 2>&1; then
    applied+=("$subject")
    continue
  fi
  conflicts=$(git diff --name-only --diff-filter=U)
  if [ -z "$conflicts" ] && git diff --cached --quiet; then
    # Upstream already contains this change; the pick is empty.
    git cherry-pick --skip >/dev/null 2>&1
    dropped+=("$subject")
    continue
  fi
  git cherry-pick --abort >/dev/null 2>&1 || true
  note "### Conflict replaying \`$fork_branch\` onto $target"
  note ""
  note "Stopped at \`$subject\` after ${#applied[@]} of $(printf '%s\n' "$patches" | wc -l | tr -d ' ') patches applied cleanly."
  note ""
  note "Conflicting files:"
  note ""
  printf '%s\n' "$conflicts" | sed 's/^/- `/; s/$/`/' >>"$SUMMARY"
  note ""
  note "Resolve locally:"
  note ""
  note '```bash'
  note "git fetch upstream tag $target && git fetch origin $fork_branch"
  note "git switch -c $sync_branch $target"
  note "git cherry-pick -x refs/tags/$base_tag..origin/$fork_branch"
  note "git push origin $target^{commit}:refs/heads/$new_branch $sync_branch"
  note '```'
  fail "cherry-pick conflict at $subject"
done

{
  echo "Rebuilds \`$fork_branch\` on upstream [$target](https://github.com/rybbit-io/rybbit/releases/tag/$target)."
  echo ""
  echo "- Upstream changes: https://github.com/rybbit-io/rybbit/compare/$base_tag...$target"
  echo "- Base branch \`$new_branch\` is exactly upstream $target, so this diff is only our patch set."
  echo ""
  echo "Applied (${#applied[@]}):"
  echo ""
  for s in "${applied[@]}"; do echo "- $s"; done
  if [ "${#dropped[@]}" -gt 0 ]; then
    echo ""
    echo "Dropped because upstream already contains them (${#dropped[@]}):"
    echo ""
    for s in "${dropped[@]}"; do echo "- $s"; done
  fi
  echo ""
  echo "Merge with **Rebase and merge** so the next sync can replay plain commits. Nothing is built or deployed automatically."
} | tee -a "$SUMMARY" >"$REPORT"

out status ready
