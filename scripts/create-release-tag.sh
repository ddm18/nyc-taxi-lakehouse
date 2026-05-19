#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/cicd_common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/create-release-tag.sh <release-tag> [--commit <commitish>]

Examples:
  scripts/create-release-tag.sh v1.2.3
  scripts/create-release-tag.sh v1.2.3 --commit 1a2b3c4d
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

RELEASE_TAG="${1:-}"
TARGET_COMMITISH="origin/main"

if [ -z "${RELEASE_TAG}" ]; then
  usage
  exit 1
fi
shift

while [ "$#" -gt 0 ]; do
  case "$1" in
    --commit)
      [ "$#" -ge 2 ] || die "--commit requires a value"
      TARGET_COMMITISH="$2"
      shift 2
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

cd_repo_root
ensure_tooling
ensure_gh_auth
require_clean_worktree
fetch_release_refs
validate_release_tag "${RELEASE_TAG}"

TARGET_COMMIT_SHA="$(resolve_commitish "${TARGET_COMMITISH}")"
ensure_commit_in_branch "${TARGET_COMMIT_SHA}" main

if git rev-parse "${RELEASE_TAG}^{tag}" >/dev/null 2>&1 || git rev-parse "${RELEASE_TAG}^{commit}" >/dev/null 2>&1; then
  die "Release tag ${RELEASE_TAG} already exists locally"
fi
if git ls-remote --tags origin "refs/tags/${RELEASE_TAG}" | grep -q .; then
  die "Release tag ${RELEASE_TAG} already exists on origin"
fi

git tag -a "${RELEASE_TAG}" "${TARGET_COMMIT_SHA}" -m "Release ${RELEASE_TAG}"
git push origin "refs/tags/${RELEASE_TAG}"

log "Created and pushed release tag ${RELEASE_TAG}"
log "Commit: ${TARGET_COMMIT_SHA}"
log "Next step: scripts/deploy-prod.sh ${RELEASE_TAG}"
