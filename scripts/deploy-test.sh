#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/cicd_common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/deploy-test.sh <commitish> [--reason <text>]

Examples:
  scripts/deploy-test.sh develop
  scripts/deploy-test.sh 1a2b3c4d
  scripts/deploy-test.sh origin/main --reason "validate runtime refactor"
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

TARGET="${1:-}"
REASON="manual_test_deploy"

if [ -z "${TARGET}" ]; then
  usage
  exit 1
fi
shift

while [ "$#" -gt 0 ]; do
  case "$1" in
    --reason)
      [ "$#" -ge 2 ] || die "--reason requires a value"
      REASON="$2"
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
fetch_release_refs

COMMIT_SHA="$(resolve_commitish "${TARGET}")"
if git merge-base --is-ancestor "${COMMIT_SHA}" origin/main >/dev/null 2>&1; then
  SOURCE_BRANCH="main"
elif git merge-base --is-ancestor "${COMMIT_SHA}" origin/develop >/dev/null 2>&1; then
  SOURCE_BRANCH="develop"
else
  die "Commit ${COMMIT_SHA} is not contained in origin/main or origin/develop"
fi

REPO_SLUG="$(github_repo_slug)"
STARTED_AT="$(timestamp_utc)"

log "Dispatching manual test deploy"
log "Repository: ${REPO_SLUG}"
log "Commit: ${COMMIT_SHA}"
log "Source branch: ${SOURCE_BRANCH}"

dispatch_workflow \
  deploy-test-manual.yml \
  -f "commit_sha=${COMMIT_SHA}" \
  -f "reason=${REASON}"

RUN_ID="$(wait_for_workflow_dispatch_run "deploy-test-manual.yml" "${STARTED_AT}")"
RUN_URL="$(run_url "${RUN_ID}")"

log "GitHub Actions run: ${RUN_URL}"
watch_run_to_completion "${RUN_ID}"

log "Manual test deploy completed successfully"
log "Next step: scripts/run-test-validation.sh ${COMMIT_SHA}"
