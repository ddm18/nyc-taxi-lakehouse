#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/cicd_common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/deploy-prod.sh <release-tag> [--reason <text>]

Examples:
  scripts/deploy-prod.sh v1.2.3
  scripts/deploy-prod.sh v1.2.3 --reason "promote validated main release"
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

RELEASE_TAG="${1:-}"
REASON="manual_prod_deploy"

if [ -z "${RELEASE_TAG}" ]; then
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
validate_release_tag "${RELEASE_TAG}"

COMMIT_SHA="$(git rev-list -n 1 "${RELEASE_TAG}")"
[ -n "${COMMIT_SHA}" ] || die "Release tag ${RELEASE_TAG} does not exist"
ensure_commit_in_branch "${COMMIT_SHA}" main

REPO_SLUG="$(github_repo_slug)"
STARTED_AT="$(timestamp_utc)"

log "Dispatching prod deploy"
log "Repository: ${REPO_SLUG}"
log "Release tag: ${RELEASE_TAG}"
log "Commit: ${COMMIT_SHA}"

dispatch_workflow \
  deploy-prod.yml \
  -f "release_tag=${RELEASE_TAG}" \
  -f "release_reason=${REASON}"

RUN_ID="$(wait_for_workflow_dispatch_run "deploy-prod.yml" "${STARTED_AT}")"
RUN_URL="$(run_url "${RUN_ID}")"

log "GitHub Actions run: ${RUN_URL}"
watch_run_to_completion "${RUN_ID}"

log "Prod deploy completed successfully"
log "GitHub Release URL: https://github.com/${REPO_SLUG}/releases/tag/${RELEASE_TAG}"
