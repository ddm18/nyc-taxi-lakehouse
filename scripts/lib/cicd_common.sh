#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[cicd] %s\n' "$*"
}

die() {
  printf '[cicd] %s\n' "$*" >&2
  exit 1
}

repo_root() {
  git rev-parse --show-toplevel
}

cd_repo_root() {
  cd "$(repo_root)"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

ensure_tooling() {
  require_command gh
  require_command git
  require_command jq
}

ensure_gh_auth() {
  gh auth status >/dev/null 2>&1 || die "GitHub CLI is not authenticated"
}

require_clean_worktree() {
  if [ -n "$(git status --porcelain)" ]; then
    die "Working tree must be clean for this operation"
  fi
}

github_repo_slug() {
  local remote_url
  remote_url="$(git remote get-url origin 2>/dev/null)" || die "Git remote origin is not configured"
  case "${remote_url}" in
    git@github.com:*.git)
      printf '%s\n' "${remote_url#git@github.com:}" | sed 's/\.git$//'
      ;;
    https://github.com/*)
      printf '%s\n' "${remote_url#https://github.com/}" | sed 's/\.git$//'
      ;;
    *)
      die "Unsupported origin remote format: ${remote_url}"
      ;;
  esac
}

fetch_release_refs() {
  git fetch origin main develop --tags >/dev/null 2>&1
}

resolve_commitish() {
  git rev-parse "$1^{commit}"
}

ensure_commit_in_branch() {
  local commit_sha="$1"
  local branch_name="$2"
  git merge-base --is-ancestor "${commit_sha}" "origin/${branch_name}" >/dev/null 2>&1 \
    || die "Commit ${commit_sha} is not contained in origin/${branch_name}"
}

validate_release_tag() {
  local release_tag="$1"
  [[ "${release_tag}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "Release tag must match vX.Y.Z"
}

timestamp_utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

dispatch_workflow() {
  local workflow_file="$1"
  local workflow_ref="${WORKFLOW_REF:-main}"
  shift
  gh workflow run "${workflow_file}" --ref "${workflow_ref}" "$@"
}

wait_for_workflow_dispatch_run() {
  local workflow_file="$1"
  local started_at="$2"
  local run_id=""
  local attempts=0

  while [ "${attempts}" -lt 30 ]; do
    run_id="$(
      gh run list \
        --workflow "${workflow_file}" \
        --event workflow_dispatch \
        --limit 20 \
        --json databaseId,createdAt \
      | jq -r --arg started_at "${started_at}" '
          map(select(.createdAt >= $started_at))
          | sort_by(.createdAt)
          | last
          | .databaseId // empty
        '
    )"
    if [ -n "${run_id}" ]; then
      printf '%s\n' "${run_id}"
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  die "Could not discover the dispatched workflow run for ${workflow_file}"
}

watch_run_to_completion() {
  gh run watch "$1" --exit-status
}

run_url() {
  gh run view "$1" --json url --jq '.url'
}
