#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/cicd_common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/run-validation-pipeline.sh --env test --commit-sha <sha> [--ci-run-url <url>] [--workflow-run-url <url>] [--reason <text>]
EOF
}

ENVIRONMENT_NAME=""
COMMIT_SHA=""
CI_RUN_URL=""
WORKFLOW_RUN_URL=""
VALIDATION_REASON="test_validation"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --env)
      ENVIRONMENT_NAME="${2:-}"
      shift 2
      ;;
    --commit-sha)
      COMMIT_SHA="${2:-}"
      shift 2
      ;;
    --ci-run-url)
      CI_RUN_URL="${2:-}"
      shift 2
      ;;
    --workflow-run-url)
      WORKFLOW_RUN_URL="${2:-}"
      shift 2
      ;;
    --reason)
      VALIDATION_REASON="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[ "${ENVIRONMENT_NAME}" = "test" ] || die "Only --env test is currently supported"
[ -n "${COMMIT_SHA}" ] || die "--commit-sha is required"

require_command aws
require_command jq

PROJECT_NAME="nyc-data-platform"
NAME_PREFIX="${PROJECT_NAME}-${ENVIRONMENT_NAME}"
DATA_BUCKET_NAME="${NAME_PREFIX}"
ARTIFACT_BUCKET_NAME="${NAME_PREFIX}-artifacts"
CONTROL_PLANE_LAMBDA_NAME="${NAME_PREFIX}-control-plane"
TEST_RUN_RECORD_URI="s3://${ARTIFACT_BUCKET_NAME}/releases/test-runs/${COMMIT_SHA}.json"

aws lambda get-function --function-name "${CONTROL_PLANE_LAMBDA_NAME}" >/dev/null
aws s3api head-bucket --bucket "${DATA_BUCKET_NAME}" >/dev/null
aws s3api head-bucket --bucket "${ARTIFACT_BUCKET_NAME}" >/dev/null

REQUEST_JSON="$(mktemp)"
cat > "${REQUEST_JSON}" <<EOF
{
  "dag_id": "nyc_taxi_pipeline",
  "conf": {
    "service": "yellow",
    "year": 2018,
    "month": 1,
    "landing_root": "s3://${DATA_BUCKET_NAME}/test",
    "transformation_version": "${COMMIT_SHA}"
  },
  "timeout_seconds": 840,
  "poll_seconds": 30,
  "report_key": "control-plane/test/${COMMIT_SHA}.json"
}
EOF

RESPONSE_JSON="$(mktemp)"
set +e
aws lambda invoke \
  --function-name "${CONTROL_PLANE_LAMBDA_NAME}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${REQUEST_JSON}" \
  "${RESPONSE_JSON}" >/dev/null
LAMBDA_EXIT_CODE=$?
set -e

STATUS="failed"
REPORT_S3_URI=""
RUN_ID=""
if [ "${LAMBDA_EXIT_CODE}" -eq 0 ]; then
  STATUS="$(jq -r '.state // "failed"' "${RESPONSE_JSON}")"
  REPORT_S3_URI="$(jq -r '.report_s3_uri // empty' "${RESPONSE_JSON}")"
  RUN_ID="$(jq -r '.run_id // empty' "${RESPONSE_JSON}")"
fi

RESULT_JSON="$(mktemp)"
jq -n \
  --arg environment_name "${ENVIRONMENT_NAME}" \
  --arg commit_sha "${COMMIT_SHA}" \
  --arg status "${STATUS}" \
  --arg reason "${VALIDATION_REASON}" \
  --arg ci_run_url "${CI_RUN_URL}" \
  --arg workflow_run_url "${WORKFLOW_RUN_URL}" \
  --arg report_s3_uri "${REPORT_S3_URI}" \
  --arg dag_run_id "${RUN_ID}" \
  --arg executed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{
    environment_name: $environment_name,
    commit_sha: $commit_sha,
    status: $status,
    reason: $reason,
    ci_run_url: $ci_run_url,
    workflow_run_url: $workflow_run_url,
    report_s3_uri: $report_s3_uri,
    dag_run_id: $dag_run_id,
    executed_at: $executed_at
  }' > "${RESULT_JSON}"

aws s3 cp "${RESULT_JSON}" "${TEST_RUN_RECORD_URI}" >/dev/null

if [ "${STATUS}" != "success" ]; then
  die "Validation run failed for ${COMMIT_SHA}. Record written to ${TEST_RUN_RECORD_URI}"
fi

log "Validation run completed successfully"
log "Record: ${TEST_RUN_RECORD_URI}"
if [ -n "${REPORT_S3_URI}" ]; then
  log "Control-plane report: ${REPORT_S3_URI}"
fi
