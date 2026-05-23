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
POLL_SECONDS=30
MAX_WAIT_SECONDS=2700

aws lambda get-function --function-name "${CONTROL_PLANE_LAMBDA_NAME}" >/dev/null
aws s3api head-bucket --bucket "${DATA_BUCKET_NAME}" >/dev/null
aws s3api head-bucket --bucket "${ARTIFACT_BUCKET_NAME}" >/dev/null

STATUS="failed"
REPORT_S3_URI=""
RUN_ID=""
LANDING_ROOT=""
ERROR_MESSAGE=""

invoke_control_plane() {
  local request_json="$1"
  local response_json="$2"
  local metadata_json

  set +e
  metadata_json="$(aws lambda invoke \
    --function-name "${CONTROL_PLANE_LAMBDA_NAME}" \
    --cli-connect-timeout 60 \
    --cli-read-timeout 900 \
    --cli-binary-format raw-in-base64-out \
    --payload "file://${request_json}" \
    "${response_json}")"
  local lambda_exit_code=$?
  set -e

  if [ "${lambda_exit_code}" -ne 0 ]; then
    ERROR_MESSAGE="Lambda invocation failed with exit code ${lambda_exit_code}"
    return 1
  fi

  local function_error
  function_error="$(printf '%s' "${metadata_json}" | jq -r '.FunctionError // empty')"
  if [ -n "${function_error}" ]; then
    ERROR_MESSAGE="$(jq -r '.errorMessage // "Control-plane lambda reported an error"' "${response_json}")"
    return 1
  fi

  return 0
}

SHORT_SHA="$(printf '%.12s' "${COMMIT_SHA}")"
RUN_SUFFIX="$(date -u +%Y%m%dT%H%M%SZ)"
if [ -n "${GITHUB_RUN_ID:-}" ]; then
  RUN_SUFFIX="${GITHUB_RUN_ID}_${RUN_SUFFIX}"
fi
RUN_ID="validation__${SHORT_SHA}__${RUN_SUFFIX}"
LANDING_ROOT="s3://${DATA_BUCKET_NAME}/test-runs/${COMMIT_SHA}/${RUN_ID}"
REPORT_KEY="control-plane/test/${COMMIT_SHA}/${RUN_ID}.json"

TRIGGER_REQUEST_JSON="$(mktemp)"
cat > "${TRIGGER_REQUEST_JSON}" <<EOF
{
  "operation": "trigger",
  "dag_id": "nyc_taxi_pipeline",
  "run_id": "${RUN_ID}",
  "conf": {
    "service": "yellow",
    "year": 2018,
    "month": 1,
    "landing_root": "${LANDING_ROOT}",
    "transformation_version": "${COMMIT_SHA}"
  },
  "report_key": "${REPORT_KEY}"
}
EOF

TRIGGER_RESPONSE_JSON="$(mktemp)"
if invoke_control_plane "${TRIGGER_REQUEST_JSON}" "${TRIGGER_RESPONSE_JSON}"; then
  RUN_ID="$(jq -r '.run_id // empty' "${TRIGGER_RESPONSE_JSON}")"
  if [ -z "${RUN_ID}" ]; then
    ERROR_MESSAGE="Control-plane trigger response did not contain a run_id"
  fi
fi

if [ -z "${ERROR_MESSAGE}" ]; then
  DEADLINE_EPOCH="$(( $(date +%s) + MAX_WAIT_SECONDS ))"
  while [ "$(date +%s)" -le "${DEADLINE_EPOCH}" ]; do
    STATUS_REQUEST_JSON="$(mktemp)"
    cat > "${STATUS_REQUEST_JSON}" <<EOF
{
  "operation": "status",
  "dag_id": "nyc_taxi_pipeline",
  "run_id": "${RUN_ID}",
  "report_key": "${REPORT_KEY}"
}
EOF

    STATUS_RESPONSE_JSON="$(mktemp)"
    if ! invoke_control_plane "${STATUS_REQUEST_JSON}" "${STATUS_RESPONSE_JSON}"; then
      rm -f "${STATUS_REQUEST_JSON}" "${STATUS_RESPONSE_JSON}"
      break
    fi

    DAG_STATE="$(jq -r '.state // "unknown"' "${STATUS_RESPONSE_JSON}")"
    REPORT_S3_URI="$(jq -r '.report_s3_uri // empty' "${STATUS_RESPONSE_JSON}")"
    rm -f "${STATUS_REQUEST_JSON}" "${STATUS_RESPONSE_JSON}"

    case "${DAG_STATE}" in
      success)
        STATUS="success"
        break
        ;;
      failed)
        STATUS="failed"
        ERROR_MESSAGE="Airflow DAG run ${RUN_ID} finished with state failed"
        break
        ;;
      queued|running)
        sleep "${POLL_SECONDS}"
        ;;
      *)
        STATUS="failed"
        ERROR_MESSAGE="Airflow DAG run ${RUN_ID} returned unexpected state ${DAG_STATE}"
        break
        ;;
    esac
  done
fi

if [ "${STATUS}" != "success" ] && [ -z "${ERROR_MESSAGE}" ]; then
  ERROR_MESSAGE="Validation timed out waiting for DAG run ${RUN_ID} to complete"
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
  --arg landing_root "${LANDING_ROOT}" \
  --arg error_message "${ERROR_MESSAGE}" \
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
    landing_root: $landing_root,
    error_message: $error_message,
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
