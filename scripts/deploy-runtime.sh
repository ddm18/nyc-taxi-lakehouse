#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/cicd_common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/deploy-runtime.sh --env test|prod --commit-sha <sha> --image-uri <uri@digest> [--ci-run-url <url>] [--release-tag <tag>] [--reason <text>] [--bundle-dir <path>]
EOF
}

ENVIRONMENT_NAME=""
COMMIT_SHA=""
IMAGE_URI=""
CI_RUN_URL=""
RELEASE_TAG=""
DEPLOY_REASON="runtime_deploy"
BUNDLE_DIR=".ci-input/deploy-bundle"

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
    --image-uri)
      IMAGE_URI="${2:-}"
      shift 2
      ;;
    --ci-run-url)
      CI_RUN_URL="${2:-}"
      shift 2
      ;;
    --release-tag)
      RELEASE_TAG="${2:-}"
      shift 2
      ;;
    --reason)
      DEPLOY_REASON="${2:-}"
      shift 2
      ;;
    --bundle-dir)
      BUNDLE_DIR="${2:-}"
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

[ -n "${ENVIRONMENT_NAME}" ] || die "--env is required"
[ -n "${COMMIT_SHA}" ] || die "--commit-sha is required"
[ -n "${IMAGE_URI}" ] || die "--image-uri is required"
[ -d "${BUNDLE_DIR}" ] || die "Bundle directory does not exist: ${BUNDLE_DIR}"
[ -d "${BUNDLE_DIR}/dags" ] || die "Missing DAG bundle in ${BUNDLE_DIR}/dags"
[ -f "${BUNDLE_DIR}/mwaa-requirements.txt" ] || die "Missing ${BUNDLE_DIR}/mwaa-requirements.txt"
[ -f "${BUNDLE_DIR}/plugins.zip" ] || die "Missing ${BUNDLE_DIR}/plugins.zip"

require_command aws
require_command jq

case "${ENVIRONMENT_NAME}" in
  test|prod)
    ;;
  *)
    die "--env must be test or prod"
    ;;
esac

if [ "${ENVIRONMENT_NAME}" = "prod" ] && [ -z "${RELEASE_TAG}" ]; then
  die "--release-tag is required for --env prod"
fi

PROJECT_NAME="nyc-data-platform"
NAME_PREFIX="${PROJECT_NAME}-${ENVIRONMENT_NAME}"
DATA_BUCKET_NAME="${NAME_PREFIX}"
ARTIFACT_BUCKET_NAME="${NAME_PREFIX}-artifacts"
MWAA_ENVIRONMENT_NAME="${NAME_PREFIX}-mwaa"
CONTROL_PLANE_LAMBDA_NAME="${NAME_PREFIX}-control-plane"
ECS_CLUSTER_NAME="${NAME_PREFIX}-cluster"
ECS_TASK_FAMILY="${NAME_PREFIX}-pipeline"
ECS_SECURITY_GROUP_NAME="${NAME_PREFIX}-ecs"

log "Deploying runtime"
log "Environment: ${ENVIRONMENT_NAME}"
log "Commit: ${COMMIT_SHA}"
log "Image: ${IMAGE_URI}"
if [ -n "${RELEASE_TAG}" ]; then
  log "Release tag: ${RELEASE_TAG}"
fi

aws lambda get-function --function-name "${CONTROL_PLANE_LAMBDA_NAME}" >/dev/null
aws mwaa get-environment --name "${MWAA_ENVIRONMENT_NAME}" >/dev/null
aws s3api head-bucket --bucket "${ARTIFACT_BUCKET_NAME}" >/dev/null
aws s3api head-bucket --bucket "${DATA_BUCKET_NAME}" >/dev/null

ECS_CLUSTER_ARN="$(
  aws ecs describe-clusters \
    --clusters "${ECS_CLUSTER_NAME}" \
    --query 'clusters[0].clusterArn' \
    --output text
)"
[ "${ECS_CLUSTER_ARN}" != "None" ] || die "ECS cluster ${ECS_CLUSTER_NAME} does not exist"

CURRENT_TASK_DEFINITION_JSON="$(mktemp)"
aws ecs describe-task-definition --task-definition "${ECS_TASK_FAMILY}" --query 'taskDefinition' > "${CURRENT_TASK_DEFINITION_JSON}"
CONTAINER_NAME="$(jq -r '.containerDefinitions[0].name' "${CURRENT_TASK_DEFINITION_JSON}")"
[ -n "${CONTAINER_NAME}" ] || die "Could not determine ECS container name for ${ECS_TASK_FAMILY}"

SECURITY_GROUP_ID="$(
  aws ec2 describe-security-groups \
    --filters \
      "Name=group-name,Values=${ECS_SECURITY_GROUP_NAME}" \
      "Name=tag:Project,Values=${PROJECT_NAME}" \
      "Name=tag:Environment,Values=${ENVIRONMENT_NAME}" \
    --query 'SecurityGroups[0].GroupId' \
    --output text
)"
[ "${SECURITY_GROUP_ID}" != "None" ] || die "Security group ${ECS_SECURITY_GROUP_NAME} does not exist"

PRIVATE_SUBNET_IDS="$(
  aws ec2 describe-subnets \
    --filters \
      "Name=tag:Project,Values=${PROJECT_NAME}" \
      "Name=tag:Environment,Values=${ENVIRONMENT_NAME}" \
    --query 'Subnets[*].{Id:SubnetId,Name:Tags[?Key==`Name`]|[0].Value}' \
    --output json \
  | jq -r 'map(select(.Name | endswith("-private"))) | map(.Id) | sort | join(",")'
)"
[ -n "${PRIVATE_SUBNET_IDS}" ] || die "Could not discover private subnets for ${ENVIRONMENT_NAME}"

REGISTER_PAYLOAD_JSON="$(mktemp)"
jq \
  --arg image_uri "${IMAGE_URI}" \
  --arg container_name "${CONTAINER_NAME}" \
  '{
     family,
     taskRoleArn,
     executionRoleArn,
     networkMode,
     containerDefinitions: (
       .containerDefinitions
       | map(
           if .name == $container_name
           then .image = $image_uri
           else .
           end
         )
     ),
     volumes,
     placementConstraints,
     requiresCompatibilities,
     cpu,
     memory
   }
   + (if has("runtimePlatform") then {runtimePlatform} else {} end)
   + (if has("ephemeralStorage") then {ephemeralStorage} else {} end)' \
  "${CURRENT_TASK_DEFINITION_JSON}" > "${REGISTER_PAYLOAD_JSON}"

NEW_TASK_DEFINITION_ARN="$(
  aws ecs register-task-definition \
    --cli-input-json "file://${REGISTER_PAYLOAD_JSON}" \
    --query 'taskDefinition.taskDefinitionArn' \
    --output text
)"
[ "${NEW_TASK_DEFINITION_ARN}" != "None" ] || die "Failed to register new ECS task definition"

aws s3 sync "${BUNDLE_DIR}/dags" "s3://${ARTIFACT_BUCKET_NAME}/airflow/dags/" --delete
aws s3 cp "${BUNDLE_DIR}/mwaa-requirements.txt" "s3://${ARTIFACT_BUCKET_NAME}/airflow/mwaa-requirements.txt"
aws s3 cp "${BUNDLE_DIR}/plugins.zip" "s3://${ARTIFACT_BUCKET_NAME}/airflow/plugins.zip"

CONFIG_PAYLOAD_JSON="$(mktemp)"
cat > "${CONFIG_PAYLOAD_JSON}" <<EOF
{
  "dag_id": "nyc_taxi_pipeline",
  "configure_only": true,
  "airflow_variables": {
    "PIPELINE_RUNTIME": "cloud",
    "CLOUD_TASK_DEFINITION_ARN": "${NEW_TASK_DEFINITION_ARN}",
    "CLOUD_CLUSTER_ARN": "${ECS_CLUSTER_ARN}",
    "CLOUD_CONTAINER_NAME": "${CONTAINER_NAME}",
    "CLOUD_SUBNET_IDS": "${PRIVATE_SUBNET_IDS}",
    "CLOUD_SECURITY_GROUP_IDS": "${SECURITY_GROUP_ID}",
    "TRANSFORMATION_VERSION": "${COMMIT_SHA}"
  }
}
EOF

CONFIG_RESPONSE_JSON="$(mktemp)"
aws lambda invoke \
  --function-name "${CONTROL_PLANE_LAMBDA_NAME}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${CONFIG_PAYLOAD_JSON}" \
  "${CONFIG_RESPONSE_JSON}" >/dev/null
jq -e '.configured_only == true' "${CONFIG_RESPONSE_JSON}" >/dev/null

MWAA_STATUS="$(
  aws mwaa get-environment \
    --name "${MWAA_ENVIRONMENT_NAME}" \
    --query 'Environment.Status' \
    --output text
)"
[ "${MWAA_STATUS}" = "AVAILABLE" ] || die "MWAA environment ${MWAA_ENVIRONMENT_NAME} is not AVAILABLE"

log "Runtime deployment completed successfully"
log "ECS cluster ARN: ${ECS_CLUSTER_ARN}"
log "Task definition ARN: ${NEW_TASK_DEFINITION_ARN}"
log "Artifact bucket: s3://${ARTIFACT_BUCKET_NAME}/airflow/"
if [ -n "${CI_RUN_URL}" ]; then
  log "CI source: ${CI_RUN_URL}"
fi
log "Reason: ${DEPLOY_REASON}"
