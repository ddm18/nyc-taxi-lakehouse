from __future__ import annotations

from base64 import b64decode
from datetime import datetime, timezone
import json
import os
import shlex
import time
from typing import Any
from urllib import request

import boto3


EXPECTED_CLOUD_TASK_IDS = (
    "init_audit_db",
    "stage_reference_data",
    "run_ingestion",
    "run_bronze",
    "run_silver",
    "run_gold",
)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} must be set")
    return value


def _mwaa_client() -> Any:
    return boto3.client("mwaa", region_name=_required_env("AWS_REGION"))


def _s3_client() -> Any:
    return boto3.client("s3", region_name=_required_env("AWS_REGION"))


def _airflow_cli_request(command: str) -> dict[str, Any]:
    environment_name = _required_env("MWAA_ENVIRONMENT_NAME")
    token_response = _mwaa_client().create_cli_token(Name=environment_name)
    endpoint = f"https://{token_response['WebServerHostname']}/aws_mwaa/cli"
    payload = command.encode("utf-8")
    http_request = request.Request(
        endpoint,
        data=payload,
        headers={
            "Authorization": f"Bearer {token_response['CliToken']}",
            "Content-Type": "text/plain",
        },
        method="POST",
    )
    with request.urlopen(http_request) as response:
        body = json.loads(response.read().decode("utf-8"))
        stdout = b64decode(body.get("stdout", "")).decode("utf-8")
        stderr = b64decode(body.get("stderr", "")).decode("utf-8")
        return {
            "stdout": stdout,
            "stderr": stderr,
            "status_code": response.status,
        }


def _write_report_to_s3(report_key: str, payload: dict[str, Any]) -> str:
    bucket = _required_env("CONTROL_PLANE_REPORT_BUCKET")
    _s3_client().put_object(
        Bucket=bucket,
        Key=report_key,
        Body=(json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{report_key}"


def _set_airflow_variables(variables: dict[str, Any]) -> list[dict[str, Any]]:
    responses: list[dict[str, Any]] = []
    for key, value in variables.items():
        rendered = str(value)
        command = "variables set " + shlex.quote(str(key)) + " " + shlex.quote(rendered)
        response = _airflow_cli_request(command)
        response["variable"] = str(key)
        responses.append(response)
    return responses


def _ensure_dag_unpaused(dag_id: str) -> dict[str, Any]:
    return _airflow_cli_request("dags unpause " + shlex.quote(dag_id))


def _list_import_errors() -> list[dict[str, Any]]:
    response = _airflow_cli_request("dags list-import-errors --output json")
    errors = json.loads(response["stdout"] or "[]")
    if not isinstance(errors, list):
        raise ValueError(f"unexpected dags list-import-errors output: {response['stdout']}")
    return errors


def _list_task_ids(dag_id: str) -> list[str]:
    response = _airflow_cli_request("tasks list " + shlex.quote(dag_id))
    task_ids = [line.strip() for line in response["stdout"].splitlines() if line.strip()]
    if not task_ids:
        raise ValueError(f"unexpected tasks list output for {dag_id}: {response['stdout']}")
    return task_ids


def _wait_for_cloud_dag_ready(
    dag_id: str,
    *,
    timeout_seconds: int = 120,
    poll_seconds: int = 5,
) -> dict[str, Any]:
    expected_task_ids = set(EXPECTED_CLOUD_TASK_IDS)
    start = time.time()
    last_task_ids: list[str] = []
    last_import_errors: list[dict[str, Any]] = []
    while time.time() - start <= timeout_seconds:
        import_errors = _list_import_errors()
        last_import_errors = import_errors
        if import_errors:
            time.sleep(poll_seconds)
            continue

        task_ids = _list_task_ids(dag_id)
        last_task_ids = task_ids
        if expected_task_ids.issubset(set(task_ids)):
            return {
                "task_ids": task_ids,
                "import_errors": import_errors,
            }

        time.sleep(poll_seconds)

    if last_import_errors:
        raise ValueError(f"DAG import errors detected: {json.dumps(last_import_errors, sort_keys=True)}")

    raise ValueError(
        "DAG did not become cloud-ready before timeout. "
        + f"Expected tasks {sorted(expected_task_ids)}, observed {last_task_ids}"
    )


def _dag_run_state(dag_id: str, run_id: str) -> dict[str, Any]:
    response = _airflow_cli_request(
        "dags list-runs -d " + shlex.quote(dag_id) + " --output json"
    )
    runs = json.loads(response["stdout"] or "[]")
    if not isinstance(runs, list):
        raise ValueError(f"unexpected dags list-runs output: {response['stdout']}")

    for run in runs:
        if str(run.get("run_id", "")) == run_id:
            return {
                "state": str(run.get("state", "unknown")),
                "stdout": response["stdout"],
                "stderr": response["stderr"],
            }

    raise ValueError(f"could not find DAG run {run_id} in output: {response['stdout']}")


def _generated_run_id() -> str:
    return "manual__" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    dag_id = event.get("dag_id", _required_env("MWAA_DAG_ID"))
    variable_updates = _set_airflow_variables(event.get("airflow_variables", {}))
    dag_readiness = _wait_for_cloud_dag_ready(dag_id)
    if bool(event.get("configure_only", False)):
        return {
            "dag_id": dag_id,
            "configured_only": True,
            "variable_updates": variable_updates,
            "dag_readiness": dag_readiness,
        }

    run_conf = event.get("conf", {})
    run_id = str(event.get("run_id") or _generated_run_id())
    unpause_response = _ensure_dag_unpaused(dag_id)
    trigger_response = _airflow_cli_request(
        "dags trigger "
        + dag_id
        + " -r "
        + shlex.quote(run_id)
        + " --conf '"
        + json.dumps(run_conf, sort_keys=True)
        + "'"
    )

    timeout_seconds = int(event.get("timeout_seconds", 900))
    poll_seconds = int(event.get("poll_seconds", 30))
    start = time.time()
    state_payload: dict[str, Any] = {}
    while time.time() - start <= timeout_seconds:
        try:
            state_response = _dag_run_state(dag_id, run_id)
        except ValueError as exc:
            if f"could not find DAG run {run_id}" not in str(exc):
                raise
            time.sleep(poll_seconds)
            continue
        state = state_response["state"]
        state_payload = {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": state,
            "stdout": state_response["stdout"],
            "stderr": state_response["stderr"],
        }
        if state in {"success", "failed"}:
            break
        time.sleep(poll_seconds)

    report_key = event.get("report_key", f"control-plane/{dag_id}/{run_id}.json")
    report_uri = _write_report_to_s3(report_key, state_payload)
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "report_s3_uri": report_uri,
        "state": state_payload.get("state", "unknown"),
        "variable_updates": variable_updates,
        "dag_readiness": dag_readiness,
        "unpause_stdout": unpause_response["stdout"],
        "unpause_stderr": unpause_response["stderr"],
        "trigger_stdout": trigger_response["stdout"],
        "trigger_stderr": trigger_response["stderr"],
    }
