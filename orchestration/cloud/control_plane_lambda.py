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

ACTIVE_DAG_RUN_STATES = {"queued", "running"}
TERMINAL_DAG_RUN_STATES = {"success", "failed"}


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


def _list_dag_runs(dag_id: str) -> list[dict[str, Any]]:
    response = _airflow_cli_request(
        "dags list-runs -d " + shlex.quote(dag_id) + " --output json"
    )
    runs = json.loads(response["stdout"] or "[]")
    if not isinstance(runs, list):
        raise ValueError(f"unexpected dags list-runs output: {response['stdout']}")
    return runs


def _dag_run_state(dag_id: str, run_id: str) -> dict[str, Any]:
    runs = _list_dag_runs(dag_id)
    for run in runs:
        if str(run.get("run_id", "")) == run_id:
            return {
                "state": str(run.get("state", "unknown")),
                "stdout": json.dumps(runs),
                "stderr": "",
            }

    raise ValueError(f"could not find DAG run {run_id} in output: {json.dumps(runs)}")


def _active_dag_runs(
    dag_id: str,
    *,
    exclude_run_id: str | None = None,
) -> list[dict[str, str]]:
    active_runs: list[dict[str, str]] = []
    for run in _list_dag_runs(dag_id):
        run_id = str(run.get("run_id", "")).strip()
        state = str(run.get("state", "unknown")).strip().lower()
        if not run_id or run_id == exclude_run_id:
            continue
        if state in ACTIVE_DAG_RUN_STATES:
            active_runs.append(
                {
                    "run_id": run_id,
                    "state": state,
                }
            )
    return active_runs


def _assert_no_active_dag_runs(
    dag_id: str,
    *,
    exclude_run_id: str | None = None,
) -> None:
    active_runs = _active_dag_runs(dag_id, exclude_run_id=exclude_run_id)
    if not active_runs:
        return

    rendered = ", ".join(
        f"{run['run_id']} ({run['state']})" for run in active_runs
    )
    raise ValueError(
        f"DAG {dag_id} already has active runs. Resolve or wait for: {rendered}"
    )


def _generated_run_id() -> str:
    return "manual__" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _resolve_operation(event: dict[str, Any]) -> str:
    if bool(event.get("configure_only", False)):
        return "configure"
    operation = str(event.get("operation", "trigger_and_wait")).strip().lower()
    if operation not in {"configure", "trigger", "status", "trigger_and_wait"}:
        raise ValueError(f"unsupported operation: {operation}")
    return operation


def _report_if_terminal_state(
    *,
    report_key: str | None,
    state_payload: dict[str, Any],
) -> str:
    if not report_key:
        return ""
    if str(state_payload.get("state", "")).lower() not in TERMINAL_DAG_RUN_STATES:
        return ""
    return _write_report_to_s3(report_key, state_payload)


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    dag_id = event.get("dag_id", _required_env("MWAA_DAG_ID"))
    operation = _resolve_operation(event)

    if operation == "status":
        run_id = str(event.get("run_id", "")).strip()
        if not run_id:
            raise ValueError("run_id is required for status operation")

        state_payload = {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": "queued",
            "stdout": "",
            "stderr": "",
        }
        try:
            state_payload = _dag_run_state(dag_id, run_id)
            state_payload["dag_id"] = dag_id
            state_payload["run_id"] = run_id
        except ValueError as exc:
            if f"could not find DAG run {run_id}" not in str(exc):
                raise
        report_uri = _report_if_terminal_state(
            report_key=str(event.get("report_key", "")).strip() or None,
            state_payload=state_payload,
        )
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "report_s3_uri": report_uri,
            "state": state_payload["state"],
            "stdout": state_payload["stdout"],
            "stderr": state_payload["stderr"],
        }

    variable_updates = _set_airflow_variables(event.get("airflow_variables", {}))
    dag_readiness = _wait_for_cloud_dag_ready(dag_id)
    if operation == "configure":
        return {
            "dag_id": dag_id,
            "configured_only": True,
            "variable_updates": variable_updates,
            "dag_readiness": dag_readiness,
        }

    run_conf = event.get("conf", {})
    run_id = str(event.get("run_id") or _generated_run_id())
    unpause_response = _ensure_dag_unpaused(dag_id)
    _assert_no_active_dag_runs(dag_id, exclude_run_id=run_id)
    trigger_response = _airflow_cli_request(
        "dags trigger "
        + dag_id
        + " -r "
        + shlex.quote(run_id)
        + " --conf '"
        + json.dumps(run_conf, sort_keys=True)
        + "'"
    )

    report_key = str(event.get("report_key", "")).strip() or None
    if operation == "trigger":
        state_payload = {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": "queued",
            "stdout": "",
            "stderr": "",
        }
        try:
            state_payload = _dag_run_state(dag_id, run_id)
            state_payload["dag_id"] = dag_id
            state_payload["run_id"] = run_id
        except ValueError as exc:
            if f"could not find DAG run {run_id}" not in str(exc):
                raise

        report_uri = _report_if_terminal_state(
            report_key=report_key,
            state_payload=state_payload,
        )
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "report_s3_uri": report_uri,
            "state": state_payload["state"],
            "variable_updates": variable_updates,
            "dag_readiness": dag_readiness,
            "unpause_stdout": unpause_response["stdout"],
            "unpause_stderr": unpause_response["stderr"],
            "trigger_stdout": trigger_response["stdout"],
            "trigger_stderr": trigger_response["stderr"],
        }

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
        if state in TERMINAL_DAG_RUN_STATES:
            break
        time.sleep(poll_seconds)

    report_uri = _write_report_to_s3(
        str(event.get("report_key", f"control-plane/{dag_id}/{run_id}.json")),
        state_payload,
    )
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
