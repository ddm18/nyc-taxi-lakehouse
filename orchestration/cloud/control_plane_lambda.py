from __future__ import annotations

from base64 import b64decode
import json
import os
import shlex
import time
from typing import Any
from urllib import error, request

import boto3


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


def _latest_dag_run_id(dag_id: str) -> str:
    response = _airflow_cli_request(
        "dags list-runs -d " + shlex.quote(dag_id) + " --output json"
    )
    runs = json.loads(response["stdout"] or "[]")
    if not isinstance(runs, list) or not runs:
        raise ValueError(f"could not determine latest DAG run from output: {response['stdout']}")
    run_id = str(runs[0].get("run_id", "")).strip()
    if not run_id:
        raise ValueError(f"latest DAG run did not include run_id: {response['stdout']}")
    return run_id


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    dag_id = event.get("dag_id", _required_env("MWAA_DAG_ID"))
    variable_updates = _set_airflow_variables(event.get("airflow_variables", {}))
    if bool(event.get("configure_only", False)):
        return {
            "dag_id": dag_id,
            "configured_only": True,
            "variable_updates": variable_updates,
        }

    run_conf = event.get("conf", {})
    trigger_response = _airflow_cli_request(
        "dags trigger "
        + dag_id
        + " --conf '"
        + json.dumps(run_conf, sort_keys=True)
        + "'"
    )
    run_id = event.get("run_id")
    if run_id is None:
        run_id = _latest_dag_run_id(dag_id)

    timeout_seconds = int(event.get("timeout_seconds", 900))
    poll_seconds = int(event.get("poll_seconds", 30))
    start = time.time()
    state_payload: dict[str, Any] = {}
    while time.time() - start <= timeout_seconds:
        state_response = _dag_run_state(dag_id, run_id)
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
        "trigger_stdout": trigger_response["stdout"],
        "trigger_stderr": trigger_response["stderr"],
    }
