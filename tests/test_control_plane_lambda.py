from __future__ import annotations

import json
import sys
import types
import unittest
from unittest import mock

boto3_stub = types.SimpleNamespace(client=lambda *args, **kwargs: None)

with mock.patch.dict(sys.modules, {"boto3": boto3_stub}):
    from orchestration.cloud import control_plane_lambda


class ControlPlaneLambdaTests(unittest.TestCase):
    def test_configure_only_retries_transient_import_errors(self) -> None:
        commands: list[str] = []

        def fake_cli(command: str) -> dict[str, object]:
            commands.append(command)
            if command == "variables set PIPELINE_RUNTIME cloud":
                return {"stdout": "", "stderr": ""}
            if command == "dags list-import-errors --output json":
                if commands.count(command) == 1:
                    return {
                        "stdout": json.dumps(
                            [
                                {
                                    "filepath": "/usr/local/airflow/dags/nyc_taxi_pipeline.py",
                                    "error": "ModuleNotFoundError: No module named 'orchestration'",
                                }
                            ]
                        ),
                        "stderr": "",
                    }
                return {"stdout": "[]", "stderr": ""}
            if command == "tasks list nyc_taxi_pipeline":
                return {
                    "stdout": "\n".join(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
                    "stderr": "",
                }
            raise AssertionError(f"Unexpected command: {command}")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
            mock.patch.object(control_plane_lambda.time, "sleep"),
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "dag_id": "nyc_taxi_pipeline",
                    "configure_only": True,
                    "airflow_variables": {"PIPELINE_RUNTIME": "cloud"},
                },
                None,
            )

        self.assertTrue(result["configured_only"])
        self.assertEqual(
            result["dag_readiness"]["task_ids"],
            list(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
        )

    def test_configure_only_waits_until_cloud_dag_is_ready(self) -> None:
        commands: list[str] = []

        def fake_cli(command: str) -> dict[str, object]:
            commands.append(command)
            if command == "variables set PIPELINE_RUNTIME cloud":
                return {"stdout": "", "stderr": ""}
            if command == "dags list-import-errors --output json":
                return {"stdout": "[]", "stderr": ""}
            if command == "tasks list nyc_taxi_pipeline":
                if commands.count(command) == 1:
                    return {
                        "stdout": "\n".join(
                            [
                                "stage_reference_data_task",
                                "resolve_yellow_config",
                                "ingest_yellow_landing",
                            ]
                        ),
                        "stderr": "",
                    }
                return {
                    "stdout": "\n".join(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
                    "stderr": "",
                }
            raise AssertionError(f"Unexpected command: {command}")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
            mock.patch.object(control_plane_lambda.time, "sleep"),
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "dag_id": "nyc_taxi_pipeline",
                    "configure_only": True,
                    "airflow_variables": {"PIPELINE_RUNTIME": "cloud"},
                },
                None,
            )

        self.assertTrue(result["configured_only"])
        self.assertEqual(
            result["dag_readiness"]["task_ids"],
            list(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
        )
        self.assertEqual(commands[1], "dags list-import-errors --output json")
        self.assertEqual(commands[2], "tasks list nyc_taxi_pipeline")

    def test_trigger_flow_unpauses_dag_before_triggering(self) -> None:
        commands: list[str] = []

        def fake_cli(command: str) -> dict[str, object]:
            commands.append(command)
            if command == "dags list-import-errors --output json":
                return {"stdout": "[]", "stderr": ""}
            if command == "tasks list nyc_taxi_pipeline":
                return {
                    "stdout": "\n".join(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
                    "stderr": "",
                }
            if command == "dags unpause nyc_taxi_pipeline":
                return {"stdout": "Dag: nyc_taxi_pipeline, paused: False\n", "stderr": ""}
            if command.startswith("dags trigger nyc_taxi_pipeline -r "):
                return {"stdout": "Created <DagRun manual__1>\n", "stderr": ""}
            if command == "dags list-runs -d nyc_taxi_pipeline --output json":
                return {
                    "stdout": json.dumps(
                        [{"run_id": "manual__1", "state": "success"}]
                    ),
                    "stderr": "",
                }
            raise AssertionError(f"Unexpected command: {command}")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
            mock.patch.object(
                control_plane_lambda,
                "_write_report_to_s3",
                return_value="s3://reports-bucket/control-plane/test.json",
            ),
            mock.patch.object(control_plane_lambda.time, "sleep"),
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "dag_id": "nyc_taxi_pipeline",
                    "conf": {"service": "yellow", "year": 2018, "month": 1},
                    "run_id": "manual__1",
                    "timeout_seconds": 1,
                    "poll_seconds": 0,
                },
                None,
            )

        self.assertEqual(result["state"], "success")
        self.assertEqual(commands[0], "dags list-import-errors --output json")
        self.assertEqual(commands[1], "tasks list nyc_taxi_pipeline")
        self.assertEqual(commands[2], "dags unpause nyc_taxi_pipeline")
        self.assertEqual(commands[3], "dags list-runs -d nyc_taxi_pipeline --output json")
        self.assertTrue(
            commands[4].startswith("dags trigger nyc_taxi_pipeline -r manual__1 --conf "),
            commands,
        )
        self.assertEqual(commands[5], "dags list-runs -d nyc_taxi_pipeline --output json")

    def test_trigger_flow_retries_until_dag_run_is_visible(self) -> None:
        commands: list[str] = []

        def fake_cli(command: str) -> dict[str, object]:
            commands.append(command)
            if command == "dags list-import-errors --output json":
                return {"stdout": "[]", "stderr": ""}
            if command == "tasks list nyc_taxi_pipeline":
                return {
                    "stdout": "\n".join(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
                    "stderr": "",
                }
            if command == "dags unpause nyc_taxi_pipeline":
                return {"stdout": "Dag: nyc_taxi_pipeline, paused: False\n", "stderr": ""}
            if command.startswith("dags trigger nyc_taxi_pipeline -r "):
                return {"stdout": "Created <DagRun manual__2>\n", "stderr": ""}
            if command == "dags list-runs -d nyc_taxi_pipeline --output json":
                if commands.count(command) == 1:
                    return {"stdout": "[]", "stderr": ""}
                return {
                    "stdout": json.dumps(
                        [{"run_id": "manual__2", "state": "success"}]
                    ),
                    "stderr": "",
                }
            raise AssertionError(f"Unexpected command: {command}")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
            mock.patch.object(
                control_plane_lambda,
                "_write_report_to_s3",
                return_value="s3://reports-bucket/control-plane/test.json",
            ),
            mock.patch.object(control_plane_lambda.time, "sleep"),
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "dag_id": "nyc_taxi_pipeline",
                    "conf": {"service": "yellow", "year": 2018, "month": 1},
                    "run_id": "manual__2",
                    "timeout_seconds": 2,
                    "poll_seconds": 0,
                },
                None,
            )

        self.assertEqual(result["state"], "success")
        self.assertEqual(commands.count("dags list-runs -d nyc_taxi_pipeline --output json"), 2)

    def test_trigger_operation_fails_when_another_run_is_active(self) -> None:
        commands: list[str] = []

        def fake_cli(command: str) -> dict[str, object]:
            commands.append(command)
            if command == "dags list-import-errors --output json":
                return {"stdout": "[]", "stderr": ""}
            if command == "tasks list nyc_taxi_pipeline":
                return {
                    "stdout": "\n".join(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS),
                    "stderr": "",
                }
            if command == "dags unpause nyc_taxi_pipeline":
                return {"stdout": "Dag: nyc_taxi_pipeline, paused: False\n", "stderr": ""}
            if command == "dags list-runs -d nyc_taxi_pipeline --output json":
                return {
                    "stdout": json.dumps(
                        [{"run_id": "manual__already-running", "state": "running"}]
                    ),
                    "stderr": "",
                }
            raise AssertionError(f"Unexpected command: {command}")

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
        ):
            with self.assertRaisesRegex(ValueError, "already has active runs"):
                control_plane_lambda.lambda_handler(
                    {
                        "operation": "trigger",
                        "dag_id": "nyc_taxi_pipeline",
                        "conf": {"service": "yellow", "year": 2018, "month": 1},
                        "run_id": "manual__new-run",
                    },
                    None,
                )

        self.assertNotIn(
            "dags trigger nyc_taxi_pipeline -r manual__new-run --conf ",
            commands,
        )

    def test_status_operation_treats_missing_run_as_queued(self) -> None:
        def fake_cli(command: str) -> dict[str, object]:
            self.assertEqual(command, "dags list-runs -d nyc_taxi_pipeline --output json")
            return {"stdout": "[]", "stderr": ""}

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "operation": "status",
                    "dag_id": "nyc_taxi_pipeline",
                    "run_id": "manual__missing",
                },
                None,
            )

        self.assertEqual(result["state"], "queued")
        self.assertEqual(result["report_s3_uri"], "")

    def test_status_operation_writes_report_for_terminal_state(self) -> None:
        def fake_cli(command: str) -> dict[str, object]:
            self.assertEqual(command, "dags list-runs -d nyc_taxi_pipeline --output json")
            return {
                "stdout": json.dumps(
                    [{"run_id": "manual__done", "state": "success"}]
                ),
                "stderr": "",
            }

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request", side_effect=fake_cli),
            mock.patch.object(
                control_plane_lambda,
                "_write_report_to_s3",
                return_value="s3://reports-bucket/control-plane/test.json",
            ) as write_report,
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "operation": "status",
                    "dag_id": "nyc_taxi_pipeline",
                    "run_id": "manual__done",
                    "report_key": "control-plane/test.json",
                },
                None,
            )

        self.assertEqual(result["state"], "success")
        self.assertEqual(result["report_s3_uri"], "s3://reports-bucket/control-plane/test.json")
        write_report.assert_called_once()

    def test_configure_only_does_not_unpause_or_trigger(self) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "MWAA_DAG_ID": "nyc_taxi_pipeline",
                    "MWAA_ENVIRONMENT_NAME": "test-mwaa",
                    "CONTROL_PLANE_REPORT_BUCKET": "reports-bucket",
                },
                clear=False,
            ),
            mock.patch.object(
                control_plane_lambda,
                "_wait_for_cloud_dag_ready",
                return_value={"task_ids": list(control_plane_lambda.EXPECTED_CLOUD_TASK_IDS)},
            ),
            mock.patch.object(control_plane_lambda, "_airflow_cli_request") as cli_request,
        ):
            result = control_plane_lambda.lambda_handler(
                {
                    "dag_id": "nyc_taxi_pipeline",
                    "configure_only": True,
                    "airflow_variables": {"PIPELINE_RUNTIME": "cloud"},
                },
                None,
            )

        self.assertTrue(result["configured_only"])
        cli_request.assert_called_once_with("variables set PIPELINE_RUNTIME cloud")


if __name__ == "__main__":
    unittest.main()
