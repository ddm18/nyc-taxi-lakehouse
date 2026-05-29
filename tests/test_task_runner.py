from __future__ import annotations

import argparse
import sys
import types
import unittest
from unittest import mock

psycopg_stub = types.SimpleNamespace(connect=lambda *args, **kwargs: None)

with mock.patch.dict(sys.modules, {"psycopg": psycopg_stub}):
    from orchestration.cloud import task_runner


class TaskRunnerTests(unittest.TestCase):
    def test_final_report_writes_structured_success_payload(self) -> None:
        args = argparse.Namespace(
            report_s3_uri="s3://reports/final.json",
            environment_name="test",
            artifact_digest="task-definition-arn",
            verification_status="success",
            metadata_json='{"cloud_task_definition_arn": "task-definition-arn"}',
            config_json=(
                '{"service": "yellow", "year": 2018, "month": 1, '
                '"landing_root": "s3://lakehouse/run", "transformation_version": "abc123"}'
            ),
            dag_id="nyc_taxi_pipeline",
            run_id="validation__abc123",
            transformation_version="abc123",
            expected_stage=["ingestion", "bronze", "silver", "ops", "gold"],
        )
        validation_metadata = {
            "verification_status": "success",
            "landing_object_exists": True,
            "missing_stages": [],
            "mismatched_transformation_version_stages": [],
        }

        with (
            mock.patch.object(
                task_runner,
                "build_validation_metadata",
                return_value=validation_metadata,
            ) as build_validation_metadata,
            mock.patch.object(task_runner, "write_json") as write_json,
            mock.patch.object(task_runner.audit, "insert_pipeline_run_audit") as insert_audit,
        ):
            exit_code = task_runner.command_write_final_report(args)

        self.assertEqual(exit_code, 0)
        build_validation_metadata.assert_called_once()
        payload = write_json.call_args.args[1]
        self.assertEqual(payload["verification_status"], "success")
        self.assertEqual(payload["transformation_version"], "abc123")
        self.assertEqual(payload["metadata"]["verification_status"], "success")
        insert_audit.assert_called_once_with(payload)

    def test_final_report_returns_failure_for_failed_verification(self) -> None:
        args = argparse.Namespace(
            report_s3_uri="s3://reports/final.json",
            environment_name="test",
            artifact_digest="task-definition-arn",
            verification_status="success",
            metadata_json="{}",
            config_json=(
                '{"service": "yellow", "year": 2018, "month": 1, '
                '"landing_root": "s3://lakehouse/run", "transformation_version": "abc123"}'
            ),
            dag_id="nyc_taxi_pipeline",
            run_id="validation__abc123",
            transformation_version="abc123",
            expected_stage=["ingestion"],
        )

        with (
            mock.patch.object(
                task_runner,
                "build_validation_metadata",
                return_value={
                    "verification_status": "failed",
                    "landing_object_exists": False,
                    "missing_stages": ["ingestion"],
                    "mismatched_transformation_version_stages": [],
                },
            ),
            mock.patch.object(task_runner, "write_json"),
            mock.patch.object(task_runner.audit, "insert_pipeline_run_audit"),
        ):
            exit_code = task_runner.command_write_final_report(args)

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
