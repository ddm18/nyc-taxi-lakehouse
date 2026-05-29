from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from ingestion.shared.pipeline_state import read_pipeline_stage_state
from orchestration.cloud import stages


TEST_CONFIG = {
    "service": "yellow",
    "year": 2018,
    "month": 1,
    "landing_root": "s3://nyc-data-platform-test/test",
}


class CloudStagesTests(unittest.TestCase):
    def test_task_env_overrides_lakehouse_root_and_warehouse_dir(self) -> None:
        env = stages.task_env(
            transformation_version="abc123",
            lakehouse_root="s3://nyc-data-platform-test/test-runs/run-1",
        )

        self.assertEqual(env["TRANSFORMATION_VERSION"], "abc123")
        self.assertEqual(env["LAKEHOUSE_ROOT"], "s3://nyc-data-platform-test/test-runs/run-1")
        self.assertEqual(
            env["SPARK_WAREHOUSE_DIR"],
            "s3a://nyc-data-platform-test/test-runs/run-1/warehouse",
        )

    def test_bronze_command_keeps_stage_local_selectors(self) -> None:
        command = stages.dbt_command("bronze", TEST_CONFIG)

        select_index = command.index("--select")
        vars_index = command.index("--vars")

        self.assertEqual(
            command[select_index + 1:vars_index],
            ["yellow_tripdata_raw"],
        )
        self.assertNotIn("--indirect-selection", command)

    def test_silver_command_includes_parent_models_for_isolated_sessions(self) -> None:
        command = stages.dbt_command("silver", TEST_CONFIG)

        indirect_index = command.index("--indirect-selection")
        select_index = command.index("--select")
        vars_index = command.index("--vars")

        self.assertEqual(
            command[indirect_index:select_index],
            ["--indirect-selection", "cautious"],
        )
        self.assertEqual(
            command[select_index + 1:vars_index],
            [
                "+dim_taxi_zones_v1",
                "+yellow_tripdata_silver",
                "+yellow_tripdata_dq_metrics_v1",
            ],
        )

    def test_gold_command_includes_parent_models_for_isolated_sessions(self) -> None:
        command = stages.dbt_command("gold", TEST_CONFIG)

        indirect_index = command.index("--indirect-selection")
        select_index = command.index("--select")
        vars_index = command.index("--vars")

        self.assertEqual(
            command[indirect_index:select_index],
            ["--indirect-selection", "cautious"],
        )
        self.assertEqual(
            command[select_index + 1:vars_index],
            [
                "+yellow_trips_v1",
                "+yellow_daily_metrics_v1",
                "+yellow_hourly_zone_metrics_v1",
            ],
        )

    def test_default_data_interval_end_is_exclusive_month_boundary(self) -> None:
        self.assertEqual(stages.default_data_interval_end(2018, 1), "2018-02-01T00:00:00")
        self.assertEqual(stages.default_data_interval_end(2018, 12), "2019-01-01T00:00:00")

    def test_run_pipeline_stage_honors_bronze_start_stage(self) -> None:
        config = {
            **TEST_CONFIG,
            "start_stage": "bronze",
            "transformation_version": "abc123",
        }

        with mock.patch.object(stages, "ingest_dataset_month") as ingest_dataset_month:
            result = stages.run_pipeline_stage("ingestion", config)

        ingest_dataset_month.assert_not_called()
        self.assertEqual(result["_skipped_stages"], ["ingestion"])

    def test_silver_completion_records_ops_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = {
                **TEST_CONFIG,
                "landing_root": tmpdir,
                "transformation_version": "abc123",
                "data_interval_start": None,
                "data_interval_end": "2018-02-01T00:00:00",
            }

            stages.record_stage_completion(
                config=config,
                stage="silver",
                dag_id="dag",
                run_id="run",
                task_id="task",
            )
            ops_state = read_pipeline_stage_state(
                lakehouse_root=tmpdir,
                dataset=stages.dataset_from_config(config),
                stage="ops",
            )

        self.assertIsNotNone(ops_state)
        self.assertEqual(ops_state.transformation_version, "abc123")

    def test_build_validation_metadata_requires_landing_and_stage_states(self) -> None:
        with TemporaryDirectory() as tmpdir:
            landing_file = (
                Path(tmpdir)
                / "landing"
                / "yellow"
                / "year=2018"
                / "month=01"
                / "yellow_tripdata_2018-01.parquet"
            )
            landing_file.parent.mkdir(parents=True)
            landing_file.write_bytes(b"parquet-placeholder")
            config = {
                **TEST_CONFIG,
                "landing_root": tmpdir,
                "transformation_version": "abc123",
                "data_interval_start": None,
                "data_interval_end": "2018-02-01T00:00:00",
            }
            for stage in ("ingestion", "bronze", "silver", "gold"):
                stages.record_stage_completion(
                    config=config,
                    stage=stage,
                    dag_id="dag",
                    run_id="run",
                    task_id=f"run_{stage}",
                )

            metadata = stages.build_validation_metadata(config=config)

        self.assertEqual(metadata["verification_status"], "success")
        self.assertTrue(metadata["landing_object_exists"])
        self.assertEqual(metadata["missing_stages"], [])
        self.assertEqual(metadata["mismatched_transformation_version_stages"], [])


if __name__ == "__main__":
    unittest.main()
