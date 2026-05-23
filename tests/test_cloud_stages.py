from __future__ import annotations

import unittest

from orchestration.cloud import stages


TEST_CONFIG = {
    "service": "yellow",
    "year": 2018,
    "month": 1,
    "landing_root": "s3://nyc-data-platform-test/test",
}


class CloudStagesTests(unittest.TestCase):
    def test_bronze_command_keeps_stage_local_selectors(self) -> None:
        command = stages.dbt_command("bronze", TEST_CONFIG)

        select_index = command.index("--select")
        vars_index = command.index("--vars")

        self.assertEqual(
            command[select_index + 1:vars_index],
            ["yellow_tripdata_raw"],
        )

    def test_silver_command_includes_parent_models_for_isolated_sessions(self) -> None:
        command = stages.dbt_command("silver", TEST_CONFIG)

        select_index = command.index("--select")
        vars_index = command.index("--vars")

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

        select_index = command.index("--select")
        vars_index = command.index("--vars")

        self.assertEqual(
            command[select_index + 1:vars_index],
            [
                "+yellow_trips_v1",
                "+yellow_daily_metrics_v1",
                "+yellow_hourly_zone_metrics_v1",
            ],
        )


if __name__ == "__main__":
    unittest.main()
