from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.pipeline_state import (
    build_pipeline_state_uri,
    record_pipeline_stage_state,
    read_pipeline_stage_state,
)


class PipelineStateTests(unittest.TestCase):
    def test_state_uri_is_deterministic(self) -> None:
        dataset = DatasetMonthDTO(service="yellow", year=2024, month=3)

        actual = build_pipeline_state_uri("s3://bucket/dev", dataset, "silver")

        self.assertEqual(
            actual,
            "s3://bucket/dev/ops/pipeline_state/service=yellow/year=2024/month=03/stage=silver.json",
        )

    def test_record_pipeline_stage_state_writes_local_json(self) -> None:
        dataset = DatasetMonthDTO(service="green", year=2024, month=5)

        with tempfile.TemporaryDirectory() as tmpdir:
            payload = record_pipeline_stage_state(
                lakehouse_root=tmpdir,
                dataset=dataset,
                stage="gold",
                dag_id="nyc_taxi_pipeline",
                run_id="manual__2024-05-01",
                task_id="record_gold_state",
                transformation_version="sha-123",
            )

            written_path = Path(payload.state_uri)
            self.assertTrue(written_path.exists())

            actual = json.loads(written_path.read_text(encoding="utf-8"))
            self.assertEqual(actual["stage"], "gold")
            self.assertEqual(actual["dataset_month"], "2024-05")
            self.assertEqual(actual["service"], "green")
            self.assertEqual(actual["transformation_version"], "sha-123")

            state = read_pipeline_stage_state(
                lakehouse_root=tmpdir,
                dataset=dataset,
                stage="gold",
            )
            self.assertIsNotNone(state)
            self.assertEqual(state.transformation_version, "sha-123")


if __name__ == "__main__":
    unittest.main()
