from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.source_config import build_landing_object_uri, load_source_config


class StorageLayoutTests(unittest.TestCase):
    def test_landing_uri_for_s3_root(self) -> None:
        dataset = DatasetMonthDTO(service="yellow", year=2024, month=3)
        config = load_source_config(dataset.service)

        actual = build_landing_object_uri(config, dataset, "s3://bucket/dev")

        self.assertEqual(
            actual,
            "s3://bucket/dev/landing/yellow/year=2024/month=03/yellow_tripdata_2024-03.parquet",
        )

    def test_landing_uri_for_local_root_is_absolute(self) -> None:
        dataset = DatasetMonthDTO(service="green", year=2024, month=5)
        config = load_source_config(dataset.service)

        with tempfile.TemporaryDirectory() as tmpdir:
            actual = build_landing_object_uri(config, dataset, tmpdir)
            expected_root = str(Path(tmpdir).resolve())

            self.assertTrue(actual.startswith(expected_root))
            self.assertTrue(
                actual.endswith(
                    "landing/green/year=2024/month=05/green_tripdata_2024-05.parquet"
                )
            )

if __name__ == "__main__":
    unittest.main()
