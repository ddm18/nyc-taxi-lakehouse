from __future__ import annotations

import unittest

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.source_config import build_source_url, load_source_config


class SourceConfigTests(unittest.TestCase):
    def test_source_url_normalizes_duplicate_trip_data_prefix_for_yellow(self) -> None:
        dataset = DatasetMonthDTO(service="yellow", year=2024, month=2)
        config = load_source_config(dataset.service)

        actual = build_source_url(config, dataset)

        self.assertEqual(
            actual,
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
        )

    def test_source_url_normalizes_duplicate_trip_data_prefix_for_green(self) -> None:
        dataset = DatasetMonthDTO(service="green", year=2024, month=7)
        config = load_source_config(dataset.service)

        actual = build_source_url(config, dataset)

        self.assertEqual(
            actual,
            "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-07.parquet",
        )


if __name__ == "__main__":
    unittest.main()
