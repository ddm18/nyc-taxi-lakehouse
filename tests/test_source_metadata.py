from __future__ import annotations

import tempfile
import unittest

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.source_metadata import (
    metadata_has_changed,
    read_source_metadata_current,
    record_source_metadata_observation,
)


class SourceMetadataTests(unittest.TestCase):
    def test_first_observation_is_not_marked_as_changed(self) -> None:
        dataset = DatasetMonthDTO(service="yellow", year=2024, month=3)

        with tempfile.TemporaryDirectory() as tmpdir:
            observation = record_source_metadata_observation(
                lakehouse_root=tmpdir,
                dataset=dataset,
                source_url="https://example.com/yellow_2024-03.parquet",
                landing_uri=f"{tmpdir}/landing/yellow/year=2024/month=03/file.parquet",
                metadata={"etag": "a", "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT", "content_length": 100},
                audit_reason="landing_ingest",
            )

            self.assertFalse(observation.source_metadata_changed)
            current = read_source_metadata_current(lakehouse_root=tmpdir, dataset=dataset)
            self.assertIsNotNone(current)
            self.assertEqual(current.etag, "a")

    def test_second_observation_detects_changed_metadata(self) -> None:
        dataset = DatasetMonthDTO(service="green", year=2024, month=4)

        with tempfile.TemporaryDirectory() as tmpdir:
            record_source_metadata_observation(
                lakehouse_root=tmpdir,
                dataset=dataset,
                source_url="https://example.com/green_2024-04.parquet",
                landing_uri=f"{tmpdir}/landing/green/year=2024/month=04/file.parquet",
                metadata={"etag": "a", "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT", "content_length": 100},
                audit_reason="landing_ingest",
            )
            observation = record_source_metadata_observation(
                lakehouse_root=tmpdir,
                dataset=dataset,
                source_url="https://example.com/green_2024-04.parquet",
                landing_uri=f"{tmpdir}/landing/green/year=2024/month=04/file.parquet",
                metadata={"etag": "b", "last_modified": "Tue, 02 Jan 2024 00:00:00 GMT", "content_length": 200},
                audit_reason="source_audit",
            )

            self.assertTrue(observation.source_metadata_changed)

    def test_metadata_has_changed_is_false_when_no_previous(self) -> None:
        self.assertFalse(metadata_has_changed(None, {"etag": "a"}))


if __name__ == "__main__":
    unittest.main()
