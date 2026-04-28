from __future__ import annotations

import tempfile
import unittest

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.reprocess_queue import (
    mark_reprocess_request_completed,
    read_reprocess_request,
    upsert_reprocess_request,
)


class ReprocessQueueTests(unittest.TestCase):
    def test_upsert_and_complete_request(self) -> None:
        dataset = DatasetMonthDTO(service="yellow", year=2024, month=6)

        with tempfile.TemporaryDirectory() as tmpdir:
            request = upsert_reprocess_request(
                lakehouse_root=tmpdir,
                dataset=dataset,
                start_stage="bronze",
                reason="transformation_version_changed",
                requested_by="unit_test",
                transformation_version="sha-123",
            )

            self.assertEqual(request.status, "pending")
            current = read_reprocess_request(lakehouse_root=tmpdir, dataset=dataset)
            self.assertIsNotNone(current)
            self.assertEqual(current.reason, "transformation_version_changed")

            completed = mark_reprocess_request_completed(
                lakehouse_root=tmpdir,
                dataset=dataset,
            )
            self.assertIsNotNone(completed)
            self.assertEqual(completed.status, "completed")
            self.assertIsNotNone(completed.completed_at)


if __name__ == "__main__":
    unittest.main()
