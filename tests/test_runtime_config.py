from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ingestion.shared.runtime_config import (
    build_lakehouse_root,
    get_transformation_version_from_env,
)


class RuntimeConfigTests(unittest.TestCase):
    def test_explicit_root_takes_precedence(self) -> None:
        actual = build_lakehouse_root(
            explicit_root="s3://custom-lake/prod",
            bucket_uri="s3://ignored-bucket",
            environment="ignored-env",
        )

        self.assertEqual(actual, "s3://custom-lake/prod")

    def test_bucket_and_environment_follow_documented_convention(self) -> None:
        actual = build_lakehouse_root(
            bucket_uri="s3://nyc-data-platform-test",
            environment="test",
        )

        self.assertEqual(actual, "s3://nyc-data-platform-test/test")

    def test_local_bucket_root_resolves_to_absolute_env_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            actual = build_lakehouse_root(
                bucket_uri=tmpdir,
                environment="test",
            )

            expected = Path(tmpdir).resolve() / "test"
            self.assertEqual(actual, str(expected))

    def test_transformation_version_comes_from_env(self) -> None:
        with mock.patch.dict("os.environ", {"TRANSFORMATION_VERSION": "abc123"}, clear=False):
            actual = get_transformation_version_from_env()

        self.assertEqual(actual, "abc123")


if __name__ == "__main__":
    unittest.main()
