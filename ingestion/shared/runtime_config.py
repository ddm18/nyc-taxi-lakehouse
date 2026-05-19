from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAKEHOUSE_BUCKET_URI = str(PROJECT_ROOT / ".local" / "lakehouse")
DEFAULT_LAKEHOUSE_ENV = "local"
DEFAULT_TRANSFORMATION_VERSION = "local-dev"


def _normalize_storage_uri(uri: str) -> str:
    normalized_uri = uri.strip()
    if not normalized_uri:
        raise ValueError("storage URI must not be empty")

    if "://" in normalized_uri:
        return normalized_uri.rstrip("/")

    return str(Path(normalized_uri).expanduser().resolve())


def build_lakehouse_root(
    *,
    explicit_root: str | None = None,
    bucket_uri: str | None = None,
    environment: str | None = None,
) -> str:
    """Resolve the canonical lakehouse root following s3://<lake>/<env>."""
    if explicit_root and explicit_root.strip():
        return _normalize_storage_uri(explicit_root)

    normalized_bucket = _normalize_storage_uri(
        bucket_uri or DEFAULT_LAKEHOUSE_BUCKET_URI
    )
    normalized_environment = (environment or DEFAULT_LAKEHOUSE_ENV).strip().strip("/")
    if not normalized_environment:
        raise ValueError("environment must not be empty")

    if normalized_bucket.startswith("s3://"):
        return f"{normalized_bucket}/{normalized_environment}"

    return str(Path(normalized_bucket) / normalized_environment)


def get_lakehouse_root_from_env() -> str:
    """Resolve the lakehouse root from runtime environment variables."""
    return build_lakehouse_root(
        explicit_root=os.getenv("LAKEHOUSE_ROOT"),
        bucket_uri=os.getenv("LAKEHOUSE_BUCKET_URI"),
        environment=os.getenv("LAKEHOUSE_ENV"),
    )


def get_transformation_version_from_env() -> str:
    """Resolve the current transformation version from runtime environment variables."""
    value = os.getenv("TRANSFORMATION_VERSION", DEFAULT_TRANSFORMATION_VERSION).strip()
    if not value:
        raise ValueError("TRANSFORMATION_VERSION must not be empty")
    return value
