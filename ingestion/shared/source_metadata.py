from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.storage_io import read_text, uri_exists, write_text


@dataclass(frozen=True)
class SourceMetadataObservationDTO:
    service: str
    year: int
    month: int
    dataset_month: str
    source_url: str
    landing_uri: str
    etag: str | None
    last_modified: str | None
    content_length: int | None
    observed_at: str
    audit_reason: str
    source_metadata_changed: bool
    current_state_uri: str
    audit_state_uri: str


def build_source_metadata_current_uri(
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
) -> str:
    normalized_root = lakehouse_root.rstrip("/")
    return (
        f"{normalized_root}/ops/source_metadata/"
        f"service={dataset.service}/year={dataset.year:04d}/month={dataset.month:02d}/current.json"
    )


def build_source_metadata_audit_uri(
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    observed_at: str,
) -> str:
    normalized_root = lakehouse_root.rstrip("/")
    timestamp_token = observed_at.replace(":", "-")
    return (
        f"{normalized_root}/ops/source_metadata_audit/"
        f"service={dataset.service}/year={dataset.year:04d}/month={dataset.month:02d}/"
        f"observed_at={timestamp_token}.json"
    )


def read_source_metadata_current(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
) -> SourceMetadataObservationDTO | None:
    current_uri = build_source_metadata_current_uri(lakehouse_root, dataset)
    if not uri_exists(current_uri):
        return None

    payload = json.loads(read_text(current_uri))
    return SourceMetadataObservationDTO(**payload)


def metadata_has_changed(
    previous: SourceMetadataObservationDTO | None,
    current_metadata: dict[str, Any],
) -> bool:
    if previous is None:
        return False

    return any(
        getattr(previous, field_name) != current_metadata.get(field_name)
        for field_name in ("etag", "last_modified", "content_length")
    )


def record_source_metadata_observation(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    source_url: str,
    landing_uri: str,
    metadata: dict[str, Any],
    audit_reason: str,
) -> SourceMetadataObservationDTO:
    previous = read_source_metadata_current(
        lakehouse_root=lakehouse_root,
        dataset=dataset,
    )
    observed_at = datetime.now(timezone.utc).isoformat()
    current_uri = build_source_metadata_current_uri(lakehouse_root, dataset)
    audit_uri = build_source_metadata_audit_uri(lakehouse_root, dataset, observed_at)
    payload = SourceMetadataObservationDTO(
        service=dataset.service,
        year=dataset.year,
        month=dataset.month,
        dataset_month=f"{dataset.year:04d}-{dataset.month:02d}",
        source_url=source_url,
        landing_uri=landing_uri,
        etag=metadata.get("etag"),
        last_modified=metadata.get("last_modified"),
        content_length=metadata.get("content_length"),
        observed_at=observed_at,
        audit_reason=audit_reason,
        source_metadata_changed=metadata_has_changed(previous, metadata),
        current_state_uri=current_uri,
        audit_state_uri=audit_uri,
    )
    encoded = json.dumps(asdict(payload), indent=2, sort_keys=True) + "\n"
    write_text(current_uri, encoded)
    write_text(audit_uri, encoded)
    return payload
