from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.io import read_text, uri_exists, write_text


@dataclass(frozen=True)
class ReprocessRequestDTO:
    service: str
    year: int
    month: int
    dataset_month: str
    start_stage: str
    reason: str
    status: str
    requested_at: str
    requested_by: str
    lakehouse_root: str
    request_uri: str
    transformation_version: str | None
    source_etag: str | None
    source_last_modified: str | None
    source_content_length: int | None
    completed_at: str | None


def build_reprocess_request_uri(
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
) -> str:
    normalized_root = lakehouse_root.rstrip("/")
    return (
        f"{normalized_root}/ops/reprocess_queue/"
        f"service={dataset.service}/year={dataset.year:04d}/month={dataset.month:02d}/request.json"
    )


def read_reprocess_request(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
) -> ReprocessRequestDTO | None:
    request_uri = build_reprocess_request_uri(lakehouse_root, dataset)
    if not uri_exists(request_uri):
        return None

    payload = json.loads(read_text(request_uri))
    return ReprocessRequestDTO(**payload)


def upsert_reprocess_request(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    start_stage: str,
    reason: str,
    requested_by: str,
    transformation_version: str | None = None,
    source_etag: str | None = None,
    source_last_modified: str | None = None,
    source_content_length: int | None = None,
) -> ReprocessRequestDTO:
    request_uri = build_reprocess_request_uri(lakehouse_root, dataset)
    payload = ReprocessRequestDTO(
        service=dataset.service,
        year=dataset.year,
        month=dataset.month,
        dataset_month=f"{dataset.year:04d}-{dataset.month:02d}",
        start_stage=start_stage,
        reason=reason,
        status="pending",
        requested_at=datetime.now(timezone.utc).isoformat(),
        requested_by=requested_by,
        lakehouse_root=lakehouse_root.rstrip("/"),
        request_uri=request_uri,
        transformation_version=transformation_version,
        source_etag=source_etag,
        source_last_modified=source_last_modified,
        source_content_length=source_content_length,
        completed_at=None,
    )
    write_text(request_uri, json.dumps(asdict(payload), indent=2, sort_keys=True) + "\n")
    return payload


def mark_reprocess_request_completed(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
) -> ReprocessRequestDTO | None:
    existing = read_reprocess_request(lakehouse_root=lakehouse_root, dataset=dataset)
    if existing is None:
        return None

    payload = ReprocessRequestDTO(
        service=existing.service,
        year=existing.year,
        month=existing.month,
        dataset_month=existing.dataset_month,
        start_stage=existing.start_stage,
        reason=existing.reason,
        status="completed",
        requested_at=existing.requested_at,
        requested_by=existing.requested_by,
        lakehouse_root=existing.lakehouse_root,
        request_uri=existing.request_uri,
        transformation_version=existing.transformation_version,
        source_etag=existing.source_etag,
        source_last_modified=existing.source_last_modified,
        source_content_length=existing.source_content_length,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    write_text(existing.request_uri, json.dumps(asdict(payload), indent=2, sort_keys=True) + "\n")
    return payload
