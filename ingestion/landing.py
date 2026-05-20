from __future__ import annotations

from ingestion.shared.dto import (
    LandingIngestionRequestDTO,
    LandingObjectSummaryDTO,
)
from ingestion.shared.storage_io import download_file
from ingestion.shared.source_metadata import record_source_metadata_observation
from ingestion.shared.source_config import (
    build_landing_object_uri,
    build_source_url,
    load_source_config,
)


def ingest_to_landing(
    request: LandingIngestionRequestDTO,
) -> LandingObjectSummaryDTO:
    """Download one TLC dataset-month into a deterministic Landing path."""
    config = load_source_config(request.dataset.service)
    source_url = build_source_url(config, request.dataset)
    landing_uri = build_landing_object_uri(
        config,
        request.dataset,
        request.landing_root_uri,
    )
    download_metadata = download_file(source_url, landing_uri)
    observation = record_source_metadata_observation(
        lakehouse_root=request.landing_root_uri,
        dataset=request.dataset,
        source_url=source_url,
        landing_uri=landing_uri,
        metadata=download_metadata,
        audit_reason="landing_ingest",
    )

    return LandingObjectSummaryDTO(
        landing_uri=landing_uri,
        dataset_month=f"{request.dataset.year:04d}-{request.dataset.month:02d}",
        source_url=source_url,
        source_etag=download_metadata.get("etag"),
        source_last_modified=download_metadata.get("last_modified"),
        source_content_length=download_metadata.get("content_length"),
        source_metadata_uri=observation.current_state_uri,
        source_metadata_changed=observation.source_metadata_changed,
    )
