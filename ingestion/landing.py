from __future__ import annotations

from ingestion.shared.dto import (
    LandingIngestionRequestDTO,
    LandingObjectSummaryDTO,
)
from ingestion.shared.io import download_file
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
    download_file(source_url, landing_uri)

    return LandingObjectSummaryDTO(
        landing_uri=landing_uri,
        dataset_month=f"{request.dataset.year:04d}-{request.dataset.month:02d}",
        source_url=source_url,
    )
