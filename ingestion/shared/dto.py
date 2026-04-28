from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetMonthDTO:
    service: str
    year: int
    month: int

    def __post_init__(self) -> None:
        if self.service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow' or 'green'")
        if self.month < 1 or self.month > 12:
            raise ValueError("month must be between 1 and 12")


@dataclass(frozen=True)
class LandingIngestionRequestDTO:
    dataset: DatasetMonthDTO
    landing_root_uri: str

    def __post_init__(self) -> None:
        if not self.landing_root_uri.strip():
            raise ValueError("landing_root_uri must not be empty")


@dataclass
class LandingObjectSummaryDTO:
    landing_uri: str
    dataset_month: str
    source_url: str
    source_etag: str | None = None
    source_last_modified: str | None = None
    source_content_length: int | None = None
    source_metadata_uri: str | None = None
    source_metadata_changed: bool | None = None
