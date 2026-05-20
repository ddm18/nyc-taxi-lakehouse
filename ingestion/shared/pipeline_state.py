from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.storage_io import read_text, uri_exists, write_text

VALID_PIPELINE_STAGES = ("ingestion", "bronze", "silver", "gold")


@dataclass(frozen=True)
class PipelineStageStateDTO:
    service: str
    year: int
    month: int
    dataset_month: str
    stage: str
    status: str
    state_uri: str
    lakehouse_root: str
    recorded_at: str
    dag_id: str | None
    run_id: str | None
    task_id: str | None
    data_interval_start: str | None
    data_interval_end: str | None
    transformation_version: str | None


def build_pipeline_state_uri(
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    stage: str,
) -> str:
    """Build the deterministic operational state path for one dataset-month stage."""
    if stage not in VALID_PIPELINE_STAGES:
        raise ValueError(f"Unsupported pipeline stage: {stage}")

    normalized_root = lakehouse_root.rstrip("/")
    return (
        f"{normalized_root}/ops/pipeline_state/"
        f"service={dataset.service}/year={dataset.year:04d}/month={dataset.month:02d}/"
        f"stage={stage}.json"
    )


def record_pipeline_stage_state(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    stage: str,
    dag_id: str | None = None,
    run_id: str | None = None,
    task_id: str | None = None,
    data_interval_start: str | None = None,
    data_interval_end: str | None = None,
    transformation_version: str | None = None,
) -> PipelineStageStateDTO:
    """Persist a success marker for one pipeline stage in the ops path."""
    state_uri = build_pipeline_state_uri(lakehouse_root, dataset, stage)
    payload = PipelineStageStateDTO(
        service=dataset.service,
        year=dataset.year,
        month=dataset.month,
        dataset_month=f"{dataset.year:04d}-{dataset.month:02d}",
        stage=stage,
        status="success",
        state_uri=state_uri,
        lakehouse_root=lakehouse_root.rstrip("/"),
        recorded_at=datetime.now(timezone.utc).isoformat(),
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        transformation_version=transformation_version,
    )
    write_text(state_uri, json.dumps(asdict(payload), indent=2, sort_keys=True) + "\n")
    return payload


def read_pipeline_stage_state(
    *,
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    stage: str,
) -> PipelineStageStateDTO | None:
    """Read one persisted stage state if it exists."""
    state_uri = build_pipeline_state_uri(lakehouse_root, dataset, stage)
    if not uri_exists(state_uri):
        return None

    payload = json.loads(read_text(state_uri))
    payload.setdefault("transformation_version", None)
    return PipelineStageStateDTO(**payload)
