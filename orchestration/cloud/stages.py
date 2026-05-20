from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.storage_io import probe_http_resource, uri_exists, write_text
from ingestion.shared.pipeline_state import (
    PipelineStageStateDTO,
    build_pipeline_state_uri,
    record_pipeline_stage_state,
    read_pipeline_stage_state,
)
from ingestion.shared.reprocess_queue import (
    mark_reprocess_request_completed,
    upsert_reprocess_request,
)
from ingestion.shared.runtime_config import (
    get_lakehouse_root_from_env,
    get_transformation_version_from_env,
)
from ingestion.shared.source_metadata import metadata_has_changed, read_source_metadata_current
from ingestion.shared.source_config import build_source_url, load_source_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "nyc_taxi_lakehouse"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt" / "profiles"
DBT_STAGE_SELECTORS = {
    "bronze": {
        "yellow": ["yellow_tripdata_raw"],
        "green": ["green_tripdata_raw"],
    },
    "silver": {
        "yellow": [
            "dim_taxi_zones_v1",
            "yellow_tripdata_silver",
            "yellow_tripdata_dq_metrics_v1",
        ],
        "green": [
            "dim_taxi_zones_v1",
            "green_tripdata_silver",
            "green_tripdata_dq_metrics_v1",
        ],
    },
    "gold": {
        "yellow": [
            "yellow_trips_v1",
            "yellow_daily_metrics_v1",
            "yellow_hourly_zone_metrics_v1",
        ],
        "green": [
            "green_trips_v1",
            "green_daily_metrics_v1",
            "green_hourly_zone_metrics_v1",
        ],
    },
    "gold_unified": ["trips_v1", "daily_metrics_v1", "hourly_zone_metrics_v1", "trip_anomalies_v1"],
}
REFERENCE_BOOTSTRAP_FILES = (
    {
        "reference_name": "taxi_zones",
        "file_name": "taxi_zone_lookup.csv",
        "source_path": PROJECT_ROOT / "dbt" / "nyc_taxi_lakehouse" / "seeds" / "taxi_zone_lookup.csv",
    },
)
SOURCE_METADATA_AUDIT_MONTHS = 12


def lakehouse_root_from_env() -> str:
    return get_lakehouse_root_from_env()


def transformation_version_from_env() -> str:
    return get_transformation_version_from_env()


def run_command(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        env=env,
    )


def dbt_vars(config: dict[str, Any]) -> str:
    return (
        "{service: '"
        + str(config["service"])
        + "', year: "
        + str(config["year"])
        + ", month: "
        + str(config["month"])
        + ", landing_root: '"
        + str(config["landing_root"])
        + "'}"
    )


def dbt_command(stage: str, config: dict[str, Any] | None = None) -> list[str]:
    if stage == "reference_bronze":
        return [
            "dbt",
            "run",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--select",
            "taxi_zone_lookup_raw",
        ]

    if stage == "gold_unified":
        return [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--select",
            *DBT_STAGE_SELECTORS["gold_unified"],
        ]

    if config is None:
        raise ValueError(f"config is required for stage {stage}")

    service = str(config["service"])
    selectors = DBT_STAGE_SELECTORS[stage][service]
    subcommand = "run" if stage == "bronze" else "build"
    return [
        "dbt",
        subcommand,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
        "--select",
        *selectors,
        "--vars",
        dbt_vars(config),
    ]


def selected_services(service_selector: str) -> set[str]:
    if service_selector == "all":
        return {"yellow", "green"}
    return {service_selector}


def iterate_dataset_months(start_year: int, end_year: int, end_month: int) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    for year in range(start_year, end_year + 1):
        month_limit = end_month if year == end_year else 12
        for month in range(1, month_limit + 1):
            months.append((year, month))
    return months


def is_within_audit_window(
    *,
    year: int,
    month: int,
    end_year: int,
    end_month: int,
    audit_months: int,
) -> bool:
    target_index = year * 12 + month
    end_index = end_year * 12 + end_month
    return end_index - target_index < audit_months


def dataset_month_state_is_current(
    lakehouse_root: str,
    dataset: DatasetMonthDTO,
    stage: str,
    transformation_version: str,
) -> bool:
    state_uri = build_pipeline_state_uri(lakehouse_root, dataset, stage)
    if not uri_exists(state_uri):
        return False

    state = read_pipeline_stage_state(
        lakehouse_root=lakehouse_root,
        dataset=dataset,
        stage=stage,
    )
    if state is None:
        return False

    return state.transformation_version == transformation_version


def resolve_landing_root(explicit_landing_root: str | None = None) -> str:
    if explicit_landing_root is not None and explicit_landing_root.strip():
        return explicit_landing_root.strip()
    return lakehouse_root_from_env()


def reference_landing_uri(landing_root: str, reference_name: str, file_name: str) -> str:
    normalized_root = landing_root.rstrip("/")
    return f"{normalized_root}/landing/reference/{reference_name}/{file_name}"


def manual_service_config(
    *,
    service: str,
    year: int,
    month: int,
    landing_root: str,
    transformation_version: str,
    data_interval_start: str | None = None,
    data_interval_end: str | None = None,
    reprocess_reason: str | None = "manual_override",
    requested_by: str = "manual_override",
    start_stage: str = "ingestion",
) -> dict[str, Any]:
    return {
        "service": service,
        "year": year,
        "month": month,
        "landing_root": landing_root,
        "mode": "manual",
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "transformation_version": transformation_version,
        "reprocess_reason": reprocess_reason,
        "start_stage": start_stage,
        "requested_by": requested_by,
    }


def build_auto_config(
    *,
    service: str,
    landing_root: str,
    start_year: int,
    end_year: int,
    end_month: int,
    transformation_version: str,
) -> dict[str, Any] | None:
    source_config = load_source_config(service)
    for year, month in iterate_dataset_months(start_year, end_year, end_month):
        dataset = DatasetMonthDTO(service=service, year=year, month=month)
        gold_state = read_pipeline_stage_state(
            lakehouse_root=landing_root,
            dataset=dataset,
            stage="gold",
        )
        source_url = build_source_url(source_config, dataset)
        source_metadata = probe_http_resource(source_url)

        if dataset_month_state_is_current(
            landing_root,
            dataset,
            "gold",
            transformation_version,
        ):
            if not is_within_audit_window(
                year=year,
                month=month,
                end_year=end_year,
                end_month=end_month,
                audit_months=SOURCE_METADATA_AUDIT_MONTHS,
            ):
                continue

            if source_metadata is None:
                continue

            current_observation = read_source_metadata_current(
                lakehouse_root=landing_root,
                dataset=dataset,
            )
            if metadata_has_changed(current_observation, source_metadata):
                upsert_reprocess_request(
                    lakehouse_root=landing_root,
                    dataset=dataset,
                    start_stage="ingestion",
                    reason="source_republish",
                    requested_by="airflow_source_audit",
                    transformation_version=transformation_version,
                    source_etag=source_metadata.get("etag"),
                    source_last_modified=source_metadata.get("last_modified"),
                    source_content_length=source_metadata.get("content_length"),
                )
                return {
                    "service": service,
                    "year": year,
                    "month": month,
                    "landing_root": landing_root,
                    "mode": "auto",
                    "reprocess_reason": "source_republish",
                    "start_stage": "ingestion",
                    "requested_by": "airflow_source_audit",
                    "source_etag": source_metadata.get("etag"),
                    "source_last_modified": source_metadata.get("last_modified"),
                    "source_content_length": source_metadata.get("content_length"),
                }
            continue

        if source_metadata is None:
            continue

        if gold_state is not None:
            upsert_reprocess_request(
                lakehouse_root=landing_root,
                dataset=dataset,
                start_stage="bronze",
                reason="transformation_version_changed",
                requested_by="airflow_transformation_version",
                transformation_version=transformation_version,
                source_etag=source_metadata.get("etag"),
                source_last_modified=source_metadata.get("last_modified"),
                source_content_length=source_metadata.get("content_length"),
            )
            return {
                "service": service,
                "year": year,
                "month": month,
                "landing_root": landing_root,
                "mode": "auto",
                "reprocess_reason": "transformation_version_changed",
                "start_stage": "bronze",
                "requested_by": "airflow_transformation_version",
                "source_etag": source_metadata.get("etag"),
                "source_last_modified": source_metadata.get("last_modified"),
                "source_content_length": source_metadata.get("content_length"),
            }

        return {
            "service": service,
            "year": year,
            "month": month,
            "landing_root": landing_root,
            "mode": "auto",
            "reprocess_reason": None,
            "start_stage": "ingestion",
            "requested_by": "airflow_new_partition",
            "source_etag": source_metadata.get("etag"),
            "source_last_modified": source_metadata.get("last_modified"),
            "source_content_length": source_metadata.get("content_length"),
        }
    return None


def serialize_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def parse_json_payload(payload: str) -> dict[str, Any]:
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object payload")
    return data


def dataset_from_config(config: dict[str, Any]) -> DatasetMonthDTO:
    return DatasetMonthDTO(
        service=str(config["service"]),
        year=int(config["year"]),
        month=int(config["month"]),
    )


def task_env(*, transformation_version: str | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
    if transformation_version is not None:
        env["TRANSFORMATION_VERSION"] = transformation_version
    return env


def stage_reference_data(landing_root: str, transformation_version: str) -> str:
    for reference_file in REFERENCE_BOOTSTRAP_FILES:
        destination_uri = reference_landing_uri(
            landing_root,
            str(reference_file["reference_name"]),
            str(reference_file["file_name"]),
        )
        source_path = Path(reference_file["source_path"])
        write_text(destination_uri, source_path.read_text(encoding="utf-8"))

    run_command(
        dbt_command("reference_bronze"),
        env=task_env(transformation_version=transformation_version),
    )
    return landing_root


def ingest_dataset_month(config: dict[str, Any]) -> None:
    run_command(
        [
            sys.executable,
            str(PROJECT_ROOT / "ingestion" / "ingest_tlc_to_landing.py"),
            "--service",
            str(config["service"]),
            "--year",
            str(config["year"]),
            "--month",
            str(config["month"]),
            "--landing-root",
            str(config["landing_root"]),
        ]
    )


def run_dbt_stage(stage: str, config: dict[str, Any]) -> None:
    run_command(
        dbt_command(stage, config),
        env=task_env(
            transformation_version=str(config["transformation_version"])
            if config.get("transformation_version") is not None
            else None
        ),
    )


def record_stage_completion(
    *,
    config: dict[str, Any],
    stage: str,
    dag_id: str,
    run_id: str,
    task_id: str,
    data_interval_start: str | None = None,
    data_interval_end: str | None = None,
) -> PipelineStageStateDTO:
    dataset = dataset_from_config(config)
    state = record_pipeline_stage_state(
        lakehouse_root=str(config["landing_root"]),
        dataset=dataset,
        stage=stage,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        data_interval_start=data_interval_start
        if data_interval_start is not None
        else str(config["data_interval_start"])
        if config.get("data_interval_start") is not None
        else None,
        data_interval_end=data_interval_end
        if data_interval_end is not None
        else str(config["data_interval_end"])
        if config.get("data_interval_end") is not None
        else None,
        transformation_version=str(config["transformation_version"])
        if config.get("transformation_version") is not None
        else None,
    )
    if stage == "gold" and config.get("reprocess_reason") not in {None, "manual_override"}:
        mark_reprocess_request_completed(
            lakehouse_root=str(config["landing_root"]),
            dataset=dataset,
        )
    return state


def stage_task_id(service: str, stage: str) -> str:
    return f"run_{service}_{stage}"


def default_data_interval_end(year: int, month: int) -> str:
    return datetime(year=year, month=month, day=1).isoformat()
