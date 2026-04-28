from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from ingestion.shared.dto import DatasetMonthDTO
from ingestion.shared.io import probe_http_resource, uri_exists, write_text
from ingestion.shared.pipeline_state import (
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

PROJECT_ROOT = Path(os.environ.get("PYTHONPATH", str(Path(__file__).resolve().parents[2])))
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "nyc_taxi_lakehouse"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt" / "profiles"
DBT_STAGE_SELECTORS = {
    "bronze": {
        "yellow": ["yellow_tripdata_raw"],
        "green": ["green_tripdata_raw"],
    },
    "silver": {
        "yellow": ["dim_taxi_zones_v1", "yellow_tripdata_silver", "yellow_tripdata_dq_metrics_v1"],
        "green": ["dim_taxi_zones_v1", "green_tripdata_silver", "green_tripdata_dq_metrics_v1"],
    },
    "gold": {
        "yellow": ["yellow_trips_v1", "yellow_daily_metrics_v1", "yellow_hourly_zone_metrics_v1"],
        "green": ["green_trips_v1", "green_daily_metrics_v1", "green_hourly_zone_metrics_v1"],
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


def _lakehouse_root() -> str:
    return get_lakehouse_root_from_env()


def _transformation_version() -> str:
    return get_transformation_version_from_env()


def _run_command(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        env=env,
    )


def _dbt_vars(config: dict[str, Any]) -> str:
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


def _dbt_command(stage: str, config: dict[str, Any] | None = None) -> list[str]:
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
        _dbt_vars(config),
    ]


def _selected_services(service_selector: str) -> set[str]:
    if service_selector == "all":
        return {"yellow", "green"}
    return {service_selector}


def _iterate_dataset_months(start_year: int, end_year: int, end_month: int) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    for year in range(start_year, end_year + 1):
        month_limit = end_month if year == end_year else 12
        for month in range(1, month_limit + 1):
            months.append((year, month))
    return months


def _is_within_audit_window(
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


def _dataset_month_state_is_current(
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


def _resolve_landing_root(params: dict[str, Any]) -> str:
    landing_root_param = params.get("landing_root")
    if landing_root_param is not None and str(landing_root_param).strip():
        return str(landing_root_param).strip()
    return _lakehouse_root()


def _reference_landing_uri(landing_root: str, reference_name: str, file_name: str) -> str:
    normalized_root = landing_root.rstrip("/")
    return f"{normalized_root}/landing/reference/{reference_name}/{file_name}"


def _build_auto_config(
    *,
    service: str,
    landing_root: str,
    start_year: int,
    end_year: int,
    end_month: int,
    transformation_version: str,
) -> dict[str, Any] | None:
    source_config = load_source_config(service)
    for year, month in _iterate_dataset_months(start_year, end_year, end_month):
        dataset = DatasetMonthDTO(service=service, year=year, month=month)
        gold_state = read_pipeline_stage_state(
            lakehouse_root=landing_root,
            dataset=dataset,
            stage="gold",
        )
        source_url = build_source_url(source_config, dataset)
        source_metadata = probe_http_resource(source_url)

        if _dataset_month_state_is_current(
            landing_root,
            dataset,
            "gold",
            transformation_version,
        ):
            if not _is_within_audit_window(
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


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
    params={
        "service": Param("all", type="string", enum=["all", "yellow", "green"]),
        "year": Param(None, type=["null", "integer"], minimum=2014),
        "month": Param(None, type=["null", "integer"], minimum=1, maximum=12),
        "landing_root": Param(None, type=["null", "string"]),
        "auto_start_year": Param(2014, type="integer", minimum=2014),
    },
    tags=["nyc", "spark", "dbt", "aws", "incremental", "backfill"],
)
def nyc_taxi_pipeline():
    @task
    def stage_reference_data() -> str:
        context = get_current_context()
        landing_root = _resolve_landing_root(context["params"])

        for reference_file in REFERENCE_BOOTSTRAP_FILES:
            destination_uri = _reference_landing_uri(
                landing_root,
                str(reference_file["reference_name"]),
                str(reference_file["file_name"]),
            )
            source_path = Path(reference_file["source_path"])
            write_text(destination_uri, source_path.read_text(encoding="utf-8"))

        return landing_root

    @task
    def resolve_service_config(service_name: str) -> dict[str, Any] | None:
        context = get_current_context()
        params = context["params"]
        selected_services = _selected_services(str(params["service"]))
        if service_name not in selected_services:
            return None

        landing_root = _resolve_landing_root(params)
        interval_end = context["data_interval_end"] or context["logical_date"]
        transformation_version = _transformation_version()

        year_param = params.get("year")
        month_param = params.get("month")
        if (year_param is None) != (month_param is None):
            raise ValueError("year and month must be provided together for manual overrides")

        if year_param is not None and month_param is not None:
            return {
                "service": service_name,
                "year": int(year_param),
                "month": int(month_param),
                "landing_root": landing_root,
                "mode": "manual",
                "data_interval_start": context["data_interval_start"].isoformat()
                if context["data_interval_start"] is not None
                else None,
                "data_interval_end": interval_end.isoformat() if interval_end is not None else None,
                "transformation_version": transformation_version,
                "reprocess_reason": "manual_override",
                "start_stage": "ingestion",
                "requested_by": "manual_override",
            }

        auto_start_year = int(params["auto_start_year"])
        config = _build_auto_config(
            service=service_name,
            landing_root=landing_root,
            start_year=auto_start_year,
            end_year=interval_end.year,
            end_month=interval_end.month,
            transformation_version=transformation_version,
        )
        if config is None:
            return None

        config["data_interval_start"] = (
            context["data_interval_start"].isoformat()
            if context["data_interval_start"] is not None
            else None
        )
        config["data_interval_end"] = interval_end.isoformat() if interval_end is not None else None
        config["transformation_version"] = transformation_version
        return config

    @task
    def ingest_landing(config: dict[str, Any] | None) -> dict[str, Any] | None:
        if config is None:
            return None

        _run_command(
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
        return config

    @task
    def record_stage_state(config: dict[str, Any] | None, stage: str) -> str | None:
        if config is None:
            return None

        context = get_current_context()
        dataset = DatasetMonthDTO(
            service=str(config["service"]),
            year=int(config["year"]),
            month=int(config["month"]),
        )
        state = record_pipeline_stage_state(
            lakehouse_root=str(config["landing_root"]),
            dataset=dataset,
            stage=stage,
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            task_id=context["task"].task_id,
            data_interval_start=str(config["data_interval_start"])
            if config.get("data_interval_start") is not None
            else None,
            data_interval_end=str(config["data_interval_end"])
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
        return state.state_uri

    @task
    def run_stage(stage: str, config: dict[str, Any] | None) -> dict[str, Any] | None:
        if config is None:
            return None

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
        if config.get("transformation_version") is not None:
            env["TRANSFORMATION_VERSION"] = str(config["transformation_version"])
        _run_command(_dbt_command(stage, config), env=env)
        return config

    @task
    def run_unified_gold(
        yellow_config: dict[str, Any] | None,
        green_config: dict[str, Any] | None,
    ) -> None:
        if yellow_config is None and green_config is None:
            return

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
        env["TRANSFORMATION_VERSION"] = _transformation_version()
        _run_command(_dbt_command("gold_unified"), env=env)

    reference_data = stage_reference_data()

    yellow_config = resolve_service_config.override(task_id="resolve_yellow_config")("yellow")
    yellow_landing = ingest_landing.override(task_id="ingest_yellow_landing")(yellow_config)
    yellow_ingestion_state = record_stage_state.override(task_id="record_yellow_ingestion_state")(
        yellow_landing, "ingestion"
    )
    yellow_bronze = run_stage.override(task_id="run_yellow_bronze")("bronze", yellow_landing)
    yellow_bronze_state = record_stage_state.override(task_id="record_yellow_bronze_state")(
        yellow_bronze, "bronze"
    )
    yellow_silver = run_stage.override(task_id="run_yellow_silver")("silver", yellow_bronze)
    yellow_silver_state = record_stage_state.override(task_id="record_yellow_silver_state")(
        yellow_silver, "silver"
    )
    yellow_gold = run_stage.override(task_id="run_yellow_gold")("gold", yellow_silver)
    yellow_gold_state = record_stage_state.override(task_id="record_yellow_gold_state")(
        yellow_gold, "gold"
    )

    green_config = resolve_service_config.override(task_id="resolve_green_config")("green")
    green_landing = ingest_landing.override(task_id="ingest_green_landing")(green_config)
    green_ingestion_state = record_stage_state.override(task_id="record_green_ingestion_state")(
        green_landing, "ingestion"
    )
    green_bronze = run_stage.override(task_id="run_green_bronze")("bronze", green_landing)
    green_bronze_state = record_stage_state.override(task_id="record_green_bronze_state")(
        green_bronze, "bronze"
    )
    green_silver = run_stage.override(task_id="run_green_silver")("silver", green_bronze)
    green_silver_state = record_stage_state.override(task_id="record_green_silver_state")(
        green_silver, "silver"
    )
    green_gold = run_stage.override(task_id="run_green_gold")("gold", green_silver)
    green_gold_state = record_stage_state.override(task_id="record_green_gold_state")(
        green_gold, "gold"
    )

    unified_gold = run_unified_gold(yellow_gold, green_gold)

    reference_data >> yellow_config
    reference_data >> green_config

    yellow_config >> yellow_landing >> yellow_ingestion_state >> yellow_bronze >> yellow_bronze_state >> yellow_silver >> yellow_silver_state >> yellow_gold >> yellow_gold_state
    green_config >> green_landing >> green_ingestion_state >> green_bronze >> green_bronze_state >> green_silver >> green_silver_state >> green_gold >> green_gold_state
    [yellow_gold_state, green_gold_state] >> unified_gold


nyc_taxi_pipeline()
