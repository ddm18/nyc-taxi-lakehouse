from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from ingestion.shared.dto import DatasetMonthDTO

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "nyc_taxi_lakehouse"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt" / "profiles"


def _lakehouse_root() -> str:
    return os.getenv("LAKEHOUSE_S3_ROOT", "s3://nyc-data-platform-dev")


def _run_command(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        env=env,
    )


@dag(
    dag_id="nyc_taxi_local_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "service": "yellow",
        "year": 2024,
        "month": 1,
        "landing_root": _lakehouse_root(),
    },
    tags=["nyc", "spark", "dbt", "aws"],
)
def nyc_taxi_local_pipeline():
    @task
    def validate_params() -> dict[str, str | int]:
        context = get_current_context()
        params = context["params"]
        dataset = DatasetMonthDTO(
            service=str(params["service"]),
            year=int(params["year"]),
            month=int(params["month"]),
        )
        landing_root = str(params["landing_root"]).strip()
        if not landing_root:
            raise ValueError("landing_root must not be empty")
        return {
            "service": dataset.service,
            "year": dataset.year,
            "month": dataset.month,
            "landing_root": landing_root,
        }

    @task
    def ingest_landing(config: dict[str, str | int]) -> None:
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

    @task
    def run_dbt(config: dict[str, str | int]) -> None:
        service = str(config["service"])
        selector = {
            "yellow": "yellow_tripdata_raw+",
            "green": "green_tripdata_raw+",
        }[service]

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
        _run_command(
            [
                "dbt",
                "build",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
                "--select",
                selector,
                "--vars",
                (
                    "{service: '"
                    + service
                    + "', year: "
                    + str(config["year"])
                    + ", month: "
                    + str(config["month"])
                    + ", landing_root: '"
                    + str(config["landing_root"])
                    + "'}"
                ),
            ],
            env=env,
        )

    config = validate_params()
    landing = ingest_landing(config)
    dbt = run_dbt(config)

    config >> landing >> dbt


nyc_taxi_local_pipeline()
