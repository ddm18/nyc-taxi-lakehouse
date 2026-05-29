from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowNotFoundException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from orchestration.cloud.stages import (
    build_auto_config,
    dbt_command,
    default_data_interval_end,
    manual_service_config,
    resolve_landing_root,
    run_command,
    selected_services,
    stage_reference_data,
    task_env,
    transformation_version_from_env,
    record_stage_completion,
    run_pipeline_stage,
    stage_was_completed,
)

def _airflow_variable(name: str) -> str:
    try:
        from airflow.models import Variable

        value = Variable.get(name)
    except (AirflowNotFoundException, Exception):
        return ""
    return str(value).strip()


def _runtime_setting(name: str, default: str = "") -> str:
    env_value = os.environ.get(name, "").strip()
    if env_value:
        return env_value
    variable_value = _airflow_variable(name)
    if variable_value:
        return variable_value
    return default


def _runtime_csv(name: str) -> list[str]:
    return [item.strip() for item in _runtime_setting(name).split(",") if item.strip()]


PIPELINE_RUNTIME = _runtime_setting("PIPELINE_RUNTIME", "local").lower()
CLOUD_TASK_DEFINITION_ARN = _runtime_setting("CLOUD_TASK_DEFINITION_ARN")
CLOUD_CLUSTER_ARN = _runtime_setting("CLOUD_CLUSTER_ARN")
CLOUD_CONTAINER_NAME = _runtime_setting("CLOUD_CONTAINER_NAME", "nyc-pipeline")
CLOUD_SUBNET_IDS = _runtime_csv("CLOUD_SUBNET_IDS")
CLOUD_SECURITY_GROUP_IDS = _runtime_csv("CLOUD_SECURITY_GROUP_IDS")
CLOUD_ENVIRONMENT_NAME = _runtime_setting("CLOUD_ENVIRONMENT_NAME", "cloud")


def _transformation_version() -> str:
    configured = _runtime_setting("TRANSFORMATION_VERSION")
    if configured:
        return configured
    return transformation_version_from_env()


def _resolve_manual_or_auto_config(service_name: str) -> dict[str, Any] | None:
    context = get_current_context()
    params = context["params"]
    selected = selected_services(str(params["service"]))
    if service_name not in selected:
        return None

    landing_root = resolve_landing_root(params.get("landing_root"))
    interval_end = context["data_interval_end"] or context["logical_date"]
    transformation_version = _transformation_version()

    year_param = params.get("year")
    month_param = params.get("month")
    if (year_param is None) != (month_param is None):
        raise ValueError("year and month must be provided together for manual overrides")

    if year_param is not None and month_param is not None:
        return manual_service_config(
            service=service_name,
            year=int(year_param),
            month=int(month_param),
            landing_root=landing_root,
            transformation_version=transformation_version,
            data_interval_start=context["data_interval_start"].isoformat()
            if context["data_interval_start"] is not None
            else None,
            data_interval_end=interval_end.isoformat() if interval_end is not None else None,
        )

    auto_start_year = int(params["auto_start_year"])
    config = build_auto_config(
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


def _cloud_network_configuration() -> dict[str, Any]:
    if not CLOUD_SUBNET_IDS or not CLOUD_SECURITY_GROUP_IDS:
        raise ValueError(
            "CLOUD_SUBNET_IDS and CLOUD_SECURITY_GROUP_IDS must be configured when PIPELINE_RUNTIME=cloud"
        )
    return {
        "awsvpcConfiguration": {
            "subnets": CLOUD_SUBNET_IDS,
            "securityGroups": CLOUD_SECURITY_GROUP_IDS,
            "assignPublicIp": "DISABLED",
        }
    }


def _cloud_ecs_task(
    *,
    task_id: str,
    command: list[str],
) -> EcsRunTaskOperator:
    if not CLOUD_TASK_DEFINITION_ARN or not CLOUD_CLUSTER_ARN:
        raise ValueError(
            "CLOUD_TASK_DEFINITION_ARN and CLOUD_CLUSTER_ARN must be configured when PIPELINE_RUNTIME=cloud"
        )
    return EcsRunTaskOperator(
        task_id=task_id,
        cluster=CLOUD_CLUSTER_ARN,
        task_definition=CLOUD_TASK_DEFINITION_ARN,
        launch_type="FARGATE",
        network_configuration=_cloud_network_configuration(),
        awslogs_group=None,
        wait_for_completion=True,
        reattach=False,
        do_xcom_push=False,
        overrides={
            "containerOverrides": [
                {
                    "name": CLOUD_CONTAINER_NAME,
                    "command": command,
                }
            ]
        },
    )


def _cloud_config_command(stage: str, config_task_id: str, task_id: str) -> list[str]:
    return [
        "run-stage",
        "--stage",
        stage,
        "--config-json",
        "{{ ti.xcom_pull(task_ids='" + config_task_id + "') | tojson }}",
        "--dag-id",
        "{{ dag.dag_id }}",
        "--run-id",
        "{{ run_id }}",
        "--task-id",
        task_id,
    ]


LOCAL_DAG_PARAMS = {
    "service": Param("all", type="string", enum=["all", "yellow", "green"]),
    "year": Param(None, type=["null", "integer"], minimum=2014),
    "month": Param(None, type=["null", "integer"], minimum=1, maximum=12),
    "landing_root": Param(None, type=["null", "string"]),
    "auto_start_year": Param(2014, type="integer", minimum=2014),
}

CLOUD_DAG_PARAMS = {
    "service": Param("yellow", type="string", enum=["yellow", "green"]),
    "year": Param(2018, type="integer", minimum=2014),
    "month": Param(1, type="integer", minimum=1, maximum=12),
    "landing_root": Param(None, type=["null", "string"]),
    "transformation_version": Param(None, type=["null", "string"]),
}


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
    params=LOCAL_DAG_PARAMS,
    tags=["nyc", "spark", "dbt", "aws", "incremental", "backfill", "local"],
)
def nyc_taxi_pipeline_local():
    @task
    def stage_reference_data_task() -> str:
        context = get_current_context()
        landing_root = resolve_landing_root(context["params"].get("landing_root"))
        stage_reference_data(landing_root, _transformation_version())
        return landing_root

    @task
    def resolve_service_config(service_name: str) -> dict[str, Any] | None:
        return _resolve_manual_or_auto_config(service_name)

    @task
    def record_stage_state(config: dict[str, Any] | None, stage: str) -> str | None:
        if config is None:
            return None
        if not stage_was_completed(config, stage):
            return None

        context = get_current_context()
        state = record_stage_completion(
            config=config,
            stage=stage,
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            task_id=context["task"].task_id,
        )
        return state.state_uri

    @task
    def run_stage_task(stage: str, config: dict[str, Any] | None) -> dict[str, Any] | None:
        if config is None:
            return None
        return run_pipeline_stage(stage, config)

    @task
    def run_unified_gold(
        yellow_config: dict[str, Any] | None,
        green_config: dict[str, Any] | None,
    ) -> None:
        if yellow_config is None and green_config is None:
            return

        run_command(
            dbt_command("gold_unified"),
            env=task_env(transformation_version=_transformation_version()),
        )

    reference_data = stage_reference_data_task()

    yellow_config = resolve_service_config.override(task_id="resolve_yellow_config")("yellow")
    yellow_landing = run_stage_task.override(task_id="run_yellow_ingestion")(
        "ingestion", yellow_config
    )
    yellow_ingestion_state = record_stage_state.override(task_id="record_yellow_ingestion_state")(
        yellow_landing, "ingestion"
    )
    yellow_bronze = run_stage_task.override(task_id="run_yellow_bronze")("bronze", yellow_landing)
    yellow_bronze_state = record_stage_state.override(task_id="record_yellow_bronze_state")(
        yellow_bronze, "bronze"
    )
    yellow_silver = run_stage_task.override(task_id="run_yellow_silver")("silver", yellow_bronze)
    yellow_silver_state = record_stage_state.override(task_id="record_yellow_silver_state")(
        yellow_silver, "silver"
    )
    yellow_gold = run_stage_task.override(task_id="run_yellow_gold")("gold", yellow_silver)
    yellow_gold_state = record_stage_state.override(task_id="record_yellow_gold_state")(
        yellow_gold, "gold"
    )

    green_config = resolve_service_config.override(task_id="resolve_green_config")("green")
    green_landing = run_stage_task.override(task_id="run_green_ingestion")(
        "ingestion", green_config
    )
    green_ingestion_state = record_stage_state.override(task_id="record_green_ingestion_state")(
        green_landing, "ingestion"
    )
    green_bronze = run_stage_task.override(task_id="run_green_bronze")("bronze", green_landing)
    green_bronze_state = record_stage_state.override(task_id="record_green_bronze_state")(
        green_bronze, "bronze"
    )
    green_silver = run_stage_task.override(task_id="run_green_silver")("silver", green_bronze)
    green_silver_state = record_stage_state.override(task_id="record_green_silver_state")(
        green_silver, "silver"
    )
    green_gold = run_stage_task.override(task_id="run_green_gold")("gold", green_silver)
    green_gold_state = record_stage_state.override(task_id="record_green_gold_state")(
        green_gold, "gold"
    )

    unified_gold = run_unified_gold(yellow_gold, green_gold)

    reference_data >> yellow_config
    reference_data >> green_config

    yellow_config >> yellow_landing >> yellow_ingestion_state >> yellow_bronze >> yellow_bronze_state >> yellow_silver >> yellow_silver_state >> yellow_gold >> yellow_gold_state
    green_config >> green_landing >> green_ingestion_state >> green_bronze >> green_bronze_state >> green_silver >> green_silver_state >> green_gold >> green_gold_state
    [yellow_gold_state, green_gold_state] >> unified_gold


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params=CLOUD_DAG_PARAMS,
    tags=["nyc", "aws", "ecs", "mwaa", "cloud"],
)
def nyc_taxi_pipeline_cloud():
    @task
    def resolve_runtime_context() -> dict[str, Any]:
        context = get_current_context()
        params = context["params"]
        transformation_version = params.get("transformation_version")
        if transformation_version is None or not str(transformation_version).strip():
            transformation_version = _transformation_version()
        if transformation_version is None or not str(transformation_version).strip():
            raise ValueError("transformation_version must be configured for cloud runtime")
        return {
            "landing_root": resolve_landing_root(params.get("landing_root")),
            "transformation_version": str(transformation_version).strip(),
        }

    @task
    def resolve_service_config(runtime_context: dict[str, Any]) -> dict[str, Any]:
        context = get_current_context()
        params = context["params"]
        return manual_service_config(
            service=str(params["service"]),
            year=int(params["year"]),
            month=int(params["month"]),
            landing_root=str(runtime_context["landing_root"]),
            transformation_version=str(runtime_context["transformation_version"]),
            data_interval_start=context["data_interval_start"].isoformat()
            if context["data_interval_start"] is not None
            else None,
            data_interval_end=context["data_interval_end"].isoformat()
            if context["data_interval_end"] is not None
            else default_data_interval_end(int(params["year"]), int(params["month"])),
        )

    runtime_context = resolve_runtime_context()
    service_config = resolve_service_config(runtime_context)

    init_audit_db = _cloud_ecs_task(
        task_id="init_audit_db",
        command=["init-audit-db"],
    )
    reference = _cloud_ecs_task(
        task_id="stage_reference_data",
        command=[
            "stage-reference-data",
            "--landing-root",
            "{{ ti.xcom_pull(task_ids='resolve_runtime_context')['landing_root'] }}",
            "--transformation-version",
            "{{ ti.xcom_pull(task_ids='resolve_runtime_context')['transformation_version'] }}",
        ],
    )
    ingestion = _cloud_ecs_task(
        task_id="run_ingestion",
        command=_cloud_config_command("ingestion", "resolve_service_config", "run_ingestion"),
    )
    bronze = _cloud_ecs_task(
        task_id="run_bronze",
        command=_cloud_config_command("bronze", "resolve_service_config", "run_bronze"),
    )
    silver = _cloud_ecs_task(
        task_id="run_silver",
        command=_cloud_config_command("silver", "resolve_service_config", "run_silver"),
    )
    gold = _cloud_ecs_task(
        task_id="run_gold",
        command=_cloud_config_command("gold", "resolve_service_config", "run_gold"),
    )
    final_report = _cloud_ecs_task(
        task_id="write_final_report",
        command=[
            "write-final-report",
            "--report-s3-uri",
            "{{ ti.xcom_pull(task_ids='resolve_runtime_context')['landing_root'] }}/ops/validation_reports/{{ run_id }}.json",
            "--environment-name",
            CLOUD_ENVIRONMENT_NAME,
            "--artifact-digest",
            CLOUD_TASK_DEFINITION_ARN,
            "--verification-status",
            "success",
            "--config-json",
            "{{ ti.xcom_pull(task_ids='resolve_service_config') | tojson }}",
            "--dag-id",
            "{{ dag.dag_id }}",
            "--run-id",
            "{{ run_id }}",
            "--transformation-version",
            "{{ ti.xcom_pull(task_ids='resolve_runtime_context')['transformation_version'] }}",
            "--expected-stage",
            "ingestion",
            "--expected-stage",
            "bronze",
            "--expected-stage",
            "silver",
            "--expected-stage",
            "ops",
            "--expected-stage",
            "gold",
        ],
    )

    runtime_context >> service_config >> init_audit_db >> reference >> ingestion >> bronze >> silver >> gold >> final_report


if PIPELINE_RUNTIME == "cloud":
    nyc_taxi_pipeline = nyc_taxi_pipeline_cloud()
else:
    nyc_taxi_pipeline = nyc_taxi_pipeline_local()
