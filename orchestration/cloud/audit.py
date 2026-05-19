from __future__ import annotations

import json
import os
from typing import Any

import psycopg


def database_dsn_from_env() -> str:
    dsn = os.getenv("AUDIT_DB_DSN", "").strip()
    if not dsn:
        raise ValueError("AUDIT_DB_DSN must be set")
    return dsn


def apply_schema(sql_text: str) -> None:
    with psycopg.connect(database_dsn_from_env()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
        connection.commit()


def insert_pipeline_run_audit(payload: dict[str, Any]) -> None:
    with psycopg.connect(database_dsn_from_env()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into pipeline_run_audit (
                    environment_name,
                    service_name,
                    dataset_month,
                    artifact_digest,
                    transformation_version,
                    dag_id,
                    run_id,
                    verification_status,
                    report_s3_uri,
                    metadata_json
                ) values (
                    %(environment_name)s,
                    %(service_name)s,
                    %(dataset_month)s,
                    %(artifact_digest)s,
                    %(transformation_version)s,
                    %(dag_id)s,
                    %(run_id)s,
                    %(verification_status)s,
                    %(report_s3_uri)s,
                    %(metadata_json)s::jsonb
                )
                """,
                {
                    "environment_name": payload["environment_name"],
                    "service_name": payload["service_name"],
                    "dataset_month": payload["dataset_month"],
                    "artifact_digest": payload["artifact_digest"],
                    "transformation_version": payload["transformation_version"],
                    "dag_id": payload["dag_id"],
                    "run_id": payload["run_id"],
                    "verification_status": payload["verification_status"],
                    "report_s3_uri": payload["report_s3_uri"],
                    "metadata_json": json.dumps(payload.get("metadata", {}), sort_keys=True),
                },
            )
        connection.commit()

