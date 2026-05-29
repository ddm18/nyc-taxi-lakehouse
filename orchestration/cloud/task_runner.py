from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ingestion.shared.storage_io import write_json
from orchestration.cloud import audit
from orchestration.cloud.stages import (
    CLOUD_VALIDATION_EXPECTED_STAGES,
    build_validation_metadata,
    default_data_interval_end,
    parse_json_payload,
    record_stage_completion,
    run_pipeline_stage,
    stage_reference_data,
    transformation_version_from_env,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_SCHEMA_PATH = PROJECT_ROOT / "orchestration" / "cloud" / "sql" / "pipeline_run_audit.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cloud runtime entrypoint for orchestrated pipeline stages.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reference_parser = subparsers.add_parser("stage-reference-data")
    reference_parser.add_argument("--landing-root", required=True)
    reference_parser.add_argument("--transformation-version", required=True)

    stage_parser = subparsers.add_parser("run-stage")
    stage_parser.add_argument("--stage", required=True, choices=("ingestion", "bronze", "silver", "gold"))
    stage_parser.add_argument("--config-json", required=True)
    stage_parser.add_argument("--dag-id", required=True)
    stage_parser.add_argument("--run-id", required=True)
    stage_parser.add_argument("--task-id", required=True)
    stage_parser.add_argument("--data-interval-start")
    stage_parser.add_argument("--data-interval-end")

    init_parser = subparsers.add_parser("init-audit-db")
    init_parser.add_argument("--schema-path", default=str(AUDIT_SCHEMA_PATH))

    report_parser = subparsers.add_parser("write-final-report")
    report_parser.add_argument("--report-s3-uri", required=True)
    report_parser.add_argument("--environment-name", required=True)
    report_parser.add_argument("--artifact-digest", required=True)
    report_parser.add_argument("--verification-status", required=True)
    report_parser.add_argument("--metadata-json", default="{}")
    report_parser.add_argument("--config-json", required=True)
    report_parser.add_argument("--dag-id", required=True)
    report_parser.add_argument("--run-id", required=True)
    report_parser.add_argument("--transformation-version")
    report_parser.add_argument(
        "--expected-stage",
        action="append",
        default=[],
        choices=("ingestion", "bronze", "silver", "ops", "gold"),
    )

    return parser.parse_args()


def command_stage_reference_data(args: argparse.Namespace) -> int:
    stage_reference_data(args.landing_root, args.transformation_version)
    return 0


def command_run_stage(args: argparse.Namespace) -> int:
    config = parse_json_payload(args.config_json)
    if "transformation_version" not in config:
        config["transformation_version"] = transformation_version_from_env()
    if config.get("data_interval_end") is None:
        config["data_interval_end"] = default_data_interval_end(
            year=int(config["year"]),
            month=int(config["month"]),
        )

    config = run_pipeline_stage(args.stage, config)

    if args.stage in set(config.get("_completed_stages", [])):
        record_stage_completion(
            config=config,
            stage=args.stage,
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
            data_interval_start=args.data_interval_start,
            data_interval_end=args.data_interval_end,
        )
    return 0


def command_init_audit_db(args: argparse.Namespace) -> int:
    schema_path = Path(args.schema_path)
    audit.apply_schema(schema_path.read_text(encoding="utf-8"))
    return 0


def command_write_final_report(args: argparse.Namespace) -> int:
    config = parse_json_payload(args.config_json)
    metadata = parse_json_payload(args.metadata_json)
    transformation_version = args.transformation_version or str(
        config.get("transformation_version", transformation_version_from_env())
    )
    expected_stages = tuple(args.expected_stage or CLOUD_VALIDATION_EXPECTED_STAGES)
    validation_metadata = build_validation_metadata(
        config=config,
        expected_stages=expected_stages,
    )
    metadata.update(validation_metadata)
    verification_status = str(validation_metadata["verification_status"])
    report_payload: dict[str, Any] = {
        "environment_name": args.environment_name,
        "service_name": str(config["service"]),
        "dataset_month": f"{int(config['year']):04d}-{int(config['month']):02d}",
        "artifact_digest": args.artifact_digest,
        "transformation_version": transformation_version,
        "dag_id": args.dag_id,
        "run_id": args.run_id,
        "verification_status": verification_status,
        "report_s3_uri": args.report_s3_uri,
        "metadata": metadata,
    }
    write_json(args.report_s3_uri, report_payload)
    audit.insert_pipeline_run_audit(report_payload)
    if verification_status != args.verification_status:
        return 1
    return 0


def main() -> int:
    args = parse_args()
    if args.command == "stage-reference-data":
        return command_stage_reference_data(args)
    if args.command == "run-stage":
        return command_run_stage(args)
    if args.command == "init-audit-db":
        return command_init_audit_db(args)
    if args.command == "write-final-report":
        return command_write_final_report(args)
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
