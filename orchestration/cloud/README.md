# Cloud Orchestration

This directory contains the code-side building blocks for the cloud validation
slice defined for the AWS `test` and `prod` environments.

Components:

- `task_runner.py`: ECS entrypoint for reference bootstrap, pipeline stages,
  RDS schema initialization, and final reporting.
- `control_plane_lambda.py`: Lambda entrypoint used inside the VPC to trigger
  and monitor MWAA DAG runs for private-only environments.
- `audit.py`: small PostgreSQL audit helpers used by ECS tasks.
- `sql/pipeline_run_audit.sql`: schema for the RDS audit table.

No resource provisioning happens from this directory by itself.
