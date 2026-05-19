# Repository Structure

This document describes the current repository layout and what belongs in each section.

## Top-level map

| Path | Purpose | What to put here |
|---|---|---|
| `README.md` | Project entry point | Project goals, quickstart, high-level workflow |
| `docs/` | Architecture and platform documentation | ADRs, architecture docs, discovery notes, process docs |
| `ingestion/` | Source ingestion configuration and Landing code | Source descriptors, Landing ingestion logic, raw ingestion config |
| `dbt/` | Transformation project(s) | dbt models, schema contracts, tests, macros, dbt config |
| `airflow/` | Airflow runtime | Airflow image files, Python dependencies, DAG definitions |
| `.github/` | CI/CD automation | GitHub Actions workflows for validation and controlled deployments |
| `scripts/` | Operator wrappers | Bash helpers for release tagging and manual workflow dispatch |
| `governance/` | Policy definitions | Retention, quality tiers, governance rules |
| `infra/` | Infrastructure as code | Terraform modules and environment-specific infra code |
| `orchestration/` | Orchestration support code | Cloud control-plane helpers, ECS task runners, and auxiliary orchestration logic |
| `exploration/` | Analysis sandbox | Notebooks and helper utilities for data exploration |
| `logs/` | Local execution logs | Tool logs (for example `dbt.log`) used for debugging |
| `typings/` | Static analysis typing helpers | Stub files used by type-checking tools |
| `site/` | Generated documentation site | MkDocs build output; generated artifact, not source content |

## Documentation section (`docs/`)

- `docs/index.md`: documentation landing page.
- `docs/architecture/`: primary architecture narrative, diagrams, and environment model.
- `docs/adr/`: architecture decision records in numeric order.
- `docs/exploration_notes/`: evidence collected during source analysis.

## Data pipeline section

- `ingestion/sources/`: source-level metadata and ingestion declarations.
- `ingestion/`: Landing ingestion code and shared ingestion helpers.
- `dbt/nyc_taxi_lakehouse/`: dbt project implementing transformation logic.
- `dbt/nyc_taxi_lakehouse/models/quarantine/`: exception datasets derived from quality rules.

## Governance section (`governance/`)

- `governance/policies/retention.yml`: data retention policy rules.
- `governance/policies/quality_tiers.yml`: quality tier definitions.
- `governance/README.md`: governance context and usage notes.

## Infra and orchestration

- `infra/terraform/bootstrap/`: shared prerequisite stacks such as Terraform state, persistent storage buckets, and bootstrap ECR repositories.
- `scripts/`: terminal wrappers for CI/CD operations, release tagging, runtime deployment, and test validation execution.
- `infra/terraform/envs/`: environment-scoped runtime stacks for deployed `test` and `prod` validation environments.
- `infra/terraform/modules/`: reusable Terraform building blocks consumed by both bootstrap and environment stacks.
- `airflow/`: Airflow runtime files and DAG definitions.
- `orchestration/cloud/`: ECS task runner, MWAA control-plane Lambda code, and audit schema for the cloud validation slice.

## Conventions

- Keep production logic in `ingestion/`, `dbt/`, `infra/`, and `orchestration/`.
- Keep exploratory or temporary work in `exploration/`.
- Keep architecture decisions in `docs/adr/` and update architecture docs when decisions materially change behavior.
- Treat `site/` as generated output and avoid manual edits.
