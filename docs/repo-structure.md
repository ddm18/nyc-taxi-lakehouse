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
| `governance/` | Policy definitions | Retention, quality tiers, governance rules |
| `infra/` | Infrastructure as code | Terraform modules and environment-specific infra code |
| `orchestration/` | Local orchestration helpers | Auxiliary local orchestration code not part of the data pipeline runtime |
| `exploration/` | Analysis sandbox | Notebooks and helper utilities for data exploration |
| `logs/` | Local execution logs | Tool logs (for example `dbt.log`) used for debugging |
| `typings/` | Static analysis typing helpers | Stub files used by type-checking tools |
| `site/` | Generated documentation site | MkDocs build output; generated artifact, not source content |

## Documentation section (`docs/`)

- `docs/index.md`: documentation landing page.
- `docs/arch-phase1.md`: architecture baseline for Phase 1.
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

- `infra/terraform/`: infrastructure provisioning code for deployed `test` and `prod` environments.
- `airflow/`: Airflow runtime files and DAG definitions.
- `orchestration/ai/`: local AI orchestration code, config, and agent specs.

## Conventions

- Keep production logic in `ingestion/`, `dbt/`, `infra/`, and `orchestration/`.
- Keep exploratory or temporary work in `exploration/`.
- Keep architecture decisions in `docs/adr/` and update architecture docs when decisions materially change behavior.
- Treat `site/` as generated output and avoid manual edits.
