# AGENTS.md

## Project Shape

This repository builds a batch-oriented lakehouse for NYC taxi mobility and fare
analysis. The core path is:

- `ingestion/` for landing ingestion and source descriptors
- `dbt/nyc_taxi_lakehouse/` for Bronze, Silver, Gold, Ops, and Quarantine models
- `airflow/dags/nyc_taxi_pipeline.py` for orchestration
- `infra/terraform/` for deployed `test` and `prod` infrastructure
- `orchestration/cloud/` for the cloud validation slice helpers

Treat this repo as a data platform project first, not as a generic app. Changes
should preserve the separation between orchestration, compute, storage, and
operational metadata.

## Source Of Truth

When project behavior is ambiguous, read sources in this order:

1. The code and Terraform that implement the behavior
2. ADRs under `docs/adr/`
3. Architecture pages under `docs/architecture/`
4. Top-level docs such as `README.md` and `docs/repo-structure.md`

Important ADR anchors:

- `docs/adr/009-environment-topology.md`: `local` is a developer runtime; only `test` and `prod` are deployed environments
- `docs/adr/013-cloud-validation-slice.md`: the cloud validation slice is MWAA + ECS/Fargate + Lambda + RDS, with teardown of non-S3 resources after validation

If code and docs disagree, trust the code, then update the docs.

## Working Rules

- Keep edits narrowly scoped to the requested behavior.
- Prefer existing project patterns over introducing new abstractions.
- Treat `site/` as generated output, never as source.
- Keep production logic in `ingestion/`, `dbt/`, `infra/`, and `orchestration/`.
- Put exploratory or throwaway work in `exploration/` or other clearly non-production locations.

## Cloud And Safety

- Do not create, update, or destroy AWS infrastructure unless the user explicitly asks for a real cloud action in that turn.
- Default to code-only changes for Terraform, GitHub Actions, MWAA, ECS, Lambda, RDS, and networking work.
- Do not assume a cloud design is “correct” just because it matches a common AWS pattern. Check what this repo actually declares.
- Be precise about the difference between:
  - facts declared in code
  - managed-service behavior handled by AWS
  - inferences or assumptions

## Architecture Docs And Diagrams

- Ground architecture docs and diagrams in the relevant IaC, runtime, and configuration files before editing them.
- Do not depict or describe a resource as pinned to a specific AZ, subnet, node, or host unless that placement is explicitly declared in code or confirmed by primary documentation.
- If a diagram must simplify reality, prefer a less specific representation over a misleadingly precise one.
- Keep diagrams visually clean. The diagram should explain itself through grouping, naming, and layout; do not rely on long explanatory notes inside the figure.
- Before finalizing architecture docs or diagrams, do a semantic consistency pass against the repo and call out any remaining assumptions.

## Validation Paths

Use the repo’s existing validation paths when relevant:

- Python tests: `python -m unittest discover -s tests -q`
- Terraform formatting: `terraform fmt -check -recursive infra/terraform`
- Terraform validation without backend: the CI workflows validate bootstrap, `test`, and `prod` with `init -backend=false`
- Docs build: `python3 -m mkdocs build --strict`

If you do not run a validation step, say so explicitly.
