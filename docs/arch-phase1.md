# Architecture – NYC Urban Mobility & Fare Dynamics (Platform) Phase 1

> This document derives architecture decisions from `business_context.md`.  
> It focuses on platform design (governance, lineage, IaC, quality), not analytics narratives.

---

# 1. Scope

## Included datasets (Phase 1)

- Taxi trips: NYC TLC (yellow)
- Taxi trips: NYC TLC (green)

## Time range (initial workload)

Initial validation runs use sparse historical sampling to validate ingestion robustness and schema normalization across historical data.

Full historical backfill will be performed after pipeline validation.

## Environments

- dev (mandatory)
- prod (optional / symbolic in Phase 1)

---

# 2. Non-goals

The following capabilities are explicitly out of scope for Phase 1:

- No real-time or streaming ingestion
- No ML production pipelines
- No serving/API layer
- No BI serving infrastructure

The platform focuses on building a reliable **data platform foundation**.

---

# 3. Architectural Drivers (derived)

| Business requirement | Driver | Architecture implication |
|---|---|---|
| Recent operational trends | Fast recent analytical queries | Hot analytical window focused on last 3 months |
| Hour/day demand | Time slicing | Derived time attributes in Silver + aggregates in Gold |
| Zone comparisons | Stable join keys | Versioned zone dimension + FK validation |
| Peak congestion | Proxy metrics | Duration/speed derivations + outlier detection |
| Weekday vs weekend | Day type | `dow`, `is_weekend` derived in Silver |

The architecture prioritizes **recent operational trend analysis** while preserving full historical raw data for reproducibility and backfill.

---

# 4. Target Architecture (Logical)

The platform follows a lakehouse architecture implemented on Databricks.

Data flows through the following layers:

Source → Landing → Bronze → Silver → Gold

## Layer responsibilities

| Layer | Role |
|---|---|
| Landing | Raw files downloaded from external sources |
| Bronze | Immutable raw ingestion with minimal transformation |
| Silver | Canonical curated datasets with schema normalization |
| Gold | Analytical datasets derived from curated data |

---

## Ingestion model

Data ingestion follows a **controlled pull model** operating at **monthly granularity**.

A scheduled **Ingestion Controller** retrieves a specific dataset-month from the source system and materializes the raw files in the Landing layer using deterministic partition paths.

When ingestion for a dataset-month completes successfully, the system emits an **internal arrival event**.

Downstream pipelines are triggered based on this event:

Landing → Bronze → Silver → Gold

The **event unit is dataset-month**.

---

## Platform capabilities

| Capability | Implementation |
|---|---|
| Governance | Unity Catalog (catalog/schema/table RBAC) |
| Orchestration | Databricks Workflows |
| Data quality | Validation rules applied in transformation |
| Observability | Data quality metrics and operational metadata |
| Lineage | Unity Catalog lineage |

---

# 5. Data Architecture

## 5.1 Storage layout

Data is stored in object storage with environment separation.

`s3://<lake>/<env>/`

Subdirectories follow the layered architecture:

`landing/`  
`bronze/`  
`silver/`  
`gold/`  
`ops/`

Storage principles:

- Bronze data is immutable
- Silver data follows canonical schema contracts
- Gold data contains analytical outputs
- Operational metadata is isolated in the `ops` area

Storage naming convention: `layer.dataset_version`.

---

## 5.2 Partitioning strategy

Trip datasets use a **time-based partitioning strategy aligned with the source publication model**.

Partition keys:

`year` and `month`

This partitioning strategy is aligned across:

Landing → Bronze → Silver

The **reprocessing unit is one dataset-month partition**.

Bronze partitions are treated as **immutable**.

Default reprocessing window: last 12 months

Analytical workloads primarily target **the most recent three months of data**, while historical partitions remain available for backfill and reproducibility.

---

## 5.3 Data contracts

Silver datasets follow **versioned canonical schemas** defined through schema contracts.

Contracts define:

- column names
- data types
- semantic meaning of fields

Schema normalization handles historical type drift observed across TLC datasets.

### Schema evolution rules

| Change type | Policy |
|---|---|
| Additive columns | Allowed |
| Breaking change | New schema version |
| Type drift | Normalized during transformation |

Example versioned datasets:

`silver.trips_v1`  
`silver.trips_v2`

Gold transformations depend on a **specific Silver dataset version** to ensure downstream stability.

Bronze preserves the original dataset structure as received from the source system.

---

## 5.4 Gold layer

The Gold layer contains analytical datasets derived from curated Silver data.

Gold datasets represent the **primary consumer interface** for analytical use cases.

Gold datasets are optimized for **analytical queries focused on recent operational trends**, typically covering the **last three months of data**.

Historical partitions remain available for deeper investigation or backfill scenarios.

---

# 6. Governance & Security

The platform implements **layer-based governance using Unity Catalog**.

---

## 6.1 Identity groups

Primary user roles:

- engineers
- analysts
- viewers

---

## 6.2 Unity Catalog structure

Catalog structure:

`<project>_dev`

Schemas:

`bronze`  
`silver`  
`gold`  
`ops`

---

## 6.3 Access policy

| Role | Access |
|---|---|
| Engineers | Read / Write all layers |
| Analysts | Read Silver and Gold |
| Viewers | Read Gold only |

---

## 6.4 Dataset ownership model

| Layer | Owner |
|---|---|
| Bronze | Data platform |
| Silver | Data engineering |
| Gold | Analytical layer |

Gold represents the **primary consumer layer**.

---

## 6.5 Operational metadata

Operational tables are stored separately in the `ops` schema.

Example: `ops.dq_metrics`.

---

# 7. Data Quality & Observability

Data Quality validation occurs during the **Bronze → Silver transformation**.

Rules are intentionally limited in Phase 1.

---

## 7.1 Rule types

| Type | Behavior |
|---|---|
| Blocking rules | Pipeline fails |
| Warning rules | Metric recorded |

---

## 7.2 Example rules

`fare_amount >= 0`  
`trip_distance BETWEEN 0 AND 200`

---

## 7.3 Metrics captured per load

Each pipeline execution records:

- row volume
- null rate for critical columns
- cast failure rate
- outlier rate
- data freshness

Metrics are stored in `ops.dq_metrics`.

Recent trend monitoring focuses particularly on the **most recent partitions**, where data freshness and completeness are critical.

---

## 7.4 Pipeline processing state tracking

The platform tracks the processing state of each dataset-month across pipeline stages.

This enables:

- deterministic backfill
- safe retries
- operational monitoring of pipeline progress

Processing state is stored in the operational table `ops.pipeline_state`.

Each dataset-month progresses through the following stages:

`ingestion → bronze → silver → gold`

Each pipeline stage updates the processing state upon successful completion.

---

# 8. Lineage

The platform relies on **Unity Catalog lineage**.

Minimum requirement:

- table-level lineage

Nice-to-have:

- column-level lineage (available through SQL lineage or Delta Live Tables)

---

# 9. Infrastructure as Code (Terraform)

Infrastructure provisioning is managed using **Terraform**.

Terraform provisions both:

- AWS infrastructure
- Databricks platform metadata resources

---

## 9.1 AWS resources

- S3 data lake bucket
- IAM roles allowing Databricks to access the lake
- Terraform remote state storage

---

## 9.2 Databricks resources

Terraform provisions the metadata structures required for the platform:

- Unity Catalog catalogs
- Unity Catalog schemas
- storage credentials
- external locations

---

## 9.3 Repository structure

`terraform/modules/`  
`terraform/envs/dev/`  
`terraform/envs/prod/`

Remote state can be stored using `S3 + DynamoDB locking` or Terraform Cloud.

---

# 10. Runbook (minimum)

## Deploy infrastructure

`terraform init`  
`terraform plan`  
`terraform apply`

---

## Run pipeline

Typical execution flow:

`run ingestion`  
`run landing → bronze transformation`  
`run bronze → silver transformation`  
`run silver → gold transformation`

Optional:

`reprocess recent partitions (typically last 3 months)`

---

## Verification checklist

Successful platform deployment requires:

- lineage visible in Unity Catalog
- rows written to `ops.dq_metrics`
- RBAC grants applied correctly
- datasets queryable from the Gold layer

---

# Appendix A – ADR Index

Architectural decisions are documented in ADR files.

- ADR-001 Ingestion model
- ADR-002 Partitioning strategy
- ADR-003 Schema evolution
- ADR-004 Data Quality strategy
- ADR-005 Governance model
- ADR-006 Infrastructure as Code layout