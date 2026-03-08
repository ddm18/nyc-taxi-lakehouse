# Architecture - NYC Urban Mobility & Fare Dynamics (Phase 1)

!!! abstract "Purpose"
    This document translates business requirements into platform architecture decisions.
    Focus: governance, lineage, infrastructure, quality, and pipeline design.

## 1. Scope

### Included datasets
- NYC TLC yellow taxi trips
- NYC TLC green taxi trips

### Time range
Initial runs use sparse historical sampling to validate ingestion and schema normalization.
Full historical backfill is executed after pipeline validation.

### Environments
- `dev` (mandatory)
- `prod` (optional in Phase 1)

## 2. Non-goals

!!! warning "Out of scope in Phase 1"
    - Real-time/streaming ingestion
    - ML production pipelines
    - Serving APIs
    - BI serving infrastructure

The objective is a reliable platform foundation.

## 3. Architectural Drivers

| Business requirement | Driver | Architecture implication |
|---|---|---|
| Recent operational trends | Fast recent analytical queries | Hot analytical window focused on last 3 months |
| Hour/day demand | Time slicing | Derived time attributes in Silver + aggregates in Gold |
| Zone comparisons | Stable join keys | Versioned zone dimension + FK validation |
| Peak congestion | Proxy metrics | Duration/speed derivations + outlier detection |
| Weekday vs weekend | Day type | `dow`, `is_weekend` derived in Silver |

## 4. Target Architecture (Logical)

The platform follows a lakehouse model on Databricks.

`Source -> Landing -> Bronze -> Silver -> Gold`

### Layer responsibilities

| Layer | Role |
|---|---|
| Landing | Raw files downloaded from external sources |
| Bronze | Immutable raw ingestion with minimal transformation |
| Silver | Canonical curated datasets with schema normalization |
| Gold | Analytical datasets derived from curated data |

### Ingestion model

The ingestion model is controlled pull at monthly granularity.

1. The Ingestion Controller pulls one dataset-month.
2. Files are written to deterministic Landing paths.
3. On success, the system emits an internal arrival event.
4. Downstream jobs execute in sequence: Landing -> Bronze -> Silver -> Gold.

Event unit: `dataset-month`.

### Platform capabilities

| Capability | Implementation |
|---|---|
| Governance | Unity Catalog (catalog/schema/table RBAC) |
| Orchestration | Databricks Workflows |
| Data quality | Validation rules in transformations |
| Observability | DQ metrics + operational metadata |
| Lineage | Unity Catalog lineage |

### Transformation layer with dbt

Production transformations are implemented with dbt.

dbt is used to provide:

- deterministic model execution based on dependencies
- SQL-first transformation logic tracked in version control
- built-in tests for data quality validation
- model contracts and schema documentation
- generated lineage and documentation for analytical datasets

Transformation models are organized in logical layers:

- `staging`: source normalization and light cleanup
- `intermediate`: integration and enrichment across datasets
- `marts`: analytics-ready outputs for consumption

Notebook-based transformation logic is limited to exploration and prototyping.
When logic becomes production-ready, it must be promoted into dbt models.

## 5. Data Architecture

### 5.1 Storage layout

Root path:

`s3://<lake>/<env>/`

Subpaths:

- `landing/`
- `bronze/`
- `silver/`
- `gold/`
- `ops/`

Principles:

- Bronze is immutable
- Silver enforces canonical contracts
- Gold exposes analytics datasets
- `ops` isolates operational metadata

Naming convention: `layer.dataset_version`.

### 5.2 Partitioning strategy

Partition keys:

- `year`
- `month`

This is aligned across Landing, Bronze, and Silver.
Reprocessing unit: one `dataset-month` partition.

Constraints:

- Bronze partitions are immutable
- Default reprocessing window: last 12 months
- Main query horizon: latest 3 months

### 5.3 Data contracts

Silver datasets follow versioned canonical schemas.

Contracts define:

- column names
- data types
- semantic meaning

Schema evolution rules:

| Change type | Policy |
|---|---|
| Additive columns | Allowed |
| Breaking change | New schema version |
| Type drift | Normalized during transformation |

Examples:

- `silver.trips_v1`
- `silver.trips_v2`

Gold depends on a specific Silver version for stability.
Bronze preserves source structure as-is.

### 5.4 Gold layer

Gold is the primary analytical interface for consumers.

- Optimized for recent operational analysis (last 3 months)
- Historical partitions remain available for backfill and deep analysis

## 6. Governance & Security

### 6.1 Identity groups
- `engineers`
- `analysts`
- `viewers`

### 6.2 Unity Catalog structure
Catalog: `<project>_dev`

Schemas:

- `bronze`
- `silver`
- `gold`
- `ops`

### 6.3 Access policy

| Role | Access |
|---|---|
| Engineers | Read/Write all layers |
| Analysts | Read Silver and Gold |
| Viewers | Read Gold only |

### 6.4 Dataset ownership model

| Layer | Owner |
|---|---|
| Bronze | Data platform |
| Silver | Data engineering |
| Gold | Analytical layer |

### 6.5 Operational metadata
Operational tables are separated in `ops`.
Example: `ops.dq_metrics`.

## 7. Data Quality & Observability

!!! info "Phase 1 posture"
    Data quality validation occurs mainly in Bronze -> Silver.
    The initial ruleset is intentionally small and high-signal.
