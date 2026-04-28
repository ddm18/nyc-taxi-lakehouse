# Architecture - NYC Urban Mobility & Fare Dynamics (Phase 1)

!!! abstract "Purpose"
    This document translates business requirements into platform architecture decisions.
    Focus: governance, lineage, infrastructure, quality, and pipeline design.

## 1. Scope

### Included datasets
- NYC TLC yellow taxi trips
- NYC TLC green taxi trips
- NYC TLC taxi zone lookup as internal reference data

### Time range
Phase 1 simulates historical retrieval from `2014-01` forward.
The orchestration loop processes one available `dataset-month` at a time and can
backfill incrementally until it reaches the latest published month.

### Environments
- `local` runtime for development and debugging
- deployed `test` environment for shared validation
- deployed `prod` environment for production workloads

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
| Zone comparisons | Stable join keys | Official TLC zone dimension + FK validation |
| Peak congestion | Proxy metrics | Duration/speed derivations + outlier detection |
| Weekday vs weekend | Day type | `dow`, `is_weekend` derived in Silver |
| Reproducible historical restatement | Controlled reprocessing | Stage state ledger + source metadata audit + transformation version |
| Analyst-facing geography | Human-readable spatial hierarchy | `location_id -> zone_name -> borough -> airport flag` enrichment |

## 4. Target Architecture (Logical)

The platform follows a layered lakehouse model on AWS-backed S3 with local Spark execution.

`Source -> Landing -> Bronze -> Silver -> Gold`

Reference and operational side paths:

- `landing/reference -> bronze -> silver` for internalized lookup datasets
- `silver -> ops/quarantine` for observability and exception handling

### Layer responsibilities

| Layer | Role |
|---|---|
| Landing | Raw files downloaded from external sources |
| Bronze | Immutable raw ingestion with minimal transformation |
| Silver | Canonical curated datasets with schema normalization |
| Gold | Analytical datasets derived from curated data |
| Ops | Operational state, source metadata, and DQ observability |
| Quarantine | Row-level anomalous records isolated for review |

### Ingestion model

The ingestion model is controlled pull at monthly granularity.

1. An Airflow controller evaluates the next candidate `dataset-month` for each taxi service.
2. The controller probes the upstream object and skips months that are not yet available.
3. One available month is pulled into a deterministic Landing path.
4. Downstream stages execute in sequence: Landing -> Bronze -> Silver -> Gold.
5. Each successful stage writes state into `ops.pipeline_state`.
6. Reference datasets are staged into `landing/reference` and promoted through Bronze/Silver like other internal datasets.

Event unit: `dataset-month`.

Operational control loop:

- default schedule: every 5 minutes
- default start year: `2014`
- manual override remains available through explicit `year/month`
- recent months can be marked for reprocessing when source metadata changes
- completed partitions can be marked stale when the deployed transformation version changes

### Platform capabilities

| Capability | Implementation |
|---|---|
| Governance | Layered schemas, dbt contracts, and repository-managed policy definitions |
| Orchestration | Airflow DAGs running local Spark + dbt workflows |
| Data quality | Validation rules in transformations, DQ metrics, and quarantine outputs |
| Observability | `ops.pipeline_state`, source metadata, reprocess requests, DQ metrics |
| Lineage | dbt lineage and dependency graph |
| Controlled reprocessing | transformation version + recent source metadata comparison |

### Transformation layer with dbt

Production transformations are implemented with dbt.

dbt is used to provide:

- deterministic model execution based on dependencies
- SQL-first transformation logic tracked in version control
- built-in tests for data quality validation
- model contracts and schema documentation
- generated lineage and documentation for analytical datasets

Transformation models are organized in logical layers:

- `bronze`: raw-to-managed ingestion over Landing files
- `silver`: source normalization, canonical cleanup, and conformed dimensions
- `gold`: analytics-ready outputs for consumption
- `ops`: partition-level observability metrics
- `quarantine`: exception datasets derived from Silver/Gold quality rules

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
- `quarantine/`

Principles:

- Bronze is immutable
- Silver enforces canonical contracts
- Gold exposes analytics datasets
- Reference datasets are internalized into the lakehouse before analytical use
- `ops` isolates operational metadata
- Quarantine isolates anomalous rows without mutating Bronze or Silver history

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
- Automatic historical discovery starts from `2014-01`

Typical reprocessing triggers:

- missing partition
- transformation version mismatch
- source republish detected through metadata comparison
- manual override

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
- `silver.dim_taxi_zones_v1`

Gold depends on a specific Silver version for stability.
Bronze preserves source structure as-is.

Reference datasets follow the same principle:

- raw reference extract in Bronze
- curated, queryable dimension in Silver
- business rollups can be added later as semantic mappings on top

### 5.4 Geography and Reference Data

Phase 1 geography is anchored on official TLC Taxi Zones.

Canonical hierarchy:

- `LocationID`
- `zone_name`
- `borough`
- airport vs non-airport flag

Optional business-defined clusters remain out of the raw physical model in Phase 1.
If needed later, they should be implemented as an explicit mapping on top of the official zone dimension.

### 5.5 Gold layer

Gold is the primary analytical interface for consumers.

- Optimized for recent operational analysis (last 3 months)
- Historical partitions remain available for backfill and deep analysis
- Service-specific outputs remain available for Yellow and Green
- Cross-service unified outputs provide a common analytical layer

Phase 1 Gold outputs:

- trip-level fact table
- daily service aggregates
- hourly pickup-zone aggregates

Gold enriches trips with:

- official TLC zone names
- borough names
- airport-zone flags
- peak/day-part business attributes
- quality-derived metrics such as invalid-trip and unmapped-zone rates

## 6. Governance & Security

### 6.1 Identity groups
- `engineers`
- `analysts`
- `viewers`

### 6.2 Environment schema structure
Environment namespace: `<project>_<env>`

Schemas:

- `bronze`
- `silver`
- `gold`
- `ops`
- `quarantine`

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
Operational datasets are separated in `ops`.

Examples:

- `ops.pipeline_state`
- `ops.source_metadata`
- `ops.source_metadata_audit`
- `ops.reprocess_queue`
- `ops.dq_metrics`

## 7. Data Quality & Observability

!!! info "Phase 1 posture"
    Data quality validation occurs mainly in Bronze -> Silver.
    The initial ruleset is intentionally small and high-signal.

Phase 1 quality handling includes:

- row-level validity flags in Silver
- aggregate DQ metrics in `ops`
- referential tests against official taxi zones
- quarantine outputs for invalid or anomalous trips

Blocking thresholds remain intentionally limited in Phase 1.
The platform favors visibility and deterministic replay over aggressive failure-on-warning semantics.
