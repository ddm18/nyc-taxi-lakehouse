# ADR-008 - Transformation Layer Implemented with dbt

## Status
**Accepted**

## Context

The platform requires a structured and maintainable transformation layer to support:

- multi-source integration (e.g. yellow taxi, green taxi, weather datasets)
- deterministic pipeline execution
- reproducible transformations
- explicit data lineage
- integrated data quality validation
- documentation of analytical datasets

Transformations could be implemented using different approaches:

- ad-hoc SQL scripts
- notebooks (Python or Spark)
- custom transformation frameworks
- a dedicated transformation tool

Notebook-based transformations introduce several drawbacks for a production analytics platform:

- implicit dependencies between datasets
- limited reproducibility
- weak lineage visibility
- fragmented documentation
- inconsistent testing practices

Given that most planned transformations are relational (joins, unions, aggregations, enrichment), a SQL-centric modeling layer is appropriate.

## Decision

Adopt 'dbt' (data build tool) as the primary transformation framework for the platform.

Transformations will be implemented as dbt models organized into structured layers:

- **staging** – source normalization and schema alignment  
- **intermediate** – dataset integration and enrichment  
- **marts** – analytics-ready datasets and business metrics  

dbt will be used to provide:

- dependency resolution via model references using 'ref()'
- deterministic build order through a directed acyclic graph (DAG)
- built-in data quality tests
- version-controlled transformation logic
- automated lineage and documentation generation

Notebook environments remain available for:

- data exploration
- debugging
- transformation prototyping

However, production transformations will be implemented as dbt models.

## Consequences

### Positive

- Explicit transformation lineage across datasets
- Consistent modeling structure across the platform
- Built-in testing for data quality validation
- Version-controlled transformation logic aligned with software engineering practices
- Clear separation between exploration workflows and production pipelines

### Trade-offs

- Transformations must primarily be expressible in SQL
- Additional project structure and conventions must be maintained
- Complex non-relational transformations may require complementary Python or Spark jobs

### Long-term impact

- Improves maintainability and observability of the analytics layer
- Enables scalable dataset integration across future pipeline phases
- Establishes a governance-oriented transformation framework for the platform