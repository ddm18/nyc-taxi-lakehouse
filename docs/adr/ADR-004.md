# ADR-004 – Data Quality Strategy

## Status
Accepted

## Context

The platform processes public datasets that may contain inconsistencies, type drift, null values, and outliers.

Data Quality checks must therefore ensure:

- robustness of the ingestion pipeline
- visibility into data anomalies
- traceability of data quality over time

The platform is not intended to block processing for minor anomalies but must detect and record them.

## Decision

1. **Data Quality checks are applied during transformation into the Silver layer.**

   Silver represents the canonical dataset and is therefore the point where validation and normalization occur.

2. **DQ rules are classified into two categories:**

   - **Blocking rules**: violations stop the pipeline for the affected partition.
   - **Warning rules**: violations are recorded but processing continues.

3. **Core DQ dimensions monitored per load include:**

   - row volume
   - null rate for critical fields
   - casting failures during schema normalization
   - outlier rate for numeric measures

4. **Data Quality metrics are recorded for each pipeline run.**

   Metrics are stored in an operational table:
   - ops.dq_metrics


    This table provides historical visibility into dataset quality.

5. **DQ rules remain intentionally limited in Phase 1.**

    The rule set focuses on a small number of high-signal checks to avoid unnecessary pipeline complexity.

## Consequences

- Data anomalies become observable without over-restricting the pipeline.
- Historical tracking of quality metrics enables trend analysis.
- Schema normalization errors (e.g., cast failures) are measurable.

## Alternatives Considered

**Strict validation with full pipeline failure**

Rejected because public datasets may contain minor inconsistencies that should not block ingestion.

**No explicit Data Quality monitoring**

Rejected because it removes visibility into anomalies and reduces operational observability.
