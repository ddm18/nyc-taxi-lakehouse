# ADR-002 – Partitioning Strategy and Reprocessing Unit

## Status
Accepted

## Context

- NYC TLC datasets are published at monthly granularity.
- Phase 1 ingestion operates at dataset-month level (see ADR-001).
- The platform must support:
  - Predictable backfill
  - Bounded reprocessing scope
  - Full historical retention
  - Efficient analytical queries focused on recent operational trends.

The primary analytical horizon for Phase 1 workloads is **the most recent three months of data**.

At the moment, **no specific business query pattern requires finer-grained partitioning** (e.g., day-level).

This decision currently applies to the NYC TLC taxi datasets included in Phase 1.  
Other datasets (e.g., weather) may adopt different partitioning strategies depending on their structure and access patterns.

---

## Decision

1. The platform adopts a **time-based partitioning strategy aligned with the source publication model**.

2. The canonical partition keys are:

   `year` + `month`

3. This partitioning strategy is **aligned across Landing, Bronze, and Silver layers** in Phase 1.

4. The **reprocessing unit** is one dataset-month partition.

5. Bronze partitions are treated as **immutable**:
   - Reprocessing rewrites the entire month partition.

6. The platform distinguishes between **data retention** and **analytical horizon**.

   Retention policies:

   - Landing: 14 days
   - Bronze: infinite
   - Silver: infinite
   - Gold: 12 months

   Analytical workloads primarily target **the most recent three months of data**, while historical partitions remain available for backfill, reproducibility, and deeper analysis.

---

## Consequences

- Backfill and recovery operations are simple and bounded to a single month.
- Storage layout remains consistent across ingestion and transformation layers.
- Operational complexity and small-file risks are reduced.
- Historical raw data remains fully preserved.

However:

- Queries targeting narrow day-level ranges may scan an entire monthly partition.

Given the absence of concrete business requirements for day-level partitioning, the simpler monthly strategy is preferred for Phase 1.

---

## Future Evolution

If query patterns or business requirements indicate significant day-level access patterns, the Silver layer partitioning strategy may evolve to use **daily partitions (e.g., `pickup_date`)** without altering the ingestion model.

Additional optimizations may include:

- clustering strategies
- derived daily aggregates in Gold
- workload-specific optimization for analytical queries.

---

## Alternatives Considered

### Daily partitioning

Rejected for Phase 1 due to lack of business requirement and higher operational complexity.

### No explicit partitioning

Rejected due to poor scalability and expensive reprocessing.