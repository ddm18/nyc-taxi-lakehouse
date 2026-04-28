# ADR-012 - Data Quality Exception Handling with Flags, Metrics, and Quarantine

## Status
**Accepted**

## Context

Public mobility data includes invalid records, outliers, and incomplete
reference mappings. Phase 1 needs visibility into these records without forcing
the pipeline to fail on every anomaly.

Existing quality decisions already established checks and metrics, but the
platform also needs a durable place to isolate the most problematic rows.

## Decision

1. Keep Silver as the canonical record set, including anomalous rows when possible.
2. Represent row-level issues through explicit flags in Silver and Gold.
3. Publish partition-level observability metrics in `ops`.
4. Publish a `quarantine` schema containing the subset of rows that meet stronger exception rules.

Phase 1 quarantine categories include:

- invalid trips
- unmapped pickup or dropoff zones
- implausible fares
- major distance, duration, or amount outliers
- zero-distance trips

## Consequences

- Analysts retain full historical visibility while quality exceptions remain reviewable.
- Gold metrics can report invalid and anomalous rates without silently deleting source records.
- Future blocking thresholds can be introduced without redesigning the data model.

## Alternatives Considered

- Delete anomalous rows during Silver transformation: rejected (loss of auditability).
- Fail the pipeline on every warning: rejected (too brittle for public upstream data).
