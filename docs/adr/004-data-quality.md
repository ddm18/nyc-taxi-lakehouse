# ADR-004 - Data Quality Strategy

## Status
**Accepted**

## Context

Public datasets may include nulls, outliers, and inconsistent types.
The platform needs quality visibility without unnecessary processing failures.

## Decision

1. Apply DQ checks during Bronze -> Silver transformation.
2. Split rules into:
   - **Blocking**: fail partition processing
   - **Warning**: log and continue
3. Track core dimensions per load:
   - row volume
   - null rate on critical fields
   - cast failures
   - numeric outlier rate
4. Store metrics in `ops.dq_metrics`.
5. Keep rule set intentionally small in Phase 1.

## Consequences

- Quality anomalies are visible and historically traceable.
- Pipeline remains resilient to minor source issues.
- Type normalization quality is measurable.

## Alternatives Considered

- Strict fail-on-any-anomaly: rejected (too rigid for public data).
- No DQ monitoring: rejected (no observability).
