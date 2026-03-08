# ADR-002 - Partitioning Strategy and Reprocessing Unit

## Status
**Accepted**

## Context

- NYC TLC is published monthly.
- Ingestion is dataset-month based (ADR-001).
- Platform needs bounded reprocessing, predictable backfill, and historical retention.
- Main analytical horizon in Phase 1: latest 3 months.

## Decision

1. Use time-based partitions aligned to source publication.
2. Canonical keys: `year` + `month`.
3. Keep partition strategy aligned across Landing, Bronze, Silver.
4. Reprocessing unit is one dataset-month partition.
5. Bronze partitions are immutable; reprocessing rewrites the full month.
6. Separate retention policy from analytical horizon.

Retention policy:

| Layer | Retention |
|---|---|
| Landing | 14 days |
| Bronze | Infinite |
| Silver | Infinite |
| Gold | 12 months |

## Consequences

- Backfill and recovery are simple and bounded.
- Storage layout is consistent across layers.
- Operational overhead and small-file risk are reduced.

Tradeoff:

- Day-level queries can scan entire monthly partitions.

## Future Evolution

If day-level access becomes a core pattern, Silver may evolve to daily partitions (for example `pickup_date`) without changing the ingestion model.

## Alternatives Considered

- Daily partitions in Phase 1: rejected (no current requirement, higher complexity).
- No explicit partitioning: rejected (poor scalability and costly reprocessing).
