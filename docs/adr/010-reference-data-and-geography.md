# ADR-010 - Internal Reference Data and Official TLC Geography

## Status
**Accepted**

## Context

Early documentation described geography with business-oriented neighborhood
labels that do not exist as native keys in the TLC trip data.

The actual trip sources expose geography through official TLC `LocationID`
values. Reliable analytical geography therefore requires:

- an internalized lookup dataset
- stable joins to zone and borough attributes
- a clear distinction between physical geography and optional semantic rollups

## Decision

1. Treat the official TLC taxi zone lookup as internal reference data.
2. Stage the reference file into `landing/reference`.
3. Promote it into Bronze and then into `silver.dim_taxi_zones_v1`.
4. Use official TLC zones and boroughs as the Phase 1 physical geography model.
5. Defer business-defined spatial clusters to a future semantic mapping layer.

Canonical geography hierarchy:

- `location_id`
- `zone_name`
- `borough`
- `is_airport_zone`

## Consequences

- Gold datasets can expose human-readable geography without losing source fidelity.
- Referential quality checks become explicit and testable.
- Documentation no longer implies unsupported zone semantics.

## Alternatives Considered

- Keep neighborhood-style business labels as native geography: rejected (not grounded in source keys).
- Use the lookup only as a repo-local seed: rejected as the target runtime model because reference data should live in the lakehouse.
