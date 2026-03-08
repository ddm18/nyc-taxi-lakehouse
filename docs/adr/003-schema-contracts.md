# ADR-003 - Schema Evolution Strategy (Type Drift Handling)

## Status
**Accepted**

## Context

Observed TLC behavior in scope:

- Column set is mostly stable.
- Data types drift across years for some fields.

Likely cause: upstream export/nullability variation more than semantic redesign.
The platform needs stability for downstream layers while preserving raw source fidelity.

## Decision

1. Bronze stores source data as received.
2. Silver enforces a canonical schema.
3. Type normalization is applied in Silver with deterministic cast rules.
4. Additive columns are allowed and onboarded explicitly.
5. Breaking schema changes require a new version.
6. Gold depends on a fixed Silver version.

Canonical schema reference: `contracts/silver/trips/ yellow_trips_v1.yaml` and `contracts/silver/trips/green_trips_v1.yaml`.

Versioning examples:

- `silver.trips_v1`
- `silver.trips_v2`

## Consequences

- Pipeline remains robust to historical type drift.
- Bronze remains reproducible/auditable.
- Silver is a stable interface for downstream logic.
- Breaking changes are explicit and versioned.

## Alternatives Considered

- Strict schema enforcement at ingestion: rejected (would fail on drifted partitions).
- Automatic schema mutation without contract: rejected (high downstream break risk).
