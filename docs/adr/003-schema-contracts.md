# ADR-003 – Schema Evolution Strategy (Handling Observed Type Drift)

## Status
Accepted

## Context

Inspection of the NYC TLC trip datasets within the project scope (2014 → latest available year) shows:

- Column names remain stable across the observed period.
- Data types vary across years for some fields.

This type drift is most likely caused by differences in upstream exports and nullability patterns rather than structural schema redesign.

The platform must therefore tolerate upstream type variation while providing a stable and predictable dataset for downstream transformations.

The Silver layer represents the **canonical dataset** used by downstream processing.

## Decision

1. **Bronze stores data as received.**

   Bronze preserves the source dataset with minimal transformation and tolerates type differences across partitions.

2. **Silver enforces a canonical schema.**

   Silver datasets follow a predefined schema that defines the canonical type for each column. contracts/silver_trips_v1.yaml

3. **Type normalization occurs in Silver.**

   When type drift exists across source partitions, Silver applies deterministic casting rules to normalize values into the canonical schema.

4. **Additive column changes are allowed.**

   If new columns appear in the upstream dataset, they are preserved in Bronze and incorporated into Silver only when explicitly added to the canonical schema.

5. **Breaking schema changes require versioning.**

   If the canonical schema must change incompatibly (for example column removal, rename, or incompatible type change affecting semantics), a new dataset version is created.

   Example:
   - silver.trips_v1 
   - silver.trips_v2


6. **Gold datasets depend on a specific Silver version.**

    Gold transformations reference a fixed Silver dataset version to ensure stability of downstream outputs.

## Consequences

- The pipeline remains robust to observed type drift across historical partitions.
- Bronze preserves the original source structure for reproducibility and auditing.
- Silver provides a stable canonical interface for downstream transformations.
- Schema changes that affect downstream semantics are managed through dataset versioning.

## Alternatives Considered

**Strict schema enforcement during ingestion**

Rejected because observed type drift across years would cause ingestion failures.

**Automatic schema mutation without a canonical schema**

Rejected because it could silently break downstream datasets and reduce schema stability.