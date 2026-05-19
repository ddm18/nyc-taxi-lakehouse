# ADR-013 - Cloud Validation Slice for Test and Prod

## Status
**Accepted**

## Context

The project needs a realistic AWS deployment path that validates infrastructure,
artifact promotion, orchestration, and runtime execution without turning Phase 1
into a full always-on platform rollout.

The repository already supports a local developer runtime. What is missing is a
production-like deployment slice that can prove the code path from packaging to
Airflow orchestration to ECS execution inside real deployed AWS environments.

## Decision

Adopt a cloud validation slice with these characteristics:

1. Keep `local` as a developer runtime that defaults to filesystem storage.
2. Introduce deployed `test` and `prod` AWS environments.
3. Use Amazon MWAA as a lean orchestrator.
4. Run compute in ECS/Fargate with five separate stage-aligned jobs:
   - `reference`
   - `ingestion`
   - `bronze`
   - `silver`
   - `gold`
5. Keep Spark embedded inside the ECS runtime image.
6. Promote the same immutable image digest and `TRANSFORMATION_VERSION` from
   `test` to `prod`.
7. Keep environment data and deployment support artifacts in S3 after
   validation, while managing environment-scoped runtime infrastructure
   separately from normal code deployment workflows.
8. Use a private control-plane Lambda inside the VPC to trigger and monitor
   MWAA runs for `PRIVATE_ONLY` webserver access.
9. Use RDS PostgreSQL as a lightweight deployment/run audit store, not as the
   main analytical storage layer.
10. Manage the stack with Terraform end-to-end, using separate bootstrap stacks
    for shared prerequisites such as the remote state bucket, persistent S3
    data/artifact buckets, and persistent ECR repositories.

Validation scope for the initial slice:

- `yellow 2018-01`
- reference bootstrap for `taxi_zone_lookup`
- verification across `landing`, `bronze`, `silver`, `gold`, and `ops`

## Consequences

- Infrastructure, orchestration, and deployment are validated together instead
  of in isolation.
- `local` remains fast and safe for development.
- The cloud path stays realistic without forcing infrastructure lifecycle
  operations into every code release.
- The implementation introduces additional code for packaging, MWAA support,
  control-plane orchestration, and validation reporting.

## Alternatives Considered

- Keep only local runtime and defer cloud validation: rejected because it would
  postpone the highest-risk integration concerns.
- Use a single monolithic ECS job: rejected because it would hide stage-level
  orchestration behavior.
- Use public MWAA webserver access: rejected in favor of a private deployment
  topology with an internal control-plane component.
