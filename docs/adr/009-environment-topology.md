# ADR-009 - Environment Topology: Local Runtime plus Test and Prod

## Status
**Accepted**

## Context

The project needs production-like deployment boundaries without introducing
unnecessary environment sprawl during Phase 1.

The original `dev/prod` split mixed two distinct concerns:

- local developer runtime concerns
- deployed shared validation concerns

This made local execution semantics ambiguous and encouraged treating a deployed
`dev` environment as both a sandbox and an integration target.

## Decision

Adopt the following environment model:

1. `local` is not a deployed environment.
2. `test` is the shared deployed environment used for integration and validation.
3. `prod` is the production deployed environment.

Practical implications:

- local Airflow/dbt/Spark can still target shared lakehouse resources through configuration
- local profiles are runtime-oriented, not environment-oriented
- deployed infrastructure is provisioned only for `test` and `prod`

## Consequences

- Fewer deployed environments to provision and maintain in Phase 1.
- Cleaner separation between local execution and deployed data domains.
- Documentation and defaults can distinguish "where code runs" from "which lakehouse namespace is targeted".

## Alternatives Considered

- Full `dev/test/prod`: rejected for Phase 1 due to higher operational overhead.
- `dev/prod` only: rejected because `dev` remained overloaded between local and shared concerns.
