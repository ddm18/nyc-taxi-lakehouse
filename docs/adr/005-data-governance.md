# ADR-005 - Data Governance Model

## Status
**Accepted**

## Context

Multiple user groups consume platform data with different responsibilities.
Governance must define ownership, access boundaries, and safe consumption layers.

## Decision

1. Apply layered governance (`landing -> bronze -> silver -> gold`).
2. Assign ownership by layer:

| Layer | Owner |
|---|---|
| Bronze | Data platform |
| Silver | Data engineering |
| Gold | Analytical layer |

3. Implement RBAC by group:

| Group | Permissions |
|---|---|
| Engineers | Read/Write all layers |
| Analysts | Read Silver + Gold |
| Viewers | Read Gold only |

4. Define Gold as primary consumer layer.
5. Keep operational metadata separated in `ops` schema.

## Consequences

- Responsibilities are explicit.
- Business users consume curated interfaces.
- Access control remains scalable and maintainable.

## Future Extensions

Potential next steps:

- stewardship workflows
- approval lifecycle
- centralized governance policy catalog
- data classification framework

## Alternatives Considered

- Open access to all layers: rejected (exposes raw/intermediate data).
