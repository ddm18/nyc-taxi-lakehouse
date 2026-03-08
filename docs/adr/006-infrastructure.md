# ADR-006 - Infrastructure as Code Model

## Status
**Accepted**

## Context

The platform requires reproducible, version-controlled provisioning for storage, catalogs, schemas, and permissions.
Manual provisioning would increase drift and operational risk.

## Decision

1. Manage infrastructure with Terraform.
2. Organize code into reusable modules.
3. Isolate configuration by environment.
4. Use remote Terraform state for safe collaboration.
5. Provision core platform metadata resources via IaC:
   - data locations
   - Unity Catalog catalogs/schemas
   - RBAC permissions

## Consequences

- Infrastructure is reproducible and reviewable.
- Environment setup becomes consistent.
- Infra changes follow the same workflow as application code.

## Alternatives Considered

- Manual provisioning: rejected (non-reproducible).
- Mixed manual + IaC: rejected (configuration drift).
