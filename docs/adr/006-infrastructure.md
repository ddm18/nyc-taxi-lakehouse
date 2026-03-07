# ADR-006 – Infrastructure as Code Model

## Status
Accepted

## Context

The data platform requires reproducible and version-controlled infrastructure provisioning.

Infrastructure components include:

- cloud storage locations for data layers
- Unity Catalog objects (catalogs, schemas, permissions)

Managing these resources manually would make the platform difficult to reproduce and evolve.

## Decision

1. **Infrastructure is managed using Infrastructure as Code (IaC).**

   Terraform is used to provision and manage platform infrastructure.

2. **Infrastructure code is organized into reusable modules.**

   Common infrastructure components are defined in Terraform modules.

3. **Environments are managed separately.**

   Each environment defines its configuration independently.

4. **Terraform state is stored remotely.**

   Remote state management is used to ensure safe collaboration and prevent state conflicts.

5. **Infrastructure provisioning includes core platform metadata objects.**

   Terraform provisions the foundational infrastructure required by the platform, including:

   - data storage locations
   - Unity Catalog catalogs
   - Unity Catalog schemas
   - access permissions (RBAC)

   Additional platform resources (such as compute policies or workflow definitions) may also be provisioned through Terraform as the platform evolves.

## Consequences

- Platform infrastructure becomes reproducible and version-controlled.
- Environment provisioning is automated and consistent.
- Infrastructure changes follow the same review and deployment workflow as application code.

## Alternatives Considered

**Manual infrastructure provisioning**

Rejected because it makes environments difficult to reproduce and increases operational risk.

**Mixed manual + IaC provisioning**

Rejected because it introduces configuration drift between environments.