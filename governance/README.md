# Governance (Phase 1)

This folder contains high-level governance policies for the Phase 1 platform.

## Ownership by layer

| Layer | Owner |
|---|---|
| Bronze | Data platform |
| Silver | Data engineering |
| Gold | Analytical layer |

## Access model (RBAC)

| Group | Permissions |
|---|---|
| Engineers | Read/Write all layers |
| Analysts | Read Silver + Gold |
| Viewers | Read Gold only |

## Notes

- Gold is the primary consumer layer.
- Operational metadata is kept in the `ops` schema.
