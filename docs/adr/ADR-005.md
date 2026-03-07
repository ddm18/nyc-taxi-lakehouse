# ADR-005 – Data Governance Model

## Status
Accepted

## Context

The platform exposes datasets to multiple types of users with different responsibilities.

To ensure responsible data usage and controlled access, the platform requires a lightweight governance model covering:

- dataset ownership
- access control
- separation between platform layers
- controlled exposure of analytical datasets

The platform uses **Unity Catalog** to manage permissions and dataset organization.

The platform follows a **layered data architecture (landing → bronze → silver → gold)** where each layer represents a different stage in the data lifecycle.

Gold datasets are designed primarily to support **recent operational analytical workloads**, typically focused on the most recent three months of data.

---

## Decision

1. **Datasets follow a layered governance model.**

   The platform uses the following layers:

   landing → bronze → silver → gold

   Each layer has a different governance role.

2. **Dataset ownership is defined by layer responsibility.**

   - **Bronze**
     - owned by the data platform
     - represents raw ingested data

   - **Silver**
     - owned by the data engineering layer
     - represents curated, canonical datasets

   - **Gold**
     - owned by the analytical layer
     - represents business-ready datasets and metrics optimized for analytical consumption

3. **Access control is implemented through RBAC.**

   Permissions are granted to user groups rather than individual users.

   - **Engineers**
     - read/write access to all layers

   - **Analysts**
     - read access to Silver and Gold datasets

   - **Viewers**
     - read access to Gold datasets only

4. **Gold is the primary consumer layer.**

   Business users, analysts, and dashboards are expected to consume datasets only from the Gold layer.

   Gold datasets may prioritize recent analytical windows while historical data remains accessible through lower layers.

5. **Operational metadata is separated from analytical datasets.**

   Operational tables (such as data quality metrics and pipeline metadata) are stored in a dedicated operational schema:

   `ops`

---

## Consequences

- Responsibilities for each dataset layer are clearly defined.
- Business consumers interact only with curated datasets.
- Access management remains simple and scalable through group-based permissions.
- The governance model supports future extension if more advanced governance policies are required.

---

## Alternatives Considered

### Open access to all layers

Rejected because it exposes raw and intermediate datasets to business consumers.

---

## Future Governance Extensions

The Phase 1 governance model intentionally focuses on foundational governance.

Potential governance capabilities that may be introduced in later phases include:

- dataset stewardship workflows
- dataset approval lifecycle management
- centralized governance policy catalog
- data classification framework