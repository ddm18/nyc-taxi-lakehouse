# ADR-001 – Ingestion Model: Controlled Pull with Internal Event (Month Granularity)

## Status
Accepted

## Context

- Public datasets (e.g., NYC TLC) are batch-based and may be served through CDN layers (e.g., CloudFront), which may introduce throttling or transient failures.
- Phase 1 goals:
  - Reproducible ingestion
  - Structured backfill
  - Basic auditability
  - Event-driven downstream processing
- Real-time streaming is out of scope.

This decision currently applies to the NYC TLC taxi datasets included in Phase 1.
Other datasets (e.g., weather) may adopt different partitioning strategies depending on their structure and access patterns.

## Decision

The platform adopts a **controlled batch pull ingestion model** at **monthly granularity**.

1. A scheduled job ("Ingestion Controller") performs pull-based ingestion for a specific dataset and month.
2. Raw files are materialized in Landing using deterministic partition paths.
3. When ingestion for a dataset-month completes successfully, the system emits an **internal arrival event**.
4. Downstream pipelines (Landing → Bronze → Silver → Gold) are triggered based on this internal event.

The event unit is **dataset-month**.

## Consequences

- The system is internally event-driven while relying on pull-based public sources.
- Backfill is implemented by executing ingestion for historical months.
- The ingestion layer is logically separated from transformation layers.

## Alternatives Considered

- Pure scheduled pipelines without an explicit arrival event  
  Rejected: weaker control and audit semantics.

- Source-driven push/event ingestion  
  Rejected: not available for public batch datasets.

- Full event-bus architecture (SNS/SQS/Kafka)  
  Deferred: unnecessary complexity for Phase 1.