# ADR-001 - Ingestion Model: Controlled Pull with Internal Event

## Status
**Accepted**

## Context

- Public datasets (for example NYC TLC) are batch-oriented.
- Upstream delivery may include transient failures/throttling.
- Phase 1 requires reproducibility, backfill support, auditability, and event-driven downstream processing.
- Streaming is out of scope.

This ADR applies to NYC TLC datasets in Phase 1.

## Decision

The platform adopts controlled batch pull ingestion at monthly granularity.

1. A scheduled **Ingestion Controller** ingests one dataset-month.
2. Raw files are stored in Landing with deterministic paths.
3. On successful completion, an internal arrival event is emitted.
4. Pipelines run from Landing -> Bronze -> Silver -> Gold based on that event.

Event unit: `dataset-month`.

## Consequences

- Internally event-driven orchestration over pull-based public sources.
- Backfill is deterministic by replaying historical months.
- Ingestion remains logically separated from transformations.

## Alternatives Considered

- Scheduled-only orchestration without arrival event: rejected (weaker audit/control semantics).
- Source push/event model: rejected (not available for these datasets).
- Full event bus (SNS/SQS/Kafka): deferred (overkill for Phase 1).
