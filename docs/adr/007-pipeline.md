# ADR-007 - Pipeline Processing State Tracking

## Status
**Accepted**

## Context

Pipelines run at dataset-month granularity and must support retry, backfill, and operational visibility.
A persistent ledger is needed to track stage-level completion.

## Decision

Introduce processing-state tracking in operational metadata.

Ledger table example: `ops.pipeline_state`.

Tracked stages:

- ingestion
- bronze
- silver
- gold

Each stage updates the ledger on successful completion.

## Consequences

- Deterministic backfill execution.
- Safer retries after failures.
- Clear operational visibility per dataset-month.
- Better idempotency in event-driven orchestration.
