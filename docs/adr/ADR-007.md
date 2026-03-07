# ADR-007 – Pipeline Processing State Tracking

## Status
Accepted

## Context

The ingestion model operates at dataset-month granularity (ADR-001).

Pipelines must support:

- retry
- backfill
- operational visibility
- bounded reprocessing

To enable these capabilities, the system must track the processing state of each dataset-month across pipeline stages.

## Decision

The platform introduces a processing state ledger stored in the operational schema.

Example table:

ops.pipeline_state

The table records the status of each dataset-month for each pipeline stage:

- ingestion
- bronze
- silver
- gold

Each pipeline updates the ledger upon successful completion.

## Consequences

- The system supports deterministic backfill.
- Failed pipeline stages can be retried safely.
- Operational visibility is improved.
- Event-driven pipelines remain idempotent.