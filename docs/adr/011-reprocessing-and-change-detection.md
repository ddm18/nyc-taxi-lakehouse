# ADR-011 - Reprocessing Triggered by Transformation Version and Source Metadata

## Status
**Accepted**

## Context

Phase 1 requires deterministic historical replay for two distinct reasons:

- the transformation logic can change over time
- upstream monthly source objects can be silently republished

The original pipeline-state decision covered stage completion, but not how to
decide that a previously completed partition has become stale.

## Decision

Introduce two explicit reprocessing signals:

1. `transformation_version`
   - supplied at runtime through deployment configuration
   - recorded in `ops.pipeline_state`
   - used to detect partitions built with older logic

2. source metadata observation
   - `etag`
   - `last_modified`
   - `content_length`
   - stored under `ops/source_metadata`
   - append-only audit trail stored under `ops/source_metadata_audit`

When either signal indicates staleness, the controller records a reprocessing
memory object under `ops/reprocess_queue` and reruns the affected partition.

Phase 1 implementation notes:

- `pipeline_state` and current source metadata are active control inputs
- `reprocess_queue` and `source_metadata_audit` are retained as operational trace memory
- automatic source-republish checks are scoped to a recent audit window rather than all historical partitions

## Consequences

- Historical restatement becomes explainable and reproducible.
- The system can distinguish "source changed" from "code changed".
- The implementation stays lightweight without introducing a full workflow queue service in Phase 1.

## Alternatives Considered

- Manual-only backfill: rejected as insufficiently reproducible.
- CI/CD-triggered full historical rebuild on every deploy: rejected as too expensive and coarse.
- Full external queue/event bus for reprocessing: deferred beyond Phase 1.
