# ADR-014 - AWS Validation Infrastructure Operating Model

## Status
**Accepted**

## Context

The repository now has a concrete AWS validation slice, but several important
infrastructure choices were only implicit in Terraform and CI workflows. That
created ambiguity around what should stay persistent between runs, which
resources belong to the bootstrap layer, and which sizing and topology choices
belong to this Phase 1 validation scope versus a future long-lived production
platform.

The project also needs a clear answer to the ECR lifecycle question. Validation
images are built and pushed before the environment stack can be applied with a
real image digest. If ECR repositories were treated as disposable environment
resources, the build and promotion path would break across runs.

## Decision

Adopt the following AWS operating model for the Phase 1 cloud validation slice:

1. Keep the generic Terraform governance model in ADR-006, and formalize the
   AWS-specific operating rules here.
2. Persist bootstrap prerequisites across runs:
   - Terraform remote state bucket
   - `test` data bucket
   - `test` artifact bucket
   - `prod` data bucket
   - `prod` artifact bucket
   - `test` ECR repository
   - `prod` ECR repository
3. Keep the deployed `test` and `prod` runtime environments persistent by
   default, even though they remain cost-aware validation environments rather
   than final production hardening targets.
4. Keep environment provisioning, repair, and teardown as explicit Terraform
   operations outside the normal code deployment workflows.
5. Require code deployment workflows to fail clearly when runtime prerequisites
   are missing instead of trying to provision infrastructure implicitly.
6. Keep analytical evidence and deployment support artifacts in S3 across runs.
7. Standardize the deployed AWS region on `eu-west-1`.
8. Use a dual-AZ validation topology with:
   - two private subnets, one per AZ
   - two public subnets, one per AZ
   - NAT gateways in the public subnets
   - an S3 gateway endpoint inside the VPC
9. Keep MWAA `PRIVATE_ONLY` and use the in-VPC control-plane Lambda for run
   triggering and polling.
10. Size the Phase 1 validation slice for realistic integration coverage rather
    than steady-state production resilience:
    - MWAA environment class `mw1.micro`
    - ECS task size `2048` CPU / `8192` MiB memory
    - RDS PostgreSQL `db.t4g.micro`
    - RDS single-AZ with `multi_az = false`
    - RDS backup retention `0`
    - CloudWatch log retention `7` days

## Consequences

- The repository now distinguishes clearly between bootstrap resources and
  persistent runtime resources.
- Validation runs remain production-like enough to exercise networking,
  orchestration, secrets, and managed services without mixing provisioning into
  every code deploy.
- ECR repositories remain available for build-before-apply and immutable digest
  promotion across runs.
- The AWS validation slice is intentionally cost-optimized and should not be
  mistaken for a final production hardening posture.

## Alternatives Considered

- Destroy ECR repositories after every run: rejected because image publication
  happens before environment apply and promotion depends on stable repositories.
- Destroy the full runtime stack after every deployment or validation run:
  rejected because it adds too much delivery complexity for the current phase
  and makes code deployment harder to reason about.
- Use a single-AZ or single-subnet shortcut for validation: rejected because it
  would reduce realism too far for the networking and orchestration path the
  project wants to validate.
