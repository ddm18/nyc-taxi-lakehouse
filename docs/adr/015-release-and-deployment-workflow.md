# ADR-015 - Release and Deployment Workflow

## Status
**Accepted**

## Context

The project now has a concrete AWS validation slice, but it also needs an
equally concrete release workflow that answers four operational questions:

1. which branch shapes are allowed to deploy
2. which artifact is considered the deployable source of truth
3. how `test` and `prod` promotions are linked
4. where infrastructure management stops and code deployment begins

Without an explicit delivery model, GitHub Actions would become an implicit
architecture layer and future contributors would have to reverse-engineer the
release contract from workflow YAML.

## Decision

Adopt the following CI/CD workflow for Phase 1:

1. Use `develop` as the integration branch and `main` as the stable release
   branch.
2. Allow hotfixes to land directly on `main` only as an exception, followed by
   an explicit back-merge into `develop`.
3. Run CI on pull requests and on pushes to `develop` and `main`.
4. Build deployable artifacts exactly once in CI for deploy-relevant pushes to
   `develop` and `main`:
   - ECS runtime image pushed to the persistent `test` ECR repository
   - MWAA deploy bundle containing DAGs, requirements, and Python support code
5. Tag images by commit SHA and treat the immutable image digest as the
   promoted artifact identity.
6. Use `TRANSFORMATION_VERSION = commit SHA`.
7. Trigger `test` deployment automatically only after a successful CI run on
   `main`, and only when the commit touches deploy-relevant paths.
8. Keep `test` and `prod` runtime infrastructure persistent by default.
9. Keep infrastructure apply and destroy outside the normal code deployment
   workflows; deploy workflows must fail clearly if their target environment is
   missing or not ready.
10. Allow manual `test` deployment only for commits contained in `develop` or
    `main`.
11. Run `test` validation as a separate action after deployment, and record the
    result in `s3://nyc-data-platform-test-artifacts/releases/test-runs/<commit_sha>.json`.
12. Promote to `prod` only through a manual workflow that receives a Git tag in
   strict `vX.Y.Z` format.
13. Require that a `prod` release tag points to a commit already contained in
    `main`.
14. Require all `prod` promotions to reuse the exact digest already validated in
    `test`; never rebuild the image during promotion.
15. Allow `prod` only when the exact `test` validation record for the tagged
    commit exists with `status = success`, and when the corresponding CI run is
    also green.
16. Keep GitHub Release creation as a `prod` success-side effect only. A tag is
    a release candidate; a GitHub Release is an actually promoted release.

## Consequences

- The branch model is now tied to a specific release contract instead of being a
  generic Git preference.
- CI becomes the only place where deployable artifacts are built.
- `test` validates the same artifact that `prod` later promotes.
- Infrastructure lifecycle is no longer mixed into code deployment workflows.
- The machine-readable promotion gate becomes the `test` validation record,
  rather than a heavier deploy manifest chain.

## Alternatives Considered

- Rebuild the image during `prod` deployment: rejected because it breaks the
  guarantee that `prod` runs the same artifact that passed in `test`.
- Promote from commit SHA without a Git release tag: rejected because it weakens
  release traceability.
- Keep deploy and validation fused into one workflow: rejected because it makes
  it harder to separate packaging problems from pipeline execution problems.
