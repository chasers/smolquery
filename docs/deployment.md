# Deployment

This document tells you how a release is published, what the release
artifacts contain, and how to upgrade a deployment that holds data. For the
full environment-variable reference, see [configuration.md](configuration.md).

## How a release is published

A release starts with a **version bump**. The release workflow publishes an
image only when a `main` commit raises the `version:` line in `mix.exs` to a
stable `X.Y.Z` value.

The pipeline runs in this order:

1. A pull request runs the CI workflow and the Cluster workflow. The Cluster
   suite runs against real kind hosts. Both gate the merge. Branch protection
   keeps the pull request current with `main`, so the merged tree is one CI
   already saw.
2. The Release workflow runs on the merge push to `main`. It checks that the
   exact commit bumped the version.
3. It publishes multi-architecture `ghcr.io/chasers/smolquery` tags for the
   version (`vX.Y.Z`) and the commit (`sha-<commit>`).

A release run that fails after the merge does not need a new version bump.
Dispatch the Release workflow with the bump commit as `sha`; the run reuses
the image tags it already published and finishes the tag and the release.

A commit that does not bump the version skips the publish step. The image
digest is the durable reference; pin deployments to it, not to a tag.

## Release artifacts

Each release attaches two files:

- `release-image.txt` — the immutable digest reference for the image.
- `release-manifest.yaml` — the `deploy/base` manifest with every smolquery
  image pinned to that digest.

The manifest is **not** a standalone production deployment. You must provide
the `smolquery-env` Secret, the Postgres catalog and discovery database, and
the sealed-store dependencies before you deploy it.

## Upgrade notes

### Web role credentials (0.7.1)

From 0.7.1, the `smolquery-env` Secret must hold `SMOLQUERY_WEB_USERNAME`,
`SMOLQUERY_WEB_PASSWORD`, and `SMOLQUERY_SECRET_KEY_BASE` for any pod whose
roles include `web`. A web pod without them **refuses to boot**, and that
boot failure stops the pod's other roles too. Push the secrets before you
roll the image.

## Catalog format upgrades

Two versions must agree for a node to run:

- The **catalog format** lives in the shared metadata database. DuckLake
  stamps it into the metadata tables.
- The **extension version** ships in the image, inside the pinned DuckDB
  driver.

A node can attach a catalog only when its extension speaks the catalog's
format. Most DuckDB pin bumps keep the format. A pin bump that raises it
(0.4 → 1.0 arrived with DuckDB 1.5.3) is a hard barrier. No rolling upgrade
can straddle it.

### What each mismatch does

- **New extension, old catalog**: the attach is refused. The node
  crash-loops at boot. Nothing changes. This is the default behavior, and it
  is an interlock: the rollout halts loudly before anything irreversible
  happens. Old pods keep serving. Rollback is a redeploy of the old image.
- **Old extension, migrated catalog**: every catalog operation fails. The
  only path back is a restore of the metadata database from a snapshot.

### The migration flag

`SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION=true` turns the refusal into a
migration. The first new-extension node to attach rewrites the shared
catalog to its format, in place. The migration is **one-way**.

From that instant, every old-extension pod fails its catalog operations —
queries, seals, commits — until the rollout replaces it. That window is an
availability gap, not corruption. The old pods' statements fail against
tables they no longer understand, and DuckLake's metadata operations stay
transactional throughout.

The flag defaults to `false` because the failure modes are not symmetric.
Off, an accidental format-bumping upgrade costs a redeploy. On, the same
accident cuts every old pod off from a catalog they can never use again, and
the only rollback is a database restore.

### Upgrade procedure

Use this procedure for a format-bumping upgrade on a deployment with data:

1. **Snapshot the metadata database.** The snapshot plus the old image is
   the whole rollback story.
2. Set `SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION=true` in the environment the
   pods read. Confirm the value reaches the pods.
3. Roll the new image. The first pod to attach migrates the catalog. Expect
   old pods to error until the rollout completes.
4. Verify: a write, a query, a seal.
5. Unset the flag. A dev or sandbox cluster can keep it on as a deliberate
   trade — self-healing rollouts instead of the interlock. Keep the
   interlock when a catalog restore would hurt.

### Known boundary

Parallel StatefulSet rollouts can attach several stale-catalog pods at the
same moment. Each one requests the migration. DuckLake runs the migration
inside the attach's transaction, but this codebase has no test that pins
concurrent migration of one catalog. If a failed first boot is not
acceptable, roll one pod first and let it migrate alone.
