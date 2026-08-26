# Deployment

This document covers three topics:

- How a release is published.
- What the release artifacts contain.
- How to upgrade a deployment that holds data.

For the full environment-variable reference, see
[configuration.md](configuration.md).

## How a release is published

Every merge to `main` publishes an image. A **version bump** also creates a
versioned release. A version bump is a `main` commit that raises the
`version:` line in `mix.exs` to a stable `X.Y.Z` value.

The pipeline runs in this order:

1. A pull request runs the continuous integration (CI) workflow and the
   Cluster workflow. The Cluster suite runs against real kind hosts. Both
   workflows gate the merge. Branch protection keeps the pull request
   current with `main`. Thus CI already saw the merged tree.
2. The Release workflow runs on the merge push to `main`. It publishes a
   multi-architecture `ghcr.io/chasers/smolquery` image tagged with the
   commit (`sha-<commit>`).
3. When the commit bumps the version, the run aliases that image as
   `vX.Y.Z`. The run also creates the GitHub release.

A release run that fails after the merge does not need a new version bump.
Dispatch the Release workflow with the bump commit as `sha`. The run reuses
the image it already published. The run then finishes the tag and the
release.

Pin deployments to the image digest, not to a tag. The digest is the
durable reference.

## Release artifacts

Each release attaches two files:

- `release-image.txt` — the immutable digest reference for the image.
- `release-manifest.yaml` — the `deploy/base` manifest with every smolquery
  image pinned to that digest.

The manifest is **not** a standalone production deployment. Provide these
items before you deploy it:

- the `smolquery-env` Secret,
- the Postgres catalog and discovery database,
- the sealed-store dependencies.

## Where queries run

Query jobs run on the nodes that hold the `query` role. In `deploy/base`,
that is the **API pod**: `smolquery-api` runs `api,ingest,query,web`, so
every query job's private DuckDB engine starts in the same pod as the HTTP
endpoint that received the request. Buffer and storage pods run no query
engines.

The link between the API edge and the query service is node-local.
`Smolquery.QueryService.Client` starts the job on the node it is called on.
A node with `api` but without `query` refuses queries with
`query_service_unavailable`.

### Adding query capacity

A distributed query (PL-49, on by default) shards its scan across every
node in the cluster that holds the `query` role. To add scan capacity
without adding HTTP replicas, deploy pods with `SMOLQUERY_ROLES=query`:

- They join the query service's `:pg` group on boot and take shard work
  from the API pods' jobs, with no configuration change elsewhere.
- They need the same secrets the API pod has for reading data: the
  catalog URL, the object-store credentials, and `SMOLQUERY_BUFFER_REPLICAS`
  (a query node's planner fails a read when an expected buffer node does
  not answer, T-94).
- They expose no HTTP listener. The API pods keep the `query` role, so they
  still coordinate every job and serve shards of their own.

`deploy/base` ships no such StatefulSet yet. A `query`-only pod is a scan
worker, not a coordinator: nothing routes a request from an API pod to it
as the job's owner. Taking the `query` role off the API pods would need
that forwarding step first.

Size the worker engines with `SMOLQUERY_DISTRIBUTED_WORKER_MEMORY_LIMIT`
and `SMOLQUERY_DISTRIBUTED_WORKER_THREADS`. A scattered query's declared
budget on one node is the worker count `×` the worker limit, on top of the
job engine's own `job_memory_limit`.

## Upgrade notes

One note per release, newest first.

### Per-table partition counts (T-304)

The release that ships T-304 lets a table raise its own write-partition
count with `PATCH {"partitions": N}`. A query node on an older release
ignores catalog counts. It expands only `SMOLQUERY_WRITE_PARTITIONS`
partitions, so it answers short while upgraded ingest nodes fill more.

**Do not raise a table's count until every node runs this release.**

### Claim release and the retire fence (T-294)

**Roll storage nodes before buffer nodes** during the one rollout that
ships T-294. The release fences retirement on the claim's keys. A sealer
from the previous release retires without keys, and a keyless retire skips
the fence. An old storage node's in-flight seal attempt can therefore still
stamp a since-released claim's entries sealed — the exact pre-fix exposure.
The window ends when the last old sealer drains. With storage rolled first,
a claim released by a new buffer never has an old sealer's attempt
outstanding.

### Release tombstones (T-386) and owed replica drops (T-390)

The rollout that ships T-386/T-390 adds four manifest-log record types
(`tombstone`, `reconciled`, `drop_owed`, `drop_settled`) and a `:reconciled`
replica mutation. New code reads old logs; old code refuses a new log's
records, so **do not downgrade a buffer node past this release once it has
released an oversized claim or compensated a failed replicated flush**. An
old replica refusing the `:reconciled` mutation costs a retry interval per
attempt until the rollout completes. A release or a compensation logged by
the previous version leaves no tombstone and no owed drop; those keep the
pre-fix exposure and close as they drain.

### Web role credentials (0.7.1)

From 0.7.1, the `smolquery-env` Secret must hold three values for any pod
whose roles include `web`:

- `SMOLQUERY_WEB_USERNAME`
- `SMOLQUERY_WEB_PASSWORD`
- `SMOLQUERY_SECRET_KEY_BASE`

A web pod without them **refuses to boot**. That boot failure stops the
pod's other roles too. Push the secrets before you roll the image.

## Sizing write partitions

**Size the partition count for seal drain, not for ingest spread.** Sealing
is the slow stage: each partition seals one claim at a time, so one
partition's seal throughput caps that partition's sustainable ingest.

A raise helps sealing twice:

- It multiplies concurrent seals. One table can seal on
  `min(P, N) × max_concurrent_seals` slots.
- It divides each partition's ingest, so each seal claim is smaller.

Ingest does not need the extra split. The extra split does not hurt it.

Two ways to raise the count:

1. Raise one backed-up table online: `PATCH {"partitions": N}` (T-304).
2. Raise the fleet default: set `SMOLQUERY_WRITE_PARTITIONS` and roll the
   fleet.

Costs rise with the count. Each partition adds a `TableBuffer`, a manifest,
a claim, and one hot-manifest fetch per manifest URL on every plan that
touches the table. Smaller per-partition flushes also dilute group commit.
The cap is **64**.

Compaction does not use partitions. It shards on `{table, bucket}`
(`SMOLQUERY_COMPACT_BUCKET_MS`), so its throughput scales with bucket width,
storage pod count, and the compaction engine's resources. The other seal
levers are `max_concurrent_seals`, the storage memory limits, and pod count.

## Catalog format upgrades

Two versions must agree for a node to run:

- The **catalog format** lives in the shared metadata database. DuckLake
  stamps it into the metadata tables.
- The **extension version** ships in the image, inside the pinned DuckDB
  driver.

A node can attach a catalog only when its extension supports the catalog's
format. Most DuckDB pin bumps keep the format. A pin bump that raises the
format is a hard barrier (the 0.4 → 1.0 raise arrived with DuckDB 1.5.3).
A rolling upgrade cannot run nodes on both sides of that barrier.

### What each mismatch does

- **New extension, old catalog**: the node refuses the attach. The node
  crash-loops at boot. Nothing changes. This is the default behavior. It is
  an interlock: the rollout halts visibly before an irreversible change.
  Old pods keep serving. Rollback is a redeploy of the old image.
- **Old extension, migrated catalog**: every catalog operation fails. The
  only recovery is a restore of the metadata database from a snapshot.

### The migration flag

`SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION=true` turns the refusal into a
migration. The first node with the new extension that attaches rewrites the
shared catalog to its format, in place. The migration is **one-way**.

From that instant, every pod with the old extension fails its catalog
operations: queries, seals, and commits. The failures continue until the
rollout replaces the pod. That window is an availability gap, not
corruption.

The old pods' statements fail against tables they no longer understand.
DuckLake's metadata operations stay transactional throughout.

The flag defaults to `false` because the failure modes are not symmetric.
With the flag off, an accidental format-bumping upgrade costs a redeploy.
With the flag on, the same accident cuts every old pod off from the
catalog. The old pods can never use the catalog again. The only rollback
is a database restore.

### Upgrade procedure

Use this procedure for a format-bumping upgrade on a deployment with data:

1. **Snapshot the metadata database.** The snapshot plus the old image is
   the full rollback plan.
2. Set `SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION=true` in the environment the
   pods read. Confirm the value reaches the pods.
3. Roll the new image. The first pod to attach migrates the catalog. Expect
   errors from old pods until the rollout completes.
4. Verify a write, a query, and a seal.
5. Unset the flag. A dev or sandbox cluster can keep the flag on as a
   deliberate trade: self-healing rollouts instead of the interlock. Keep
   the interlock when a catalog restore is costly.

### Known boundary

Parallel StatefulSet rollouts can attach several stale-catalog pods at the
same moment. Each pod requests the migration. DuckLake runs the migration
inside the attach's transaction. However, this codebase has no test that
pins concurrent migration of one catalog.

If a failed first boot is not acceptable, roll one pod first. Let that pod
migrate alone.
