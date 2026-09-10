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
Dispatch the Release workflow with the bump commit as `sha` — the **full**
40-character SHA; `actions/checkout` reads a short one as a branch name and
the run fails at checkout. The run reuses the image it already published.
The run then finishes the tag and the release.

The push-triggered run detects a bump by diffing `mix.exs` at the pushed
head against its parent, so the bump commit must *be* the head of the push.
A stack merged with `gh stack merge --rebase` lands its commits in order:
put the version bump in the stack's **top** PR, or the run sees no
version-line change and the release must be dispatched by hand at the
bump commit — which then tags that commit, not the head (0.19.0 was
released this way, one commit below `main`'s head).

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

### A retire is O(claim), and replies before its maintenance (T-459)

A partition holding 140,000 unsealed entries could not drain: the storage node's
seal succeeded, and the `:retire` call that stamps the entries timed out, 161
times in two hours, so the same claim was re-sealed forever. Inside that call,
retiring one member of a claim looked its siblings up by scanning every entry
of the table with its stats — about a gigabyte copied out of ETS per retire at
that depth — then re-derived every live claim with another scan, and then ran
the maintenance the retire made due (the grace reaper's drop with its own
replication round, a log compaction that rewrites the whole manifest, the next
claim) before replying, all against a 5 s budget the endpoint never widened.
The siblings now come from the live-claim cache and leave it by name, the
maintenance runs on a message behind the calls already waiting, the retire's
replication round runs before it takes the log so the group commit never waits
on it, and the call waits `control_timeout_ms` (15 s), the budget the sealer's
transport already waited, answering a named error rather than an exit past it.
Buffer-side only; no protocol change.

### A buffer node refuses commits once its unsealed backlog is too deep to survive (T-457)

A buffer node accepted commits at whatever rate a client sent them, however
far behind its sealing was. One stalled seal path grew a node's hot manifest to
331,000 unsealed entries, and the boot peak of adopting a backlog scales with
it at roughly 15 KB per entry: buffer-2 needed 7,666 MiB to adopt on boot, could
not inside 4 Gi, and crash-looped instead of running the heal that would have
drained it. The limit was raised to 6 Gi and then 8 Gi in one day and the
backlog outgrew each. Every node now keeps its unsealed entries and bytes as
O(1) counters and refuses a commit with a retryable 429 (`retry-after: 5`,
naming the table and the depth) once the count reaches
`SMOLQUERY_BACKLOG_MAX_ENTRIES` — derived from the container's cgroup memory
limit so a node can always adopt what it holds, `200000` without one — or the
bytes reach `SMOLQUERY_BACKLOG_MAX_BYTES` when set. The valve is on ingest
only: sealing keeps draining a node that refuses, and replicated shipments are
never refused, though a shipped copy counts toward the follower's depth (it is
memory at its next boot), so a node can pass its ceiling by what its owners
ship it; size the ceiling to the replication factor if the copies are the
larger share. `smolquery_buffer_unsealed_entries`, `_bytes` and
`_entries_limit` are on `GET /metrics`; alert on the ratio well before one.
The ceiling is on the `buffer shape:` boot line. Clients that retry on 429
need no change; ones that treat 429 as fatal now lose a batch a full node
would previously have taken and later died holding.

### Compaction takes its own catalog connection, and backs off when it fails (T-458)

A storage node's catalog engine now carries two connections: one for seal
commits, retention and GC, and one the compactor alone commits through. The
compaction swap holds a connection for as long as DuckLake takes to read the
merged file's footer, and on the sandbox that was minutes per attempt, on
repeat: `SELECT 1` on the shared connection waited 34.77 s while every seal's
catalog call timed out at 30 s, so a compacting node could not seal and the
buffer backlog it existed to drain grew instead. Separately, a table whose
compaction fails now waits `SMOLQUERY_COMPACT_BACKOFF_BASE_MS` (default ten
minutes, two sweeps) before the sweep looks at it again, doubling per
consecutive failure up to `SMOLQUERY_COMPACT_BACKOFF_MAX_MS` (four hours);
`smolquery_compaction_backoffs_total` counts the deferrals and the log
escalates to an error at five in a row. Both are storage-side; no protocol
changes. One more DuckDB connection per storage pod.

The merges that failed were OOMs under a configured
`SMOLQUERY_STORAGE_COMPACT_MEMORY_LIMIT` of 512 MiB in a 6 Gi container whose
derived quarter is 1536 MiB. That override predates T-452, when storage pods
held 3.7–4.2 GiB; they now hold about 1 GiB, so unset it and let the quarter
derive. The boot log prints the derived value beside a configured one.

### The hot read is lazy for files with column ids (T-453)

The planner's view reads each group of same-id micro-segments with a plain
`read_parquet([...])`, no `union_by_name`: DuckDB binds the schema from the
first file and opens the rest as the scan reaches them, so a `LIMIT` over a deep
hot tier stops after a handful of files for every statement shape, and a full
scan no longer pays every footer up front (`bench/results/planner.md`: a
`WHERE … LIMIT 10` over 8,000 hot files 3.8 s → 0.4 s, a `count(*)` 51 s → 7 s).
Files written before column ids (PL-62) still read by name, eagerly; T-455
removes that path, after which a pre-id hot file fails the query with a clear
error rather than being read.

### DuckDB returns the encode's pages: allocator_background_threads on by default (T-452)

Every DuckDB instance now opens with `allocator_background_threads = true`, and
`Smolquery.Allocator` re-asserts it every second: the flag is one per OS
process, and libduckdb forces it off whenever any instance closes — a query
pod closes a job engine per query — so a `SET` at boot alone would be undone
within seconds.
Measured in `bench/results/buffer.md`: after six bursts of four concurrent 94 MB
encodes a buffer process settled 1.1–1.5 GiB above its start without it and
~50–90 MiB above with it, at the same wall time — the pages jemalloc kept were
the buffer pods' steady anon climb (T-451). The burst's own peak is unchanged,
so `encode_transient_bytes` on the shape line still sizes the container. One
background thread for the process. `SMOLQUERY_ALLOCATOR_BACKGROUND_THREADS=false`
turns it back off.

### Memory on /metrics, and the encode budget on the shape line (T-451)

The buffer pods' OOM kills were not root-caused in place: nothing at rest
predicted them, and the number that kills a pod — what the cgroup charges the
container — was readable only by hand inside it. Every node now publishes
`smolquery_memory_cgroup_bytes{kind}`, `smolquery_memory_cgroup_peak_bytes`
(the highest charge of the last 60 s, so a 15 s scrape still sees a spike),
`smolquery_memory_cgroup_limit_bytes`, the kernel's cumulative
`smolquery_memory_cgroup_events_total{kind="max|oom_kill"}`, the resident set
and the BEAM's split on `GET /metrics` (`Smolquery.MemoryMetrics`, sampling
every 250 ms). Point the scraper at the metrics listener of every buffer pod;
alert on `rate(smolquery_memory_cgroup_events_total{kind="max"}[5m]) > 0`.

Separately, `bench/results/buffer.md` sized one candidate: one commit's
DuckDB encode peaks near three times its NDJSON body, outside DuckDB's and the
BEAM's own accounting, and `encode_concurrency` of them run at once — 94 MB
commits four at a time are ~1.1 GiB per burst, retained by the allocator
afterwards. The buffer shape line now prints `encode_transient_bytes` and warns
when it exceeds a quarter of the container's limit; lower `flush_max_bytes` or
`encode_concurrency`, or raise the limit, until it does not.

### Claim retries back off, and a diverged follower claim heals (T-450)

A follower holding every id of an owner's claim under a claim the owner never
froze used to refuse that claim identically on every attempt, and the owner
re-attempted on every flush — three to six times a second on the sandbox, for
over ten minutes, with no path out. The owner now releases the follower's
claim and applies its own, the same way a diverged *release* has healed since
T-297; a follower claim holding ids the owner already sealed still fails
loudly, naming them. Independently, a failed claim now waits before the next
attempt — 250 ms, doubling per consecutive failure up to `seal_retry_ms`
(default 30 s) — so a refusal nobody anticipated costs one warning per
interval, not one per flush. Both are owner-side; no protocol changes.

### The query planner reads the hot manifest without stats when it cannot prune (T-449)

A plan with no WHERE conjunct and no Top-N bound on a table now fetches that
table's manifest as `GET …/manifest?stats=false`, and an unordered, unfiltered
`LIMIT n` reads only the newest micro-segments that hold n rows. Both are
query-side; the one protocol change is the query parameter. A buffer node from
before this release ignores it and answers with the stats, so a mixed-version
rollout costs bytes and nothing else — upgrade buffer nodes to get the saving.
The table page's preview of a table with a deep seal backlog went from
seconds per thousand hot files and a gigabyte of BEAM memory per query to
milliseconds; it was what OOMKilled the sandbox's query pods.

### Column changes, column ids, and materialized columns (0.19.0)

Tables can now add and drop columns (`POST`/`DELETE .../columns`, or `ALTER
TABLE` on either edge) and carry `MATERIALIZED` columns ([api.md](api.md#ddl),
[api.md](api.md#materialized-columns)). Three things change for an operator.

- **Every file is read by column id** (PL-62). A file this release writes
  stamps its column ids; a file written before it carries none and is read by
  name — in the sealed tier as of the snapshot it was registered at, which is
  exact, and in the hot tier plainly, until it seals. The one rule that
  leaves: after deploying, **wait one seal cycle before re-adding a name you
  dropped earlier**, so no unstamped micro-segment still carries the old
  column. Compaction rewrites the sealed tier with ids over time; nothing has
  to be run by hand.
- **Buffer nodes attach the metadata database** (T-439). The buffer confirms
  a batch's column ids against the catalog before writing them — one
  `schema_version` read per micro-segment — so a buffer-only node now needs the
  same reach to `SMOLQUERY_CATALOG` / `CATALOG_DATABASE_URL` that a storage
  node has. It resolves the same configuration every other role does; a node
  whose configuration names no metadata database runs without the check and
  logs nothing, so a bare test peer still boots. One more DuckDB instance per
  buffer node, with the catalog attached, carrying `catalog_connections`
  (default 4) connections so a node's tables do not queue their flushes
  behind one another's check.
- **Every ingest node hears a column change at once.** A change broadcasts
  cluster-wide over `Smolquery.PubSub` and drops the table from every ingest
  schema cache; `schema_cache_ttl_ms` is only the backstop for a node the
  broadcast does not reach. The buffer's id check is what makes even that
  case safe: a stale write is refused and retried, never stored under the
  wrong column.

There is no privilege split between reading and changing a table: the API
key, or the wire password, that can `SELECT` can `ALTER TABLE`. Rolling
order does not matter for this release; a new query node's `ALTER` is
readable by an old node the moment the catalog commits it, and an old node
that never heard of ids reads every file by name, as it always did.

### The Polars flush writer is gone (PL-57)

`SMOLQUERY_FLUSH_WRITER` is no longer a setting. DuckDB writes every flush,
which was already the default. A deployment that still sets it to `duckdb`
boots with a warning: unset it. A deployment that still sets it to `polars`
fails the boot with a message that names this note, because its writer would
change under it. Nothing else changes for a deployment that never set it.

A deployment that had pinned `polars` has a rollout order. A new ingest node
always forwards NDJSON bytes; an old buffer node on `polars` refuses them, and
the new node answers `503 UNAVAILABLE` for that table until the buffer node is
upgraded. Roll the buffer nodes first, then the ingest nodes, and the window
closes. No data is lost in the window: the request is refused, not dropped.

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
