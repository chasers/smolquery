# Configuration

smolquery is configured by environment variables in a release (resolved at boot
in `config/runtime.exs`) and by application config in a Mix project. The
environment variables are the deployment surface; the application config is the
full set of dials behind them.

## Environment variables

### Roles and the front door

| variable | effect (default) |
|---|---|
| `SMOLQUERY_ROLES` | which service subtrees start — `all`, or a comma-separated subset of `api,ingest,buffer,storage,query,web` (all). An unknown name fails the boot |
| `SMOLQUERY_API_KEY` | the Bearer key every `/v1` route requires; a node with the `:api` role and no key refuses to boot |
| `SMOLQUERY_API_IP` / `SMOLQUERY_API_PORT` | API bind (`0.0.0.0` in the prod image / `4000`) |
| `SMOLQUERY_WEB_IP` / `SMOLQUERY_WEB_PORT` | web UI bind — expose the listener only on purpose (`127.0.0.1` / `4002`) |
| `SMOLQUERY_WEB_USERNAME` / `SMOLQUERY_WEB_PASSWORD` | the basic-auth credential every UI route requires; a node with the `:web` role and no credential refuses to boot |
| `SMOLQUERY_WEB_HOST` | the public host of the UI; also the default `check_origin` source (`localhost`) |
| `SMOLQUERY_WEB_CHECK_ORIGIN` | `false` to accept any websocket origin, or a comma-separated origin list — each entry needs a scheme or a leading `//`, e.g. `https://ui.example.com` (the `SMOLQUERY_WEB_HOST` value) |
| `SMOLQUERY_SECRET_KEY_BASE` | signs the web UI session that guards the LiveView socket; **required** on a node with the `:web` role, at least 64 bytes (`mix phx.gen.secret`), same value on every `:web` node |
| `SMOLQUERY_INTERNAL_SECRET` | what internal HTTP proves itself with; generated per boot on a single node, required explicitly in a cluster or reads fail with 401s |

### Storage and the catalog

| variable | effect (default) |
|---|---|
| `SMOLQUERY_DATA_DIR` | the one directory everything durable lives under (`/data` in the image) |
| `SMOLQUERY_BUFFER_DIR` / `SMOLQUERY_SEALED_DIR` | split a tier onto its own disk (under the data dir) |
| `SMOLQUERY_CATALOG` | DuckLake metadata database, e.g. `postgres:dbname=smolquery` (the data dir's SQLite) |
| `SMOLQUERY_SNAPSHOT_KEEP_MS` | the time-travel promise; must exceed the longest pinned query and `retire_grace_ms` (`86400000`) |
| `SMOLQUERY_S3_BUCKET` | puts the sealed tier on an S3-compatible store: points both the storage service's and the query service's `store:` at `Segments.Store.S3` |
| `SMOLQUERY_S3_ACCESS_KEY_ID` / `SMOLQUERY_S3_SECRET_ACCESS_KEY` | S3 credentials (required with `SMOLQUERY_S3_BUCKET`) |
| `SMOLQUERY_S3_ENDPOINT` | S3-compatible endpoint (unset targets AWS S3) |
| `SMOLQUERY_S3_REGION` | S3 region (`us-east-1`) |
| `SMOLQUERY_S3_URL_STYLE` | `path` or `vhost` (`path` when an endpoint is set) |
| `SMOLQUERY_S3_STAGING_DIR` | local scratch for segments before upload (`<data dir>/sealed-staging`) |

### Engine and the write path

| variable | effect (default) |
|---|---|
| `SMOLQUERY_MEMORY_LIMIT` | per-engine DuckDB memory limit (`2GB`) |
| `SMOLQUERY_MAX_RESULT_ROWS` | ceiling on rows `Engine.query/3` converts to Elixir terms (`100000`, or `infinity`) |
| `SMOLQUERY_FLUSH_INTERVAL_MS` | group-commit cadence, and so the ack-latency bound (`1000`) |
| `SMOLQUERY_COMMIT_SIBLINGS` | the in-flight insert count at which the full interval applies (`5`, Postgres's `commit_siblings`). A window opening below it closes after `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` instead — waiting only buys batching when other writers are active. `0` turns the short window off |
| `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` | the group-commit window below `SMOLQUERY_COMMIT_SIBLINGS` (`5`). A few ms rather than zero so a burst's simultaneous first inserts still share one commit |
| `SMOLQUERY_FLUSH_MAX_BYTES` | the other trigger: accumulated wire bytes that force a group commit before the interval elapses (`2000000`). Whichever fires first ends the commit, so a batch size and arrival rate that reach this sooner than `SMOLQUERY_FLUSH_INTERVAL_MS` make the interval decorative — raise it to let the cadence actually govern, at the cost of resident bytes per table |
| `SMOLQUERY_MAX_BUFFERED_BYTES` | the admission ceiling on one table's accumulator, past which a write is refused with `buffer_full` (`64000000`). Must stay comfortably above `SMOLQUERY_FLUSH_MAX_BYTES` — the accumulator overshoots the flush trigger by up to one batch |
| `SMOLQUERY_ENCODE_CONCURRENCY` | how many of a table's Parquet encodes may run at once (the node's scheduler count — a container held to one core encodes serially, which is what one core means); the manifest append, replication round and replies stay serialized in the Committer regardless. The scheduler count follows cpusets, not CFS quotas — set this and the pool size explicitly where quotas are the fence |
| `SMOLQUERY_FLUSH_WRITER` | which writer turns a flush into Parquet: `duckdb` (default) or `polars`. `duckdb` also stops the ingest edge parsing — the NDJSON body is forwarded to the owning buffer as bytes and one `COPY ... read_json` parses, sorts and writes it at flush. The default path defers schema validation to flush, then salvages a failed batch row by row, preserving per-row `insertErrors`; a bad row does not always fail the whole commit, and successful rows are still written. `/insert` accepts NDJSON only; the JSON-array envelope was removed |
| `SMOLQUERY_WRITE_POOL_SIZE` | how many DuckDB instances a `duckdb` flush writer runs (the node's scheduler count, capped at `32`; valid `1..32` — boot refuses anything else). Selected per segment, not per table, since a table has one committer and hashing on it would send every flush to one connection. Each member gets `Smolquery.Engine`'s thread count divided by the pool size (floor one) and inherits its memory limit whole — the two variables below size a member explicitly. When no explicit engine thread count is configured, both the engine and the pool resolve `System.schedulers_online()` on the deployment host at boot, not on the release builder. Deriving the count from the host means the *declared* DuckDB write memory scales with it: see `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT`, and read the resolved numbers off the `buffer shape:` line at boot |
| `SMOLQUERY_WRITE_ENGINE_THREADS` | DuckDB threads for one write-pool member, replacing the division. The division describes a budget only while the pool is smaller than the thread count — past that it sits at its floor of one, and an operator who wants a different shape states the number |
| `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT` | DuckDB memory limit for one write-pool member (e.g. `512MB`). Unset, every member inherits `SMOLQUERY_MEMORY_LIMIT` whole, so the node's declared DuckDB write budget is `write_pool_size ×` that — a size string has its own grammar, and nothing divides it for you |
| `SMOLQUERY_WRITE_PARTITIONS` | how many buffer identities one table's writes spread over, and so how many nodes ingest it (`1`). Reader and writer counts must match — set it identically on ingest and query roles |
| `SMOLQUERY_HOT_SERVER_PORT` | port `HotServer` binds to serve micro-segments over `httpfs` (`4001`), on every node — peers derive each other's hot-tier URLs from node name plus this port |
| `SMOLQUERY_HOT_SERVER_IP` | `HotServer` bind (`127.0.0.1` single-node, `0.0.0.0` once clustered) |
| `SMOLQUERY_BUFFER_BASE_URL` | where the sealer and query planner reach the hot tier on a single node (`http://127.0.0.1:4001`) |

### Clustering

| variable | effect (default) |
|---|---|
| `CATALOG_DATABASE_URL` | Postgres URL (e.g. `postgres://user:pass@host/db`). This is what makes a cluster: it tiers the DuckLake catalog onto Postgres (loading DuckDB's `postgres` extension alongside `ducklake`) and enables node discovery (`Smolquery.Cluster`, over `libcluster_postgres`) through that same database. One node is not a cluster, so a single-node deployment leaves it unset. `SMOLQUERY_CATALOG` overrides just the catalog side, e.g. to point it at a different database than discovery uses |
| `SMOLQUERY_BUFFER_NODES` | the buffer fleet this deployment expects, as comma-separated node names. A reader fails a query when one of these cannot answer, instead of counting its unsealed rows as zero — `:pg` membership drops a crashed node exactly the way it drops a drained one, and only the expected set can tell them apart. Scaling down is drain, stop, *then* remove from the expected set. Unset (single-node, dev), the live ring is the whole set and nothing changes. With replication on, up to `replication_factor - 1` of these may be absent and reads still answer completely (T-97). Under clustering (`CATALOG_DATABASE_URL`), this list only *seeds* a Postgres-backed row on first boot (T-109): after that the live set is the row, readable on every node within ~1s of a change and CAS-writable at runtime (`Smolquery.BufferService.ExpectedNodes.resize/3`) with no redeploy — editing this variable later has no effect on an already-seeded deployment |
| `SMOLQUERY_BUFFER_REPLICATION` | replication factor for the hot tier's unsealed tail (T-96). Set to `N >= 2` to enable `Replicator.SegmentShipping`: every group commit is on `N` disks before its ack, a ring smaller than `N` refuses writes, and readers tolerate `N - 1` absent buffer nodes. Set it on every role — buffer nodes ship, query nodes use it to size read tolerance. Unset: single-copy (`Replicator.None`). **Raising it on a fleet with data**: read tolerance comes from this setting, not from actual copy counts, and nothing backfills segments committed before the change — until that pre-existing unsealed tail seals, a rollout that takes a node down can silently drop it from reads. Force-seal first (drain each node, or wait out `seal_max_age_ms`) before rolling restarts under the new factor |
| `SMOLQUERY_BUFFER_REPLICAS` | on Kubernetes, the buffer StatefulSet's replica count: `rel/env.sh.eex` expands it into `SMOLQUERY_BUFFER_NODES` using pod-DNS naming (`SMOLQUERY_BUFFER_STATEFULSET`, default `smolquery-buffer`), so the expected fleet carries no hardcoded namespace and scaling is one number. Ignored if `SMOLQUERY_BUFFER_NODES` is set explicitly |
| `GEN_RPC_PORT` | inter-node transport port (`5369`) |
| `GEN_RPC_TLS` | `true` to switch buffer/query inter-node traffic to mutual TLS (`false` by default). Verification is chain-only against the cluster CA (the emqx gen_rpc fork does no hostname/CN check), so the CA is the trust boundary: certificate files are per node (`GEN_RPC_TLS_DIR`, default `/etc/smolquery/gen-rpc-tls`; `POD_NAME` names the file) but any CA-signed certificate authenticates to any peer — a leaked node cert means rotating the CA, not just that node |
| `GEN_RPC_SSL_PORT` | gen_rpc TLS port (`5870`) |
| `DIST_TLS` | `true` to run Erlang distribution (cluster membership only) over TLS with the same certificates (`false` by default) — set in `rel/env.sh.eex`, not `config/runtime.exs`, since distribution starts before the release's Elixir config does |
| `POD_NAME` / `POD_NAMESPACE` | when set (a Kubernetes Downward API convention), `rel/env.sh.eex` derives `RELEASE_NODE` from the pod's stable headless-service DNS name — the same name a peer needs to reach this node |
| `HEADLESS_SERVICE` | the headless-service name in that derived node name (`smolquery-headless`) |
| `RELEASE_NODE_HOST` | overrides the derived host part of `RELEASE_NODE` outright (non-StatefulSet deployments) |

The checked-in Kind overlays set `GEN_RPC_TLS=false` and `DIST_TLS=false`.
They still mount per-node development certificates so operators can opt into
mutual TLS by changing both values to `true` before applying the overlay.

## Releases and deployment artifacts

A push to `main` runs the Kind workflow as well as the ordinary CI workflow.
The release workflow listens for a successful main-push Kind run, checks that
its exact commit changed the `version:` line in `mix.exs` to a stable `X.Y.Z`
version strictly greater than its parent, and waits for successful CI on that same commit. It then publishes
multi-architecture `ghcr.io/chasers/smolquery` tags for the version and commit.

The release attaches `release-image.txt`, containing the immutable digest
reference, and `release-manifest.yaml`, an image-pinned base manifest rendered
from `deploy/base` with every smolquery image replaced by that digest-qualified
reference. It is not a standalone production deployment: integrate it with and
provide the `smolquery-env` Secret, Postgres catalog/discovery, and sealed-store
dependencies before deploying.

An upgrade note for existing deployments: the `smolquery-env` Secret must now
also hold `SMOLQUERY_WEB_USERNAME`, `SMOLQUERY_WEB_PASSWORD`, and
`SMOLQUERY_SECRET_KEY_BASE` for any pod whose roles include `web`. A pod
without them refuses to boot. That boot failure stops the pod's other roles
too.

## Application config

The defaults, as a Mix project sees them:

```elixir
config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  extensions: [:httpfs, :json]

config :smolquery, :data_dir, "priv/data"

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"

config :smolquery, Smolquery.BufferService,
  dir: "priv/data/buffer",
  flush_interval_ms: 1_000,
  flush_idle_interval_ms: 5,
  commit_siblings: 5,
  flush_max_rows: 100_000,
  flush_max_bytes: 2_000_000,
  max_buffered_rows: 500_000,
  max_buffered_bytes: 64_000_000,
  write_timeout_ms: 15_000,
  seal_max_bytes: 67_108_864,
  seal_max_files: 64,
  seal_max_age_ms: 60_000,
  seal_retry_ms: 30_000,
  retire_grace_ms: 600_000,
  maintenance_interval_ms: 5_000,
  seal_consumer: {Smolquery.StorageService.Client, []},
  hot_server_ip: {127, 0, 0, 1},
  hot_server_port: 4001,
  epoch_lease_ms: 10_000,
  epoch_refresh_ms: 1_000

config :smolquery, Smolquery.StorageService,
  dir: "priv/data/sealed",
  buffer_base_url: "http://127.0.0.1:4001",
  buffer_timeout_ms: 30_000,
  engine_extensions: [:httpfs],
  compression: :zstd,
  target_segment_bytes: 268_435_456,
  max_concurrent_seals: 2,
  gc_interval_ms: 300_000,
  gc_grace_ms: 3_600_000,
  compact_interval_ms: 300_000,
  compact_below_bytes: 33_554_432,
  compact_min_inputs: 2,
  compact_max_bytes: 134_217_728,
  retention_interval_ms: 3_600_000,
  snapshot_keep_ms: 86_400_000,
  handoff: {Smolquery.StorageService.Handoff.Seal, []}

config :smolquery, Smolquery.QueryService,
  buffer_base_url: "http://127.0.0.1:4001",
  buffer_timeout_ms: 30_000,
  engine_extensions: [:httpfs],
  max_concurrent_jobs: 8,
  default_timeout_ms: 60_000,
  job_memory_limit: "1GB",
  result_ttl_ms: 300_000
```

Engine options can also be passed per instance to `Smolquery.Engine.start_link/1`,
which overrides the application config. `:threads` is intentionally absent from
the defaults above: unless configured explicitly, each engine resolves it from
`System.schedulers_online()` when it starts on the deployment host, not when the
release is built.

### The buffer service

`:dir` is the buffer's root: micro-segments go to a `Store.Local` beneath
`segments/`, manifest logs to `manifests/`. They are separate because they
answer to different rules — segments can move to another store, while the log
stays on the node that gave the ack. Point the segments elsewhere with
`store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}`.

`flush_interval_ms` is the ack-latency dial: a batch waits out the remainder of
the current group commit, so lowering it trades throughput for latency.

That trade only exists when there is something to group. `commit_siblings` and
`flush_idle_interval_ms` make the wait adaptive: a window that opens with
fewer than `commit_siblings` inserts already in flight closes after
`flush_idle_interval_ms` instead. A lone writer acks at commit speed rather
than the interval, and real concurrency keeps the configured cadence.

It is only half the trigger, though, and under load usually not the half that
fires. `flush_max_rows` and `flush_max_bytes` end a commit as soon as the
accumulator reaches either, so a deployment whose arrival rate reaches
`flush_max_bytes` in less than `flush_interval_ms` is running a cadence it
never configured — the interval becomes decorative and commit size is pinned
to the byte cap instead. Two 2 MB batches against the 8 MB default is a
four-batch commit however long the interval says to wait. When tuning the
cadence, check which one is actually ending the commit: divide
`flush_max_bytes` by the offered bytes per second and compare against
`flush_interval_ms`.

`ring:` is the static fallback only — with `CATALOG_DATABASE_URL` set,
ownership instead tracks which nodes are actually alive and hosting this
instance, via `:pg` (`Smolquery.Cluster.PgGroup`); the config value only matters
again if clustering is off.

### The storage service

The storage service's own `ring:` is the static fallback for a *second*,
independent ring — which storage node seals a table's work, not which buffer
node accumulates it. With clustering on it likewise tracks live `:pg`
membership, and `Smolquery.StorageService.Routing.own?/2` is what the sealer,
compactor, retention, and GC gate on before acting. The gate is advisory, not
mutual exclusion — during a ring change two nodes can transiently both pass it;
what keeps that from double-registering a segment is the catalog re-deriving its
registration diff inside every commit retry (see
`Smolquery.StorageService.Routing` for the residual window).

`:dir` is where sealed segments land, and `:store` overrides it the same way —
including onto an object store:

```elixir
store: {Smolquery.Segments.Store.S3,
        bucket: "smolquery-sealed",
        access_key_id: "...",
        secret_access_key: "...",
        endpoint: "http://minio:9000",
        staging_dir: "/mnt/scratch/sealed-staging"}
```

In a release, configure that through the `SMOLQUERY_S3_*` environment variables
rather than a config snippet: `config/config.exs` is evaluated at *build* time,
so `System.get_env/1` there bakes the builder's credentials (or `nil`) into the
artifact. The env wiring configures the query service's `store:` with the same
values, which every job engine needs to read the sealed tier back.

`buffer_base_url` is where the sealer reaches `HotServer` to pull manifests and
segment bytes — honest for a single node. Clustered, each seal signal carries
the node it came from, and the sealer derives that node's URL from the node name
(`buffer_hot_port`), since the signal's origin — not the static config, and not
even the ring's current owner — is where the claimed bytes physically live.
`engine_extensions` loads `httpfs` into the sealer's own engine, which the merge
cannot work without; the same engine also authenticates to the sealed tier's
`Store.S3` credentials, when configured, via `CREATE SECRET`
(`Smolquery.EngineSecrets`), so a compaction re-merging existing sealed segments
can read them back over `s3://`.

### The query service

The query service runs each query as a job with a private DuckDB engine
(`Smolquery.QueryService.Client.query/3` sync, `submit/3` + `fetch/2` async).
Given `catalog:` options it starts its own DuckLake engine to plan through;
given a `%Smolquery.Catalog{}` it starts none — but then `job_bootstrap:` must
carry the `ATTACH` job engines need, since they attach the lake themselves.

`buffer_base_url` is where the planner reaches `HotServer` for hot manifests on
a single node. Clustered, the planner ignores it and fans each table's manifest
fetch out across the fleet instead — see [fan-out](architecture.md#clustered-fan-out).

`store` takes the same `Store.S3` config as the storage service's when the
sealed tier lives there — every job's engine needs the matching `CREATE SECRET`
to read it, even though the query path never writes through the store itself.
`max_concurrent_jobs` refuses rather than queues; `default_timeout_ms` bounds
every job's runtime; `job_memory_limit` is each job engine's DuckDB
`memory_limit`; `result_ttl_ms` is how long a finished job holds its result
frame for an async caller.
