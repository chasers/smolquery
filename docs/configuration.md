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
| `SMOLQUERY_WEB_IP` / `SMOLQUERY_WEB_PORT` | web UI bind (`127.0.0.1` — exposing the unauthenticated UI is a deliberate act / `4002`) |
| `SMOLQUERY_SECRET_KEY_BASE` | signs web UI sessions; generated per boot when unset (sessions reset on restart) |
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
| `SMOLQUERY_HOT_SERVER_PORT` | port `HotServer` binds to serve micro-segments over `httpfs` (`4001`), on every node — peers derive each other's hot-tier URLs from node name plus this port |
| `SMOLQUERY_HOT_SERVER_IP` | `HotServer` bind (`127.0.0.1` single-node, `0.0.0.0` once clustered) |
| `SMOLQUERY_BUFFER_BASE_URL` | where the sealer and query planner reach the hot tier on a single node (`http://127.0.0.1:4001`) |

### Clustering

| variable | effect (default) |
|---|---|
| `CATALOG_DATABASE_URL` | Postgres URL (e.g. `postgres://user:pass@host/db`). This is what makes a cluster: it tiers the DuckLake catalog onto Postgres (loading DuckDB's `postgres` extension alongside `ducklake`) and enables node discovery (`Smolquery.Cluster`, over `libcluster_postgres`) through that same database. One node is not a cluster, so a single-node deployment leaves it unset. `SMOLQUERY_CATALOG` overrides just the catalog side, e.g. to point it at a different database than discovery uses |
| `SMOLQUERY_BUFFER_NODES` | the buffer fleet this deployment expects, as comma-separated node names. A reader fails a query when one of these cannot answer, instead of counting its unsealed rows as zero — `:pg` membership drops a crashed node exactly the way it drops a drained one, and only configuration can tell them apart. Scaling down is drain, stop, *then* remove from this list. Unset (single-node, dev), the live ring is the whole set and nothing changes |
| `SMOLQUERY_BUFFER_REPLICAS` | on Kubernetes, the buffer StatefulSet's replica count: `rel/env.sh.eex` expands it into `SMOLQUERY_BUFFER_NODES` using pod-DNS naming (`SMOLQUERY_BUFFER_STATEFULSET`, default `smolquery-buffer`), so the expected fleet carries no hardcoded namespace and scaling is one number. Ignored if `SMOLQUERY_BUFFER_NODES` is set explicitly |
| `GEN_RPC_PORT` | inter-node transport port (`5369`) |
| `GEN_RPC_TLS` | `true` to switch buffer/query inter-node traffic to mutual TLS. Verification is chain-only against the cluster CA (the emqx gen_rpc fork does no hostname/CN check), so the CA is the trust boundary: certificate files are per node (`GEN_RPC_TLS_DIR`, default `/etc/smolquery/gen-rpc-tls`; `POD_NAME` names the file) but any CA-signed certificate authenticates to any peer — a leaked node cert means rotating the CA, not just that node |
| `GEN_RPC_SSL_PORT` | gen_rpc TLS port (`5870`) |
| `DIST_TLS` | `true` to run Erlang distribution (cluster membership only) over TLS with the same certificates — set in `rel/env.sh.eex`, not `config/runtime.exs`, since distribution starts before the release's Elixir config does |
| `POD_NAME` / `POD_NAMESPACE` | when set (a Kubernetes Downward API convention), `rel/env.sh.eex` derives `RELEASE_NODE` from the pod's stable headless-service DNS name — the same name a peer needs to reach this node |
| `HEADLESS_SERVICE` | the headless-service name in that derived node name (`smolquery-headless`) |
| `RELEASE_NODE_HOST` | overrides the derived host part of `RELEASE_NODE` outright (non-StatefulSet deployments) |

## Application config

The defaults, as a Mix project sees them:

```elixir
config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  threads: System.schedulers_online(),
  extensions: [:httpfs, :json]

config :smolquery, :data_dir, "priv/data"

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"

config :smolquery, Smolquery.BufferService,
  dir: "priv/data/buffer",
  flush_interval_ms: 1_000,
  flush_max_rows: 100_000,
  flush_max_bytes: 8_000_000,
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
which overrides the application config.

### The buffer service

`:dir` is the buffer's root: micro-segments go to a `Store.Local` beneath
`segments/`, manifest logs to `manifests/`. They are separate because they
answer to different rules — segments can move to another store, while the log
stays on the node that gave the ack. Point the segments elsewhere with
`store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}`.

`flush_interval_ms` is the ack-latency dial: a batch waits out the remainder of
the current group commit, so lowering it trades throughput for latency.

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
