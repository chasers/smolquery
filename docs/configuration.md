# Configuration

smolquery is configured by environment variables in a release (resolved at boot
in `config/runtime.exs`) and by application config in a Mix project. The
environment variables are the deployment surface; the application config is the
full set of dials behind them. Release environment values are validated before
any service subtree starts. Numeric operational values must be positive decimal integers unless a setting
explicitly documents zero as a switch (such as `SMOLQUERY_COMMIT_SIBLINGS`),
listener ports must be in `1..65535`, IP values must be valid IPv4 or IPv6
addresses, and `SMOLQUERY_MAX_RESULT_ROWS` additionally accepts `infinity`.
Invalid values name the variable, received value, and accepted shape in the boot
error.

## Environment variables

### Roles and the front door

| variable | effect (default) |
|---|---|
| `SMOLQUERY_ROLES` | which service subtrees start — `all`, or a comma-separated subset of `api,ingest,buffer,storage,query,web` (all). An unknown name fails the boot |
| `SMOLQUERY_AUTH_MODE` | authentication mode (`static` or `oidc`); required on `:api` and `:web` nodes; OIDC starts only with the validated `SMOLQUERY_OIDC_*` settings |
| `SMOLQUERY_API_KEY` | the Bearer key every `/v1` route requires in static mode; a node with the `:api` role and no key refuses to boot |
| `SMOLQUERY_API_IP` / `SMOLQUERY_API_PORT` | API bind (`0.0.0.0` in the prod image / `4000`) |
| `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES` | the most ingest-body bytes an API node admits at once (T-245). `SmolqueryApi.Admission` counts `POST .../insert` and `.../load` bodies by declared `content-length` before any body is read, and refuses past the limit with a 429 and `retry-after: 1` — the refusal costs a header read, never a body, so a client burst sheds load instead of OOMKilling the pod. Unset, the limit is **a quarter of the container's cgroup memory limit**, floored at one NDJSON body (`8000000`); without a cgroup limit, `268435456`. An idle counter always admits one request — the route's own body cap decides what is too large |
| `SMOLQUERY_WEB_IP` / `SMOLQUERY_WEB_PORT` | web UI bind — expose the listener only on purpose (`127.0.0.1` / `4002`) |
| `SMOLQUERY_WEB_USERNAME` / `SMOLQUERY_WEB_PASSWORD` | the basic-auth credential every UI route requires in static mode; a node with the `:web` role and no credential refuses to boot |
| `SMOLQUERY_WEB_HOST` | the public host of the UI; also the default `check_origin` source (`localhost`). In OIDC mode it must match `SMOLQUERY_OIDC_WEB_ORIGIN` |
| `SMOLQUERY_WEB_CHECK_ORIGIN` | `false` to accept any websocket origin, or a comma-separated origin list — each entry needs a scheme or a leading `//`, e.g. `https://ui.example.com` (the `SMOLQUERY_WEB_HOST` value) |
| `SMOLQUERY_SECRET_KEY_BASE` | signs the web UI session that guards the LiveView socket; **required** on a node with the `:web` role, at least 64 bytes (`mix phx.gen.secret`), same value on every `:web` node |

### OIDC foundation (T-231)

OIDC mode is explicit and fail-closed. The API and web roles validate their
own required settings before their listeners start. T-232 verifies API bearer
access tokens against the supervised discovery/JWKS cache. Browser login and
per-route capability authorization are added by later stack layers. Provider
outage or malformed discovery/JWKS never opens either listener.

| variable | effect |
|---|---|
| `SMOLQUERY_OIDC_ISSUER` | exact HTTPS issuer string; trailing slash is retained, while query, fragment, and userinfo are rejected |
| `SMOLQUERY_OIDC_API_AUDIENCE` | required API access-token audience on `:api` roles; it must differ from the browser client id when both are configured |
| `SMOLQUERY_OIDC_WEB_CLIENT_ID` | required browser client id on `:web` roles; optional on API-only roles so the token verifier can reject browser-client audiences |
| `SMOLQUERY_OIDC_WEB_CLIENT_SECRET` | required only with `SMOLQUERY_OIDC_WEB_CLIENT_AUTH_METHOD=client_secret_basic`; never shown by runtime inspection |
| `SMOLQUERY_OIDC_WEB_CLIENT_AUTH_METHOD` | `client_secret_basic` (default) or `none` |
| `SMOLQUERY_OIDC_WEB_ORIGIN` | exact HTTPS public browser origin; its host must match `SMOLQUERY_WEB_HOST` |
| `SMOLQUERY_OIDC_WEB_REDIRECT_URI` | authorization callback URI; must be exactly the web origin plus `/auth/callback`, without a query |
| `SMOLQUERY_OIDC_WEB_SCOPES` | comma-separated browser authorization scopes (default `openid`); `openid` is mandatory, with at most 32 unique scope tokens and 1024 bytes total |
| `SMOLQUERY_OIDC_ALGORITHMS` | comma-separated local allowlist (default `RS256`); token or discovery metadata never expands it |
| `SMOLQUERY_OIDC_CLOCK_SKEW` | bounded non-negative seconds for later token validation (default `30`) |
| `SMOLQUERY_OIDC_CLAIM_CAPABILITIES` | optional JSON object mapping claim names to exact string values and capability arrays, e.g. `{"roles":{"reader":["query"],"operator":["web_access","query","platform_operate"]}}`; list-valued token claims union matching values |
| `SMOLQUERY_OIDC_API_TOKEN_TYPES` / `SMOLQUERY_OIDC_WEB_TOKEN_TYPES` | optional role-specific comma-separated protected-header `typ` allowlists; use these when API access tokens and browser ID tokens carry different types |
| `SMOLQUERY_OIDC_TOKEN_TYPES` | backward-compatible common `typ` allowlist used only when the role-specific setting is absent |
| `SMOLQUERY_OIDC_API_REQUIRED_CLAIMS` / `SMOLQUERY_OIDC_WEB_REQUIRED_CLAIMS` | optional role-specific JSON objects mapping required payload claim names to allowed exact string values, e.g. `{"token_use":["access"]}` and `{"token_use":["id"]}` |
| `SMOLQUERY_OIDC_REQUIRED_CLAIMS` | backward-compatible common required-claim map used only when the role-specific setting is absent |
| `SMOLQUERY_OIDC_MAX_TOKEN_BYTES` / `SMOLQUERY_OIDC_MAX_TOKEN_SEGMENT_BYTES` | bounds compact token and individual encoded segments before JOSE decoding (defaults `65536` / `32768`) |
| `SMOLQUERY_OIDC_IAT_FUTURE_SECONDS` | maximum future `iat` allowance (default `300`); `exp` contexts remain active through the configured clock-skew boundary |
| `SMOLQUERY_OIDC_DISCOVERY_MAX_AGE_MS` / `SMOLQUERY_OIDC_JWKS_MAX_AGE_MS` | bounded cache freshness windows (defaults `3600000`) |
| `SMOLQUERY_OIDC_FORCED_REFRESH_COOLDOWN_MS` | positive minimum interval between unknown-`kid` forced JWKS fetches (default `1000`); concurrent/repeated attempts reuse the current cache and fail closed if the key remains unknown |
| `SMOLQUERY_OIDC_REFRESH_FAILURE_BACKOFF_MS` | positive interval suppressing repeated discovery/JWKS network attempts after a failed refresh (default `1000`) |
| `SMOLQUERY_OIDC_CONNECT_TIMEOUT_MS` / `SMOLQUERY_OIDC_RECEIVE_TIMEOUT_MS` / `SMOLQUERY_OIDC_REQUEST_TIMEOUT_MS` | bounded Req connection, per-chunk receive, and complete-response timeouts (defaults `2000` / `5000` / `10000`) |
| `SMOLQUERY_OIDC_MAX_BODY_BYTES` | bounded discovery/JWKS response size (default `1048576`) |

The API verifier requires a non-empty `kid`, a locally allowlisted asymmetric
algorithm, a compatible public signing key, exact issuer and audience, and
integer NumericDate claims. API and web token type/required-claim profiles are
resolved separately. When the browser client ID is configured on an API role,
the verifier rejects any API token whose audience list also contains that client ID,
preventing a browser ID token from crossing the access-token boundary. Unknown keys trigger one supervised JWKS refresh,
subject to the global forced-refresh cooldown; concurrent or repeated unknown
keys within that cooldown reuse the current cache and reject without another
network fetch. All other failures reject without revealing the verification
reason. The context expiry is `exp + SMOLQUERY_OIDC_CLOCK_SKEW`, matching the
accepted expiration-skew boundary. Before T-233, an OIDC API token must map to
all three current API capabilities (`query`, `ingest`, and `catalog_manage`), so
this layer cannot accidentally grant a query-only token write or catalog access.

The discovery client requires JSON responses, byte-for-byte issuer equality,
HTTPS authorization/token/JWKS endpoints, an algorithm overlap with the local
asymmetric allowlist, unique key ids, and at least one public signing key whose
explicit algorithm and key type are compatible with that allowlist. It refuses
redirects and bounds response bodies. Refresh I/O runs
outside the cache process, so fresh reads continue while a key fetch is in
flight; expired data still fails closed. Unknown-`kid` refreshes use a global
cooldown measured from fetch completion, and failed discovery/JWKS attempts
start a separate retry backoff. Protected `jku`, `jwk`, `x5u`, `crit`, and `b64`
headers are rejected. The client does not trust token claims or provider groups
as tenant identifiers.

| `SMOLQUERY_INTERNAL_SECRET` | what internal HTTP proves itself with; generated per boot on a single node, required as a non-empty shared value before a cluster boots |

### Storage and the catalog

| variable | effect (default) |
|---|---|
| `SMOLQUERY_DATA_DIR` | the one directory everything durable lives under (`/data` in the image) |
| `SMOLQUERY_BUFFER_DIR` / `SMOLQUERY_SEALED_DIR` | split a tier onto its own disk (under the data dir) |
| `SMOLQUERY_CATALOG` | DuckLake metadata database, e.g. `postgres:dbname=smolquery` (the data dir's SQLite) |
| `SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION` | `true` lets an attach migrate the catalog to the extension's newer format — one-way; nodes on the old extension cannot read the result (`false`; see [deployment.md](deployment.md#catalog-format-upgrades)) |
| `SMOLQUERY_SNAPSHOT_KEEP_MS` | the time-travel promise; must exceed the longest pinned query and `retire_grace_ms` (`86400000`) |
| `SMOLQUERY_MERGE_INPUTS_PER_CALL` | cap on `read_parquet` inputs any one of the merge's engine calls carries (`12`, T-246/T-247). Per-input cost over `httpfs` is what outruns the engine's 30 s call timeout. The merge reads a larger input list in capped chunks into a temp table, so a seal claim of any size merges. The default's derivation is in `Smolquery.StorageService.Runtime`'s docs |
| `SMOLQUERY_STORAGE_MEMORY_LIMIT` | DuckDB memory limit for the storage merge engine (T-250). Unset, the limit is **half the container's cgroup memory limit**, so the merge scales with the pod; only without a cgroup limit does the engine fall back to `SMOLQUERY_MEMORY_LIMIT` — one size for every engine on every role, which is what left a 4 Gi pod merging inside 954 MiB. The resolved value and its source are logged at boot |
| `SMOLQUERY_STORAGE_COMPACT_MEMORY_LIMIT` | DuckDB memory limit for the compaction engine (T-259). Compaction runs on its own engine so a timed-out merge cannot starve seals, and the compactor recycles it after a call exit. Unset, the limit is **a quarter of the container's cgroup memory limit**; only without a cgroup limit does the engine fall back to `SMOLQUERY_MEMORY_LIMIT`. The resolved value and its source are logged at boot |
| `SMOLQUERY_COMPACT_MAX_ROWS` | cap on a compaction group's summed rows (`4194304`, T-260). `compact_max_bytes` bounds compressed bytes, and on ~100x-compressible data a 47 MiB group held ~25M rows — merge cost scales with rows, so the group blew the merge's five-minute budget and re-planned identically every sweep. Sizing already reads each footer's `num_rows`, so the cap costs no new I/O. A head file no neighbor fits beside under the cap is skipped, so a row-heavy file cannot wedge the table's backlog. The default is a start, not a pin-rate prediction: the compactor adapts the cap per table — a merge OOM halves a table's cap, never below `65536` rows, and sustained evidence at the tightened cap earns it back (`Smolquery.StorageService.Compactor.adjusted_row_caps/3`, T-262) |
| `SMOLQUERY_S3_BUCKET` | puts the sealed tier on an S3-compatible store: points both the storage service's and the query service's `store:` at `Segments.Store.S3` |
| `SMOLQUERY_S3_ACCESS_KEY_ID` / `SMOLQUERY_S3_SECRET_ACCESS_KEY` | static S3 credentials. Set both, or neither — leaving both out uses the [AWS credential chain](#s3-credentials) instead. One without the other is rejected at startup |
| `SMOLQUERY_S3_ENDPOINT` | S3-compatible endpoint (unset targets AWS S3) |
| `SMOLQUERY_S3_REGION` | S3 region (`us-east-1`) |
| `SMOLQUERY_S3_URL_STYLE` | `path` or `vhost` (`path` when an endpoint is set) |
| `SMOLQUERY_S3_STAGING_DIR` | local scratch for segments before upload (`<data dir>/sealed-staging`) |

### Engine and the write path

The build packages DuckDB 1.5.3 through ADBC 0.12.1 and every database process
uses that same driver version. The compile-time asset is selected from the build
target: macOS uses the universal asset, while Linux GNU uses the matching
`aarch64` or `x86_64` asset. An unsupported target falls back to ADBC's own
driver matrix, so Mix tasks still run there; the engine then refuses to start
until the pinned version's driver exists for that target.

| variable | effect (default) |
|---|---|
| `SMOLQUERY_MEMORY_LIMIT` | per-engine DuckDB memory limit (`2GB`). The storage merge engine resolves its own instead — see `SMOLQUERY_STORAGE_MEMORY_LIMIT` |
| `SMOLQUERY_ENGINE_THREADS` | DuckDB threads for one standalone engine, and the number the write pool divides (the deployment host's scheduler count) |
| `SMOLQUERY_MAX_RESULT_ROWS` | ceiling on rows `Engine.query/3` converts to Elixir terms (`100000`, or `infinity`) |
| `SMOLQUERY_SPILL_DIR` | root for per-instance DuckDB spill directories (`.tmp`, relative to the working directory). Use node-local storage; it is intentionally separate from `SMOLQUERY_DATA_DIR` |
| `SMOLQUERY_MAX_TEMP_DIRECTORY_SIZE` | per-instance spill limit (e.g. `10GiB`). DuckDB otherwise permits up to 90% of free space per instance. The limit multiplies: `N` concurrent instances may spill `N ×` this value, so it is not a disk budget |
| `SMOLQUERY_FLUSH_INTERVAL_MS` | group-commit cadence, and so the ack-latency bound (`1000`) |
| `SMOLQUERY_COMMIT_SIBLINGS` | the in-flight insert count at which the full interval applies (`5`, Postgres's `commit_siblings`). A window opening below it closes after `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` instead — waiting only buys batching when other writers are active. `0` turns the short window off |
| `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` | the group-commit window below `SMOLQUERY_COMMIT_SIBLINGS` (`5`). A few ms rather than zero so a burst's simultaneous first inserts still share one commit |
| `SMOLQUERY_FLUSH_MAX_BYTES` | the other trigger: accumulated wire bytes that force a group commit before the interval elapses (`2000000`). Whichever fires first ends the commit, so a batch size and arrival rate that reach this sooner than `SMOLQUERY_FLUSH_INTERVAL_MS` make the interval decorative — raise it to let the cadence actually govern, at the cost of resident bytes per table |
| `SMOLQUERY_MAX_BUFFERED_BYTES` | the admission ceiling on one table's accumulator, past which a write is refused with `buffer_full` (`64000000`). Must stay comfortably above `SMOLQUERY_FLUSH_MAX_BYTES` — the accumulator overshoots the flush trigger by up to one batch. A pair that is not strictly greater logs a warning at the buffer boot, and the row-side pair (`flush_max_rows`/`max_buffered_rows`) gets the same check |
| `SMOLQUERY_ENCODE_CONCURRENCY` | how many of a table's Parquet encodes may run at once (the node's scheduler count — a container held to one core encodes serially, which is what one core means); the manifest append, replication round and replies stay serialized in the Committer regardless. The scheduler count follows cpusets, not CFS quotas — set this and the pool size explicitly where quotas are the fence |
| `SMOLQUERY_FLUSH_WRITER` | which writer turns a flush into Parquet: `duckdb` (default) or `polars`; any other value fails boot. `duckdb` also stops the ingest edge parsing — the NDJSON body is forwarded to the owning buffer as bytes and one `COPY ... read_json` parses, sorts and writes it at flush. The default path defers schema validation to flush, then salvages a failed batch row by row, preserving per-row `insertErrors`; a bad row does not always fail the whole commit, and successful rows are still written. `/insert` accepts NDJSON only; the JSON-array envelope was removed |
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
| `SMOLQUERY_BUFFER_NODES` | the buffer fleet this deployment expects, as at most 256 comma-separated `name@host` node names. Names stay as bounded strings during release configuration and become node atoms only when the expected-nodes keeper consumes the fleet, so unrelated roles do not create atoms from this setting. An empty value seeds nothing and the keeper keeps polling, the same as unset. A reader fails a query when one of these cannot answer, instead of counting its unsealed rows as zero — `:pg` membership drops a crashed node exactly the way it drops a drained one, and only the expected set can tell them apart. Scaling down is drain, stop, *then* remove from the expected set. Unset (single-node, dev), the live ring is the whole set and nothing changes. With replication on, up to `replication_factor - 1` of these may be absent and reads still answer completely (T-97). Under clustering (`CATALOG_DATABASE_URL`), this list only *seeds* a Postgres-backed row on first boot (T-109): after that the live set is the row, readable on every node within ~1s of a change and CAS-writable at runtime (`Smolquery.BufferService.ExpectedNodes.resize/3`) with no redeploy — editing this variable later has no effect on an already-seeded deployment |
| `SMOLQUERY_BUFFER_REPLICATION` | replication factor for the hot tier's unsealed tail (T-96). Set to `N >= 2` to enable `Replicator.SegmentShipping`; values below `2` fail boot. Every group commit is on `N` disks before its ack, a ring smaller than `N` refuses writes, and readers tolerate `N - 1` absent buffer nodes. Set it on every role — buffer nodes ship, query nodes use it to size read tolerance. Unset: single-copy (`Replicator.None`). **Raising it on a fleet with data**: read tolerance comes from this setting, not from actual copy counts, and nothing backfills segments committed before the change — until that pre-existing unsealed tail seals, a rollout that takes a node down can silently drop it from reads. Force-seal first (drain each node, or wait out `seal_max_age_ms`) before rolling restarts under the new factor |
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

Releases, artifacts, and upgrade procedures live in
[deployment.md](deployment.md).

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
  merge_inputs_per_call: 12,
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
release is built. In a release, `SMOLQUERY_ENGINE_THREADS` sets it explicitly —
the lever for a container whose CPU quota the scheduler count does not see.

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

#### S3 credentials

Leave `:access_key_id` and `:secret_access_key` out and the sealed tier
authenticates through the AWS default credential chain instead — environment,
profile, ECS, EKS Pod Identity, web identity, then EC2 instance metadata. That
is how a deployment runs with no static S3 secrets at all, which some AWS
organizations require: an SCP that denies `s3:*` to IAM users cannot be
overridden by any IAM policy, and static keys can only ever be an IAM user.

Set both keys or neither. One alone is a half-written configuration, not a
chain, so `Store.S3.new/1` rejects it at startup.

Both halves of the sealed tier follow the same rule. Elixir signs its own
uploads and listings through `Smolquery.AwsCredentials`, which resolves fresh
credentials per request so rotation needs no restart. DuckDB engines get a
`CREATE SECRET ... PROVIDER credential_chain, REFRESH auto` instead of static
keys, and load the `aws` extension that provider comes from. `REFRESH auto`
is what keeps the long-lived storage-service engine working past the expiry of
the temporary credentials it started with.

MinIO and other non-AWS stores keep using static keys — they have no credential
chain to consult.

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
