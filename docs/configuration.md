# Configuration

Environment variables configure a release. Application config configures a Mix
project. `config/runtime.exs` resolves the environment variables at boot. The
environment variables are the deployment surface. The application config is the
full set of settings behind them.

The release validates environment values before any service subtree starts:

- A numeric operational value must be a positive decimal integer. The exception
  is a setting that documents zero as a switch, such as
  `SMOLQUERY_COMMIT_SIBLINGS` or `SMOLQUERY_SEAL_BACKOFF_BASE_MS`.
- A listener port must be in `1..65535`.
- An IP (Internet Protocol) value must be a valid IPv4 or IPv6 address.
- `SMOLQUERY_MAX_RESULT_ROWS` also accepts `infinity`.

An invalid value fails the boot. The boot error names the variable, the
received value, and the accepted shape.

## Environment variables

### Roles and the front door

| variable | effect (default) |
|---|---|
| `SMOLQUERY_ROLES` | Selects the service subtrees that start (`all`). Use `all`, or a comma-separated subset of `api,ingest,buffer,storage,query,web`. An unknown name fails the boot |
| `SMOLQUERY_API_KEY` | The Bearer key that every `/v1` route requires. A node with the `:api` role and no key refuses to boot |
| `SMOLQUERY_API_IP` / `SMOLQUERY_API_PORT` | The API (application programming interface) bind address and port (`0.0.0.0` in the prod image / `4000`) |
| `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES` | The most ingest-body bytes that an API node admits at once (T-245). `SmolqueryApi.Admission` counts `POST .../insert` and `.../load` bodies by the declared `content-length` before it reads any body. Past the limit, it refuses a request with a 429 and `retry-after: 1`. The refusal costs a header read, never a body. A client burst thus sheds load instead of an out-of-memory (OOM) kill of the pod. Unset, the limit is a quarter of the container's cgroup memory limit, with a floor of one NDJSON (newline-delimited JSON) body (`SMOLQUERY_INSERT_MAX_NDJSON_BYTES`). Without a cgroup limit, the value is `268435456`. An idle counter always admits one request; the route's own body cap decides what is too large |
| `SMOLQUERY_INSERT_MAX_NDJSON_BYTES` | The largest `POST .../insert` body an API node reads (`8000000`). A larger body is a 413 that names the cap and points at `POST .../load`. The value is also what `SmolqueryApi.Admission` reserves for an insert that declares no `content-length`, and the floor of the derived in-flight limit. Raise it only with the memory to match: the route holds the whole body in heap before it parses |
| `SMOLQUERY_LOAD_MAX_BYTES` | The largest `POST .../load` body an API node reads (`268435456`). A larger body is a 413. The body spools to a temporary file under the data directory, but the parser materializes every row, so a load peaks at roughly ten times the file in memory. The cap is in bytes, so it admits a different row count per format — see [benchmarks.md](benchmarks.md) |
| `SMOLQUERY_WEB_IP` / `SMOLQUERY_WEB_PORT` | The web UI (user interface) bind address and port (`127.0.0.1` / `4002`). Expose the listener only on purpose |
| `SMOLQUERY_METRICS_IP` / `SMOLQUERY_METRICS_PORT` | The metrics listener bind address and port (`127.0.0.1` single-node, `0.0.0.0` in the prod image / `4003`). Every node serves `GET /metrics`, whatever its roles. The internal secret gates the endpoint |
| `SMOLQUERY_WEB_USERNAME` / `SMOLQUERY_WEB_PASSWORD` | The basic-auth credential that every UI route requires. A node with the `:web` role and no credential refuses to boot |
| `SMOLQUERY_WEB_HOST` | The public host of the UI (`localhost`). It is also the default `check_origin` source |
| `SMOLQUERY_WEB_CHECK_ORIGIN` | Set `false` to accept any websocket origin. Or set a comma-separated origin list (default: the `SMOLQUERY_WEB_HOST` value). Each entry needs a scheme or a leading `//`, for example `https://ui.example.com` |
| `SMOLQUERY_SECRET_KEY_BASE` | Signs the web UI session that guards the LiveView socket. **Required** on a node with the `:web` role. The value must be at least 64 bytes (`mix phx.gen.secret`). Set the same value on every `:web` node |
| `SMOLQUERY_INTERNAL_SECRET` | The secret that internal HTTP uses to prove itself. A single node generates it per boot. A cluster requires a non-empty shared value before it boots |
| `SMOLQUERY_CREDENTIAL_KEY` | The key that seals the passwords of federated Postgres connections (T-322). The value is 32 bytes, base64 encoded (`openssl rand -base64 32`). Set the same value on every node. The catalog stores only ciphertext, so the key never reaches the metadata database; a lost key invalidates every stored connection and the operator re-enters the passwords. A deployment that registers no connection needs no key |

### Storage and the catalog

| variable | effect (default) |
|---|---|
| `SMOLQUERY_DATA_DIR` | The one directory that holds everything durable (`/data` in the image) |
| `SMOLQUERY_BUFFER_DIR` / `SMOLQUERY_SEALED_DIR` | Splits a tier onto its own disk (default: under the data dir) |
| `SMOLQUERY_CATALOG` | The DuckLake metadata database, for example `postgres:dbname=smolquery` (default: the data dir's SQLite) |
| `SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION` | `true` lets an attach migrate the catalog to the extension's newer format (`false`). The migration is one-way. Nodes on the old extension cannot read the result. See [deployment.md](deployment.md#catalog-format-upgrades) |
| `SMOLQUERY_SNAPSHOT_KEEP_MS` | The time-travel promise (`86400000`). The value must exceed the longest pinned query. It must also exceed `retire_grace_ms` |
| `SMOLQUERY_SEAL_ROW_GROUP_SIZE` | Sets `ROW_GROUP_SIZE` on every sealed Parquet `COPY`, for seal and compaction alike (`1048576`, T-280). A sealed-tier scan over `httpfs` pays roughly one range request per row group. This value thus sets a query's request count per segment. Smaller groups buy finer clustered-key pruning at that cost. The setting applies to newly written segments only. Compaction never revisits a healthy-sized segment, so old data keeps its old row groups. The seal path has no OOM adaptation. A claim's inputs are frozen. A seal `COPY` that deterministically OOMs at this size therefore retries forever. On a memory-tight node, lower this value or raise `SMOLQUERY_STORAGE_MEMORY_LIMIT` |
| `SMOLQUERY_MERGE_INPUTS_PER_CALL` | The cap on `read_parquet` inputs in any one engine call of the merge (`12`, T-246/T-247). Per-input cost over `httpfs` is what outruns the engine's 30 s call timeout. The merge reads a larger input list in capped chunks into a temp table. A seal claim of any size therefore merges. The docs of `Smolquery.StorageService.Runtime` derive the default |
| `SMOLQUERY_MERGE_COPY_TIMEOUT_MS` | The call budget for the merge's final `COPY` (`300000`, T-261). The `COPY` writes the sealed segment. Its duration scales with the claim's bytes, not with per-input `httpfs` latency, so the engine's 30 s default would decide how large a backlog may seal |
| `SMOLQUERY_MERGE_STAGING_TIMEOUT_MS` | The call budget for one staging chunk of the merge (`120000`). `SMOLQUERY_MERGE_INPUTS_PER_CALL` bounds a chunk's inputs, not its bytes. Twelve compaction inputs near `compact_below_bytes` move hundreds of megabytes on a slow link |
| `SMOLQUERY_MERGE_DESCRIBE_TIMEOUT_MS` | The call budget for the schema `DESCRIBE` before a projection (`120000`, T-288). The call's own work is small. Its budget is spent on a serialized connection, where every other merge's staging call is this call's queue time |
| `SMOLQUERY_MAX_CONCURRENT_SEALS` | The seals in flight on one storage node (`2`). Each slot gets its own connection on the merge engine, so this value also sets the merge calls that can run at once. Signalling is level-triggered. A signal shed at the bound costs a `SMOLQUERY_SEAL_RETRY_MS` delay, never a lost seal. More, smaller claims in parallel is the shape that drains a wide backlog; raise this together with a lower `SMOLQUERY_CLAIM_VALVE_FACTOR`, and watch the engine's memory limit, which the slots share. Read the resolved value off the `storage shape:` line at boot |
| `SMOLQUERY_SEAL_BACKOFF_BASE_MS` | The cooldown after a table's first failed seal (`30000`, T-299). After `n` consecutive failures the table's signals are shed for `min(base × 2^(n-1), max)`. A success clears the cooldown. The default equals `SMOLQUERY_SEAL_RETRY_MS`, so the first failure adds nothing. `0` turns the cooldown off |
| `SMOLQUERY_SEAL_BACKOFF_MAX_MS` | The ceiling on that cooldown (`600000`). A claim failing identically every re-signal burns an attempt slot and the merge engine's shared memory for the full failed attempt. That is the shape that stalled sealing cluster-wide. Unlike the base, this value must be positive. The cooldown is `min(base × 2^(n-1), max)`, so a `0` ceiling disables the cooldown whatever the base says. Turn the cooldown off through the base |
| `SMOLQUERY_STORAGE_MEMORY_LIMIT` | The DuckDB memory limit for the storage merge engine (T-250). Unset, the limit is half the container's cgroup memory limit. The merge thus scales with the pod. Only without a cgroup limit does the engine fall back to `SMOLQUERY_MEMORY_LIMIT`. That fallback is one size for every engine on every role. That one size is what left a 4 Gi pod merging inside 954 MiB. The boot log shows the resolved value and its source, and the `storage shape:` line repeats it |
| `SMOLQUERY_STORAGE_COMPACT_MEMORY_LIMIT` | The DuckDB memory limit for the compaction engine (T-259). Compaction runs on its own engine, so a timed-out merge cannot starve seals. The compactor recycles the engine after a call exit. Unset, the limit is a quarter of the container's cgroup memory limit. Only without a cgroup limit does the engine fall back to `SMOLQUERY_MEMORY_LIMIT`. The boot log shows the resolved value and its source |
| `SMOLQUERY_COMPACT_MAX_ROWS` | The cap on a compaction group's summed rows (`4194304`, T-260). `compact_max_bytes` bounds compressed bytes only. On ~100x-compressible data, a 47 MiB group held ~25M rows. Merge cost scales with rows, so that group blew the merge's five-minute budget. It then re-planned identically every sweep. Sizing already reads each footer's `num_rows`, so the cap costs no new I/O (input/output). The compactor skips a head file when no neighbor fits beside it under the cap. A row-heavy file thus cannot wedge the table's backlog. The default is a start, not a prediction of the pin rate. The compactor adapts the cap per table. A merge OOM halves a table's cap, never below `65536` rows. Sustained evidence at the tightened cap earns it back (`Smolquery.StorageService.Compactor.adjusted_row_caps/3`, T-262) |
| `SMOLQUERY_COMPACT_BUCKET_MS` | The time-bucket width that compaction ownership shards on (`3600000`, T-269). Ownership used to be per table. One node then compacted a hot table alone while its peers idled. The ring now owns `{table, bucket}`. A segment's bucket is its ULID (Universally Unique Lexicographically Sortable Identifier) timestamp over this value. **Set it identically on every storage node.** Disjointness requires every node to compute the same bucket ids. Divergent values during a rolling change make two nodes merge the same runs until the rollout completes. A merge group stays inside its bucket, with one exception. A bucket that cannot meet `compact_min_inputs` alone rolls its candidates into the node's next owned bucket. Quiet tables thus still compact. Smaller buckets spread a backlog across more nodes. They also widen those carries |
| `SMOLQUERY_S3_BUCKET` | Puts the sealed tier on an S3-compatible store. It points the `store:` of both the storage service and the query service at `Segments.Store.S3` |
| `SMOLQUERY_S3_ACCESS_KEY_ID` / `SMOLQUERY_S3_SECRET_ACCESS_KEY` | Static S3 credentials. Set both, or neither. With both unset, the store uses the AWS (Amazon Web Services) [credential chain](#s3-credentials) instead. Startup rejects one without the other |
| `SMOLQUERY_S3_ENDPOINT` | The S3-compatible endpoint. Unset targets AWS S3 |
| `SMOLQUERY_S3_REGION` | The S3 region (`us-east-1`) |
| `SMOLQUERY_S3_URL_STYLE` | `path` or `vhost` (`path` when an endpoint is set) |
| `SMOLQUERY_S3_STAGING_DIR` | The local scratch directory for segments before upload (`<data dir>/sealed-staging`) |

### Engine and the write path

The build packages DuckDB 1.5.3 through ADBC (Arrow Database Connectivity)
0.12.1. Every database process uses that same driver version. The build target
selects the compile-time asset. macOS uses the universal asset. Linux GNU uses
the matching `aarch64` or `x86_64` asset.

An unsupported target falls back to ADBC's own driver matrix, so Mix tasks
still run there. On such a target, the engine refuses to start until the pinned
version's driver exists.

| variable | effect (default) |
|---|---|
| `SMOLQUERY_MEMORY_LIMIT` | The per-engine DuckDB memory limit (`2GB`). The storage merge engine resolves its own limit instead — see `SMOLQUERY_STORAGE_MEMORY_LIMIT` |
| `SMOLQUERY_ENGINE_THREADS` | The DuckDB threads for one standalone engine (default: the deployment host's scheduler count). The write pool also divides this number |
| `SMOLQUERY_MAX_RESULT_ROWS` | The ceiling on rows that `Engine.query/3` converts to Elixir terms (`100000`, or `infinity`) |
| `SMOLQUERY_EXTENSION_DIRECTORY` | Where DuckDB finds and installs its extensions (PL-50). The image sets it to `/app/duckdb-extensions`, which ships `httpfs`, `json`, `ducklake`, `aws`, and `postgres` pre-installed. Unset, DuckDB uses `$HOME/.duckdb/extensions` — under `/data` in the image, an `emptyDir` that a pod roll wipes, so the first query after a roll used to download the `aws` extension (429 ms on prod) |
| `SMOLQUERY_SPILL_DIR` | The root for per-instance DuckDB spill directories (`.tmp`, relative to the working directory). Use node-local storage. It is intentionally separate from `SMOLQUERY_DATA_DIR` |
| `SMOLQUERY_MAX_TEMP_DIRECTORY_SIZE` | The per-instance spill limit, for example `10GiB`. DuckDB otherwise permits up to 90% of free space per instance. The limit multiplies: `N` concurrent instances may spill `N ×` this value. It is not a disk budget |
| `SMOLQUERY_FLUSH_INTERVAL_MS` | The group-commit cadence (`1000`). It is thus the ack-latency bound |
| `SMOLQUERY_COMMIT_SIBLINGS` | The in-flight insert count at which the full interval applies (`5`, Postgres's `commit_siblings`). A window that opens below this count closes after `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` instead. The wait only buys batching when other writers are active. `0` turns the short window off |
| `SMOLQUERY_FLUSH_IDLE_INTERVAL_MS` | The group-commit window below `SMOLQUERY_COMMIT_SIBLINGS` (`5`). The value is a few ms rather than zero, so the simultaneous first inserts of a burst still share one commit |
| `SMOLQUERY_FLUSH_MAX_BYTES` | The other flush trigger (`2000000`): accumulated wire bytes that force a group commit before the interval elapses. Whichever trigger fires first ends the commit. A batch size and arrival rate that reach this cap sooner than `SMOLQUERY_FLUSH_INTERVAL_MS` make the interval decorative. Raise the cap to let the cadence govern, at the cost of resident bytes per table |
| `SMOLQUERY_MAX_BUFFERED_BYTES` | The admission ceiling on one table's accumulator (`64000000`). Past it, the buffer refuses a write with `buffer_full`. Keep the value above `SMOLQUERY_FLUSH_MAX_BYTES` with a clear margin; the accumulator overshoots the flush trigger by up to one batch. A pair that is not strictly greater logs a warning at the buffer boot. The row-side pair (`flush_max_rows`/`max_buffered_rows`) gets the same check |
| `SMOLQUERY_BUFFER_FULLSWEEP_AFTER` | The `fullsweep_after` spawn option of every `TableBuffer` and its committer (`0`, T-330). Both processes take a whole payload onto their heap per group commit. The garbage lands on the old heap, and only a fullsweep collects an old heap, so OTP's default of 65,535 keeps every payload the process ever handled resident. A loaded buffer pod measured 1,892 MB of process heaps against a live set of 0.0 MB, and the tier was OOM-killed holding that garbage. `0` makes every collection a fullsweep. Its cost is proportional to the live set, which is what makes `0` affordable here. Raise it only to trade resident bytes for collection work; a value near OTP's default restores the leak |
| `SMOLQUERY_SEAL_MAX_BYTES` | The byte trigger that makes a table's unsealed tail sealable (`67108864`). It also sizes the claim's byte valve, through `SMOLQUERY_CLAIM_VALVE_FACTOR` |
| `SMOLQUERY_SEAL_MAX_FILES` | The micro-segment-count trigger (`64`). It likewise sizes the claim's count valve |
| `SMOLQUERY_SEAL_MAX_AGE_MS` | The age trigger (`60000`): a table seals its tail this long after the oldest unsealed micro-segment, however small the tail is. It is thus the sealing latency floor of a quiet table |
| `SMOLQUERY_SEAL_RETRY_MS` | How often the buffer repeats a seal signal until the claim retires (`30000`). Signalling is level-triggered, so this value is the recovery time of every shed or lost signal |
| `SMOLQUERY_CLAIM_VALVE_FACTOR` | The multiplier from the two seal triggers to the two claim valves (`16`, T-335). One claim freezes at most `SMOLQUERY_SEAL_MAX_BYTES ×` this value in bytes, and `SMOLQUERY_SEAL_MAX_FILES ×` this value in micro-segments — `1 GiB` and `1024` on the defaults. The claim is what the storage tier must merge in one go. Every limit it has to fit inside is a separate setting: the merge engine's memory limit, its spill limit, and its three call budgets. Nothing checks the relationship. A 488-segment claim on a 63-column table exhausted all five in turn, and the partition wedged: a claim's inputs are frozen, so every retry merges the same set. Lower the factor to seal a wide table in smaller pieces. Seal cost measured as roughly `segments^1.21`, so a claim four times smaller costs more than four times less, and the ref's one live claim blocks it for a shorter window. Read the resolved valves off the `buffer shape:` line at boot |
| `SMOLQUERY_ENCODE_CONCURRENCY` | How many of a table's Parquet encodes run at once (default: the node's scheduler count). A container held to one core thus encodes serially; that is what one core means. The manifest append, the replication round, and the replies stay serialized in the Committer regardless. The scheduler count follows cpusets, not CFS (Completely Fair Scheduler) quotas. Where quotas are the fence, set this value and the pool size explicitly |
| `SMOLQUERY_MAX_LIVE_CLAIMS` | How many seal claims one table ref may hold open at once (`1`, T-339). At `1` a ref seals serially: the next claim forms only after the previous one retires, so one slow merge is the whole ref's seal throughput. Above `1`, the buffer keeps freezing valve-sized claims from the unclaimed tail while earlier claims merge. One ref's backlog can then occupy that many storage seal slots in parallel, and the unsealed tail drains toward zero instead of growing behind one merge. Sealed segments may land out of input order within the ref; every manifest consumer is per-entry, so nothing reads that order. Size it with `SMOLQUERY_MAX_CONCURRENT_SEALS` and the merge engine's memory limit, which concurrent merges share |
| `SMOLQUERY_WRITE_POOL_SIZE` | How many DuckDB instances the flush write pool runs (default: the node's scheduler count, capped at `32`). Valid values are `1..32`; the boot refuses anything else. The pool selects a member per segment, not per table. A table has one committer, so a hash on the table would send every flush to one connection. Each member gets the thread count of `Smolquery.Engine` divided by the pool size (floor one). Each member inherits the engine's memory limit whole. The two variables below size a member explicitly. With no explicit engine thread count, both the engine and the pool resolve `System.schedulers_online()` at boot on the deployment host, not on the release builder. The host-derived count means the *declared* DuckDB write memory scales with it — see `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT`. Read the resolved numbers off the `buffer shape:` line at boot |
| `SMOLQUERY_WRITE_ENGINE_THREADS` | The DuckDB threads for one write-pool member. It replaces the division. The division describes a budget only while the pool is smaller than the thread count. Past that point, the result sits at its floor of one. An operator who wants a different shape states the number |
| `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT` | The DuckDB memory limit for one write-pool member, for example `512MB`. Unset, every member inherits `SMOLQUERY_MEMORY_LIMIT` whole. The node's declared DuckDB write budget is then `write_pool_size ×` that value. A size string has its own grammar; nothing divides it for you |
| `SMOLQUERY_READ_ENGINE_THREADS` | The DuckDB threads for the private engine of one query job (T-279). It is the read-side counterpart of `SMOLQUERY_WRITE_ENGINE_THREADS`. Unset, each job engine takes DuckDB's own core detection; a cgroup-limited container can misreport that. A sealed-tier scan overlaps its `httpfs` range requests per thread. This value is thus also the request-concurrency ceiling of the scan. Jobs in flight multiply it, up to `max_concurrent_jobs` |
| `SMOLQUERY_WARM_ENGINES` | How many job engines the query service keeps bootstrapped ahead of demand (`2`, PL-50). A job engine's bootstrap — three extension loads and the catalog `ATTACH` — measured ~650 ms per query on prod; a warm engine takes it off the request path. `0` starts every engine cold. Each warm engine holds one catalog connection and DuckDB's baseline memory while it waits; it is recycled after 5 minutes and probed at checkout, so a stale connection falls back to a cold start. Read `smolquery_query_engines_total{source}` to see the warm/cold split |
| `SMOLQUERY_DISTRIBUTED_QUERY` | Whether a decomposable aggregate query scatters its file list across several DuckDB instances (`true`, PL-49). Set `false` as the kill switch. A job's own `"distributed"` option overrides it per query, and the web editor's Distribute toggle sends that option. A query that does not decompose, or any distributed failure, runs the normal single-engine path. With clustering on, the workers are the query service's group members; without, `SMOLQUERY_DISTRIBUTED_LOCAL_WORKERS` instances run on this node |
| `SMOLQUERY_DISTRIBUTED_MIN_FILES` | The smallest shardable file count a query scatters over (`8`). Under it, the fixed costs — one engine start per worker, one partial transfer per shard — outweigh the scan (PL-48) |
| `SMOLQUERY_DISTRIBUTED_LOCAL_WORKERS` | How many worker instances run on this node when clustering is off (`4`) |
| `SMOLQUERY_DISTRIBUTED_WORKER_MEMORY_LIMIT` | The DuckDB memory limit for one worker engine (default: the `job_memory_limit` config value, whole). A scattered query's declared budget is the worker count `×` this, on top of the job engine's own limit. A size string has its own grammar; nothing divides it for you |
| `SMOLQUERY_DISTRIBUTED_WORKER_THREADS` | The DuckDB threads for one worker engine (default: `SMOLQUERY_READ_ENGINE_THREADS`) |
| `SMOLQUERY_WRITE_PARTITIONS` | How many buffer identities the writes of one table spread over (`1`). It thus sets how many nodes ingest and seal the table. This is the deployment-wide *default*. A table's own count in catalog metadata can raise it online, per table, via `PATCH {"partitions": N}` (T-304). The effective count is the maximum of the two. Reader and writer defaults must match; set the value identically on ingest and query roles. **Lower it only with the fleet at rest.** A table without its own catalog count has no floor but this default. A decrease under load strands that table's hot rows in partitions that readers no longer expand |
| `SMOLQUERY_HOT_SERVER_PORT` | The port `HotServer` binds to serve micro-segments over `httpfs` (`4001`), on every node. Peers derive each other's hot-tier URLs from the node name plus this port |
| `SMOLQUERY_HOT_SERVER_IP` | The `HotServer` bind address (`127.0.0.1` single-node, `0.0.0.0` once clustered) |
| `SMOLQUERY_BUFFER_BASE_URL` | Where the sealer and the query planner reach the hot tier on a single node (`http://127.0.0.1:4001`) |

### Clustering

| variable | effect (default) |
|---|---|
| `CATALOG_DATABASE_URL` | The Postgres URL, for example `postgres://user:pass@host/db`. This is what makes a cluster. It tiers the DuckLake catalog onto Postgres; DuckDB loads the `postgres` extension alongside `ducklake`. It also enables node discovery through that same database (`Smolquery.Cluster`, over `libcluster_postgres`). One node is not a cluster, so a single-node deployment leaves it unset. `SMOLQUERY_CATALOG` overrides just the catalog side, for example to point it at a different database than discovery uses |
| `SMOLQUERY_BUFFER_NODES` | The buffer fleet that this deployment expects: at most 256 comma-separated `name@host` node names. The names stay bounded strings during release configuration. They become node atoms only when the expected-nodes keeper consumes the fleet. Unrelated roles thus do not create atoms from this setting. An empty value seeds nothing; the keeper keeps polling, the same as unset. A reader fails a query when one of these nodes cannot answer. It does not count that node's unsealed rows as zero. `:pg` membership drops a crashed node exactly the way it drops a drained one. Only the expected set can tell them apart. To scale down: drain, stop, *then* remove the node from the expected set. Unset (single-node, dev), the live ring is the whole set; nothing changes. With replication on, up to `replication_factor - 1` of these nodes may be absent while reads still answer completely (T-97). Under clustering (`CATALOG_DATABASE_URL`), this list only *seeds* a Postgres-backed row on the first boot (T-109). After that, the live set is the row. Every node reads a change within ~1 s. A CAS (compare-and-swap) write can change the row at runtime (`Smolquery.BufferService.ExpectedNodes.resize/3`) with no redeploy. A later edit of this variable has no effect on an already-seeded deployment |
| `SMOLQUERY_BUFFER_REPLICATION` | The replication factor for the hot tier's unsealed tail (T-96). Set `N >= 2` to enable `Replicator.SegmentShipping`; a value below `2` fails the boot. Every group commit is on `N` disks before its ack. A ring smaller than `N` refuses writes. Readers tolerate `N - 1` absent buffer nodes. Set it on every role: buffer nodes ship, query nodes size read tolerance from it. Unset means single-copy (`Replicator.None`). An increase on a fleet with data has a caveat. Read tolerance comes from this setting, not from actual copy counts. Nothing backfills segments committed before the change. Until that pre-existing unsealed tail seals, a rollout that takes a node down can silently drop it from reads. **Force-seal first** (drain each node, or wait out `seal_max_age_ms`) before rolling restarts under the new factor |
| `SMOLQUERY_BUFFER_REPLICAS` | On Kubernetes, the replica count of the buffer StatefulSet. `rel/env.sh.eex` expands it into `SMOLQUERY_BUFFER_NODES` with pod-DNS (Domain Name System) naming (`SMOLQUERY_BUFFER_STATEFULSET`, default `smolquery-buffer`). The expected fleet thus carries no hardcoded namespace; scaling is one number. The release ignores it if `SMOLQUERY_BUFFER_NODES` is set explicitly |
| `GEN_RPC_PORT` | The inter-node transport port (`5369`) |
| `GEN_RPC_BULK_CHANNELS` | How many `:bulk` gen_rpc connections a node opens to each peer (`4`). A table hashes to one connection, so its writes and replica shipments stay in order. More connections let more tables write to one node at once |
| `GEN_RPC_SCATTER_CHANNELS` | How many `{:scatter, _}` gen_rpc connections a node opens to each peer for distributed-query partials (`4`). A job hashes to one connection; concurrent jobs spread over the pool |
| `GEN_RPC_TLS` | `true` switches buffer/query inter-node traffic to mutual TLS (Transport Layer Security) (`false`). Verification is chain-only against the cluster CA (certificate authority); the emqx gen_rpc fork does no hostname or CN (common name) check. The CA is thus the trust boundary. Certificate files are per node (`GEN_RPC_TLS_DIR`, default `/etc/smolquery/gen-rpc-tls`; `POD_NAME` names the file). Any CA-signed certificate authenticates to any peer. A leaked node certificate thus means a rotation of the CA, not just of that node |
| `GEN_RPC_SSL_PORT` | The gen_rpc TLS port (`5870`) |
| `DIST_TLS` | `true` runs Erlang distribution (cluster membership only) over TLS with the same certificates (`false`). Set it in `rel/env.sh.eex`, not `config/runtime.exs`. Distribution starts before the release's Elixir config does |
| `POD_NAME` / `POD_NAMESPACE` | When set (a Kubernetes Downward API convention), `rel/env.sh.eex` derives `RELEASE_NODE` from the pod's stable headless-service DNS name. That is the same name a peer needs to reach this node |
| `HEADLESS_SERVICE` | The headless-service name in that derived node name (`smolquery-headless`) |
| `RELEASE_NODE_HOST` | Overrides the derived host part of `RELEASE_NODE` outright (non-StatefulSet deployments) |

The checked-in Kind overlays set `GEN_RPC_TLS=false` and `DIST_TLS=false`. They
still mount per-node development certificates. Operators can thus opt into
mutual TLS: change both values to `true` before you apply the overlay.

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
  claim_valve_factor: 16,
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
  seal_backoff_base_ms: 30_000,
  seal_backoff_max_ms: 600_000,
  gc_interval_ms: 300_000,
  gc_grace_ms: 3_600_000,
  compact_interval_ms: 300_000,
  compact_below_bytes: 33_554_432,
  compact_min_inputs: 2,
  compact_max_bytes: 134_217_728,
  merge_inputs_per_call: 12,
  merge_copy_timeout_ms: 300_000,
  merge_staging_timeout_ms: 120_000,
  merge_describe_timeout_ms: 120_000,
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
  read_engine_threads: nil,
  result_ttl_ms: 300_000,
  result_max_rows: 10_000
```

You can also pass engine options per instance to
`Smolquery.Engine.start_link/1`. That overrides the application config.
`:threads` is intentionally absent from the defaults above. Unless you
configure it explicitly, each engine resolves it from
`System.schedulers_online()` when the engine starts on the deployment host, not
when the release is built. In a release, `SMOLQUERY_ENGINE_THREADS` sets it
explicitly. That is the control for a container whose CPU quota the scheduler
count does not see.

### The buffer service

`:dir` is the buffer's root. Micro-segments go to a `Store.Local` beneath
`segments/`. Manifest logs go to `manifests/`. They are separate because they
answer to different rules: segments can move to another store, while the log
stays on the node that gave the ack. Point the segments elsewhere with
`store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}`.

`flush_interval_ms` is the ack-latency dial. A batch waits out the remainder of
the current group commit. A lower value thus trades throughput for latency.

That trade only exists when there is something to group. `commit_siblings` and
`flush_idle_interval_ms` make the wait adaptive. A window that opens with fewer
than `commit_siblings` inserts in flight closes after `flush_idle_interval_ms`
instead. A lone writer acks at commit speed rather than at the interval. Real
concurrency keeps the configured cadence.

The interval is only half the trigger. Under load, it is usually not the half
that fires. `flush_max_rows` and `flush_max_bytes` end a commit as soon as the
accumulator reaches either cap.

Consider a deployment whose arrival rate reaches `flush_max_bytes` in less than
`flush_interval_ms`. It runs a cadence it never configured: the interval
becomes decorative, and the byte cap pins the commit size instead. Four
500 kB batches against the 2 MB default is a four-batch commit however long
the interval says to wait.

When you tune the cadence, check which trigger ends the commit:

1. Divide `flush_max_bytes` by the offered bytes per second.
2. Compare the result against `flush_interval_ms`.

`ring:` is the static fallback only. With `CATALOG_DATABASE_URL` set, ownership
instead tracks the nodes that are alive and host this instance, via `:pg`
(`Smolquery.Cluster.PgGroup`). The config value only matters again if
clustering is off.

### The storage service

The storage service's own `ring:` is the static fallback for a *second*,
independent ring. This ring decides which storage node seals a table's work,
not which buffer node accumulates it. With clustering on, it likewise tracks
live `:pg` membership. `Smolquery.StorageService.Routing.own?/2` is the gate
that the sealer, the compactor, retention, and GC (garbage collection) check
before they act.

The gate is advisory, not mutual exclusion. During a ring change, two nodes can
transiently both pass it. The catalog re-derives its registration diff inside
every commit retry; that is what keeps a segment from a double registration.
See `Smolquery.StorageService.Routing` for the residual window.

`:dir` is where sealed segments land. `:store` overrides it the same way,
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
rather than a config snippet. Elixir evaluates `config/config.exs` at *build*
time, so `System.get_env/1` there bakes the builder's credentials (or `nil`)
into the artifact. The env wiring configures the query service's `store:` with
the same values. Every job engine needs those values to read the sealed tier
back.

#### S3 credentials

Leave `:access_key_id` and `:secret_access_key` out to authenticate the sealed
tier through the AWS default **credential chain**: environment, profile, ECS,
EKS Pod Identity, web identity, then EC2 instance metadata. This is how a
deployment runs with no static S3 secrets at all. Some AWS organizations
require that. No IAM (Identity and Access Management) policy can override an
SCP (service control policy) that denies `s3:*` to IAM users. Static keys can
only ever be an IAM user.

Set both keys or neither. One alone is a half-written configuration, not a
chain. `Store.S3.new/1` rejects it at startup.

Both halves of the sealed tier follow the same rule. Elixir signs its own
uploads and listings through `Smolquery.AwsCredentials`. That module resolves
fresh credentials per request, so rotation needs no restart.

DuckDB engines get a `CREATE SECRET ... PROVIDER credential_chain, REFRESH
auto` instead of static keys. They load the `aws` extension that this provider
comes from. `REFRESH auto` keeps the long-lived storage-service engine working
past the expiry of the temporary credentials it started with.

MinIO and other non-AWS stores keep static keys. They have no credential chain
to consult.

`buffer_base_url` is where the sealer reaches `HotServer` to pull manifests and
segment bytes. That is honest for a single node. Clustered, each seal signal
carries the node it came from. The sealer derives that node's URL from the node
name (`buffer_hot_port`). The claimed bytes physically live at the signal's
origin — not at the static config, and not even at the ring's current owner.

`engine_extensions` loads `httpfs` into the sealer's own engine. The merge
cannot work without it. The same engine also authenticates to the sealed tier's
`Store.S3` credentials, when configured, via `CREATE SECRET`
(`Smolquery.EngineSecrets`). A compaction that re-merges existing sealed
segments can thus read them back over `s3://`.

### The query service

The query service runs each query as a job with a private DuckDB engine
(`Smolquery.QueryService.Client.query/3` sync, `submit/3` + `fetch/2` async).
Given `catalog:` options, it starts its own DuckLake engine to plan through.
Given a `%Smolquery.Catalog{}`, it starts none. In that case, `job_bootstrap:`
must carry the `ATTACH` that job engines need, since they attach the lake
themselves.

`buffer_base_url` is where the planner reaches `HotServer` for hot manifests on
a single node. Clustered, the planner ignores it. It fans each table's manifest
fetch out across the fleet instead — see
[fan-out](architecture.md#clustered-fan-out).

`store` takes the same `Store.S3` config as the storage service's when the
sealed tier lives there. Every job's engine needs the matching `CREATE SECRET`
to read it. The query path itself never writes through the store.

The remaining settings:

- `max_concurrent_jobs` refuses rather than queues.
- `default_timeout_ms` bounds every job's runtime.
- `job_memory_limit` is each job engine's DuckDB `memory_limit`.
- `read_engine_threads` is each job engine's DuckDB `threads` (T-279). Unset,
  DuckDB detects cores itself. The count doubles as the scan's `httpfs`
  request-concurrency ceiling.
- `result_ttl_ms` is how long a finished job holds its result frame for an
  async caller.

`result_max_rows` bounds a job's materialized result (T-274). The default is
10,000. That matches the API's `maxResults` ceiling, so every result the
budget admits is pageable in one page.

`job_memory_limit` binds only DuckDB's scan; the result frame lives in Polars
memory outside it. This bound is thus what turns a `SELECT *` over a large
table into a `RESULT_TOO_LARGE` error instead of an OOM. The engine enforces it
as a `LIMIT`, so an over-budget query stops producing rows at the bound.
`:infinity` disables it.
