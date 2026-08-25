import Config

if roles = System.get_env("SMOLQUERY_ROLES") do
  config :smolquery, roles: Smolquery.Roles.parse!(roles)
end

if api_key = System.get_env("SMOLQUERY_API_KEY") do
  config :smolquery, SmolqueryApi, api_key: api_key
end

# T-245: the 429 must fire while the request is still a header. Unset, the
# in-flight limit derives as a quarter of the cgroup memory limit.
if bytes = System.get_env("SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES") do
  config :smolquery, SmolqueryApi,
    insert_max_in_flight_bytes:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES", bytes)
end

if bytes = System.get_env("SMOLQUERY_INSERT_MAX_NDJSON_BYTES") do
  config :smolquery, SmolqueryApi,
    max_ndjson_bytes:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_INSERT_MAX_NDJSON_BYTES", bytes)
end

if bytes = System.get_env("SMOLQUERY_LOAD_MAX_BYTES") do
  config :smolquery, SmolqueryApi,
    load_max_bytes: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_LOAD_MAX_BYTES", bytes)
end

if internal_secret = System.get_env("SMOLQUERY_INTERNAL_SECRET") do
  config :smolquery, :internal_secret, internal_secret
end

if credential_key = System.get_env("SMOLQUERY_CREDENTIAL_KEY") do
  config :smolquery, :credential_key, credential_key
end

if api_port = System.get_env("SMOLQUERY_API_PORT") do
  config :smolquery, SmolqueryApi.Endpoint,
    http: [port: Smolquery.RuntimeConfig.port!("SMOLQUERY_API_PORT", api_port)]
end

if api_ip = System.get_env("SMOLQUERY_API_IP") do
  ip = Smolquery.RuntimeConfig.ip!("SMOLQUERY_API_IP", api_ip)

  config :smolquery, SmolqueryApi.Endpoint, http: [ip: ip]
end

if web_port = System.get_env("SMOLQUERY_WEB_PORT") do
  config :smolquery, SmolqueryWeb.Endpoint,
    http: [port: Smolquery.RuntimeConfig.port!("SMOLQUERY_WEB_PORT", web_port)]
end

if web_ip = System.get_env("SMOLQUERY_WEB_IP") do
  ip = Smolquery.RuntimeConfig.ip!("SMOLQUERY_WEB_IP", web_ip)

  config :smolquery, SmolqueryWeb.Endpoint, http: [ip: ip]
end

if metrics_port = System.get_env("SMOLQUERY_METRICS_PORT") do
  config :smolquery, Smolquery.MetricsServer,
    port: Smolquery.RuntimeConfig.port!("SMOLQUERY_METRICS_PORT", metrics_port)
end

if metrics_ip = System.get_env("SMOLQUERY_METRICS_IP") do
  config :smolquery, Smolquery.MetricsServer,
    ip: Smolquery.RuntimeConfig.ip!("SMOLQUERY_METRICS_IP", metrics_ip)
end

if username = System.get_env("SMOLQUERY_WEB_USERNAME") do
  config :smolquery, SmolqueryWeb, username: username
end

if password = System.get_env("SMOLQUERY_WEB_PASSWORD") do
  config :smolquery, SmolqueryWeb, password: password
end

if web_host = System.get_env("SMOLQUERY_WEB_HOST") do
  config :smolquery, SmolqueryWeb.Endpoint, url: [host: web_host]
end

if check_origin = System.get_env("SMOLQUERY_WEB_CHECK_ORIGIN") do
  config :smolquery, SmolqueryWeb.Endpoint,
    check_origin: SmolqueryWeb.CheckOrigin.parse!(check_origin)
end

if secret_key_base = System.get_env("SMOLQUERY_SECRET_KEY_BASE") do
  config :smolquery, SmolqueryWeb.Endpoint, secret_key_base: secret_key_base
end

if limit = System.get_env("SMOLQUERY_MEMORY_LIMIT") do
  config :smolquery, Smolquery.Engine, memory_limit: limit
end

# The storage merge engine's own limit (T-250). Unset, it derives from the
# container's cgroup memory limit, and only without one of those does it
# inherit SMOLQUERY_MEMORY_LIMIT — one size for every engine on every role,
# which is what left a 4 Gi storage pod merging inside 954 MiB.
if limit = System.get_env("SMOLQUERY_STORAGE_MEMORY_LIMIT") do
  config :smolquery, Smolquery.StorageService, engine_memory_limit: limit
end

if threads = System.get_env("SMOLQUERY_ENGINE_THREADS") do
  config :smolquery, Smolquery.Engine, threads: String.to_integer(threads)
end

# Keep spills off `SMOLQUERY_DATA_DIR`, which may hold acknowledged buffer data.
# Unset preserves DuckDB's `.tmp` filesystem.
if dir = System.get_env("SMOLQUERY_SPILL_DIR") do
  config :smolquery, :spill_dir, dir
end

if size = System.get_env("SMOLQUERY_MAX_TEMP_DIRECTORY_SIZE") do
  config :smolquery, :max_temp_directory_size, size
end

if max_rows = System.get_env("SMOLQUERY_MAX_RESULT_ROWS") do
  ceiling =
    Smolquery.RuntimeConfig.positive_integer_or_infinity!("SMOLQUERY_MAX_RESULT_ROWS", max_rows)

  config :smolquery, Smolquery.Engine, max_result_rows: ceiling
end

if data_dir = System.get_env("SMOLQUERY_DATA_DIR") do
  config :smolquery, :data_dir, data_dir

  config :smolquery, Smolquery.Catalog.DuckLake,
    metadata: "sqlite:#{Path.join(data_dir, "catalog.sqlite")}",
    data_path: Path.join(data_dir, "ducklake")

  config :smolquery, Smolquery.BufferService, dir: Path.join(data_dir, "buffer")

  config :smolquery, Smolquery.StorageService, dir: Path.join(data_dir, "sealed")
end

if buffer_dir = System.get_env("SMOLQUERY_BUFFER_DIR") do
  config :smolquery, Smolquery.BufferService, dir: buffer_dir
end

if sealed_dir = System.get_env("SMOLQUERY_SEALED_DIR") do
  config :smolquery, Smolquery.StorageService, dir: sealed_dir
end

if base_url = System.get_env("SMOLQUERY_BUFFER_BASE_URL") do
  config :smolquery, Smolquery.StorageService, buffer_base_url: base_url
  config :smolquery, Smolquery.QueryService, buffer_base_url: base_url
end

if buffer_nodes = System.get_env("SMOLQUERY_BUFFER_NODES") do
  config :smolquery, Smolquery.BufferService,
    expected_node_names:
      Smolquery.RuntimeConfig.node_names!("SMOLQUERY_BUFFER_NODES", buffer_nodes)
end

if replication = System.get_env("SMOLQUERY_BUFFER_REPLICATION") do
  config :smolquery, Smolquery.BufferService,
    replicator:
      {Smolquery.BufferService.Replicator.SegmentShipping,
       replication_factor:
         Smolquery.RuntimeConfig.integer_at_least!(
           "SMOLQUERY_BUFFER_REPLICATION",
           replication,
           2
         )}
end

if interval = System.get_env("SMOLQUERY_FLUSH_INTERVAL_MS") do
  config :smolquery, Smolquery.BufferService,
    flush_interval_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_FLUSH_INTERVAL_MS", interval)
end

if interval = System.get_env("SMOLQUERY_FLUSH_IDLE_INTERVAL_MS") do
  config :smolquery, Smolquery.BufferService,
    flush_idle_interval_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_FLUSH_IDLE_INTERVAL_MS", interval)
end

if siblings = System.get_env("SMOLQUERY_COMMIT_SIBLINGS") do
  config :smolquery, Smolquery.BufferService,
    commit_siblings:
      Smolquery.RuntimeConfig.non_negative_integer!("SMOLQUERY_COMMIT_SIBLINGS", siblings)
end

if concurrency = System.get_env("SMOLQUERY_ENCODE_CONCURRENCY") do
  config :smolquery, Smolquery.BufferService,
    encode_concurrency:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_ENCODE_CONCURRENCY", concurrency)
end

if after_gcs = System.get_env("SMOLQUERY_BUFFER_FULLSWEEP_AFTER") do
  config :smolquery, Smolquery.BufferService,
    fullsweep_after:
      Smolquery.RuntimeConfig.non_negative_integer!("SMOLQUERY_BUFFER_FULLSWEEP_AFTER", after_gcs)
end

if bytes = System.get_env("SMOLQUERY_FLUSH_MAX_BYTES") do
  config :smolquery, Smolquery.BufferService,
    flush_max_bytes: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_FLUSH_MAX_BYTES", bytes)
end

# T-335: claim sizing was the one seal input fixed in code while every limit it
# has to fit inside — engine memory, spill, and the merge's call budgets — is
# set per deployment. A 488-segment claim exhausted all three in turn and no
# variable could shrink it. The four below are the seal triggers; the valve
# factor multiplies the byte and file triggers into what one claim may freeze.
if bytes = System.get_env("SMOLQUERY_SEAL_MAX_BYTES") do
  config :smolquery, Smolquery.BufferService,
    seal_max_bytes: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_MAX_BYTES", bytes)
end

if files = System.get_env("SMOLQUERY_SEAL_MAX_FILES") do
  config :smolquery, Smolquery.BufferService,
    seal_max_files: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_MAX_FILES", files)
end

if ms = System.get_env("SMOLQUERY_SEAL_MAX_AGE_MS") do
  config :smolquery, Smolquery.BufferService,
    seal_max_age_ms: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_MAX_AGE_MS", ms)
end

if ms = System.get_env("SMOLQUERY_SEAL_RETRY_MS") do
  config :smolquery, Smolquery.BufferService,
    seal_retry_ms: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_RETRY_MS", ms)
end

if factor = System.get_env("SMOLQUERY_CLAIM_VALVE_FACTOR") do
  config :smolquery, Smolquery.BufferService,
    claim_valve_factor:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_CLAIM_VALVE_FACTOR", factor)
end

# T-339: how many claims one table ref may hold open at once. Above 1, the
# buffer keeps freezing valve-sized claims while earlier ones merge, so one
# ref's backlog can occupy several storage seal slots concurrently.
if claims = System.get_env("SMOLQUERY_MAX_LIVE_CLAIMS") do
  config :smolquery, Smolquery.BufferService,
    max_live_claims:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MAX_LIVE_CLAIMS", claims)
end

if seals = System.get_env("SMOLQUERY_MAX_CONCURRENT_SEALS") do
  config :smolquery, Smolquery.StorageService,
    max_concurrent_seals:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MAX_CONCURRENT_SEALS", seals)
end

# T-299: the base takes `0`, the documented switch that disables the per-table
# cooldown. The ceiling does not. `backoff_ms/2` is
# `min(base * 2^n, max)`, so a `0` ceiling returns `0` at every failure count
# and disables the cooldown whatever the base says — a typo in the ceiling
# would silently drop the protection instead of failing the boot.
if ms = System.get_env("SMOLQUERY_SEAL_BACKOFF_BASE_MS") do
  config :smolquery, Smolquery.StorageService,
    seal_backoff_base_ms:
      Smolquery.RuntimeConfig.non_negative_integer!("SMOLQUERY_SEAL_BACKOFF_BASE_MS", ms)
end

if ms = System.get_env("SMOLQUERY_SEAL_BACKOFF_MAX_MS") do
  config :smolquery, Smolquery.StorageService,
    seal_backoff_max_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_BACKOFF_MAX_MS", ms)
end

# The merge's three call budgets (T-261, T-288). A timed-out merge re-stages
# its whole claim, so a budget the claim cannot fit inside never completes.
if ms = System.get_env("SMOLQUERY_MERGE_COPY_TIMEOUT_MS") do
  config :smolquery, Smolquery.StorageService,
    merge_copy_timeout_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MERGE_COPY_TIMEOUT_MS", ms)
end

if ms = System.get_env("SMOLQUERY_MERGE_STAGING_TIMEOUT_MS") do
  config :smolquery, Smolquery.StorageService,
    merge_staging_timeout_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MERGE_STAGING_TIMEOUT_MS", ms)
end

if ms = System.get_env("SMOLQUERY_MERGE_DESCRIBE_TIMEOUT_MS") do
  config :smolquery, Smolquery.StorageService,
    merge_describe_timeout_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MERGE_DESCRIBE_TIMEOUT_MS", ms)
end

# T-246/T-247/T-248: `merge_inputs_per_call` bounds every `read_parquet` list
# one engine call carries — per-input cost over httpfs is what outruns the
# call timeout — so a seal claim or a compaction group of any size merges in
# bounded chunks. The two variables it replaced never shipped in a release,
# but a deployment that set one from main should hear that it went silent.
for removed <- ["SMOLQUERY_SEAL_BATCH_MAX_FILES", "SMOLQUERY_COMPACT_MAX_INPUTS"] do
  if System.get_env(removed) do
    IO.warn(
      "#{removed} is removed and ignored: the merge bounds its own engine calls — " <>
        "set SMOLQUERY_MERGE_INPUTS_PER_CALL instead"
    )
  end
end

if inputs = System.get_env("SMOLQUERY_MERGE_INPUTS_PER_CALL") do
  config :smolquery, Smolquery.StorageService,
    merge_inputs_per_call:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MERGE_INPUTS_PER_CALL", inputs)
end

# T-280: the `ROW_GROUP_SIZE` on every sealed Parquet `COPY`. A sealed-tier
# scan over httpfs pays roughly one range request per row group, so this
# value sets a query's request count per segment.
if size = System.get_env("SMOLQUERY_SEAL_ROW_GROUP_SIZE") do
  config :smolquery, Smolquery.StorageService,
    seal_row_group_size:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_ROW_GROUP_SIZE", size)
end

# T-260: bytes alone do not bound a compaction group's merge cost — on
# highly compressible data the decompressed row count diverges from the
# compressed bytes by ~100x, and merge cost scales with rows.
if rows = System.get_env("SMOLQUERY_COMPACT_MAX_ROWS") do
  config :smolquery, Smolquery.StorageService,
    compact_max_rows:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_COMPACT_MAX_ROWS", rows)
end

# T-269: compaction ownership is per {table, time bucket}, so a hot table's
# backlog spreads across the storage fleet instead of one node compacting
# (and OOMing) alone. The bucket is a segment ULID's timestamp over this.
if ms = System.get_env("SMOLQUERY_COMPACT_BUCKET_MS") do
  config :smolquery, Smolquery.StorageService,
    compact_bucket_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_COMPACT_BUCKET_MS", ms)
end

# T-259: compaction runs on its own engine so a timed-out merge cannot starve
# seals. Unset, the limit derives as a quarter of the cgroup memory limit.
if limit = System.get_env("SMOLQUERY_STORAGE_COMPACT_MEMORY_LIMIT") do
  config :smolquery, Smolquery.StorageService, compact_engine_memory_limit: limit
end

if bytes = System.get_env("SMOLQUERY_MAX_BUFFERED_BYTES") do
  config :smolquery, Smolquery.BufferService,
    max_buffered_bytes:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_MAX_BUFFERED_BYTES", bytes)
end

# One variable sets both halves, because they are one decision: the ingest edge
# only stops parsing if the buffer it forwards to can write the bytes, and a
# buffer that starts DuckDB instances for flushes nothing sends is waste.
if writer = System.get_env("SMOLQUERY_FLUSH_WRITER") do
  flush_writer =
    Smolquery.RuntimeConfig.enum!("SMOLQUERY_FLUSH_WRITER", writer, [
      {"polars", :polars},
      {"duckdb", :duckdb}
    ])

  config :smolquery, Smolquery.BufferService, flush_writer: flush_writer
  config :smolquery, Smolquery.IngestService, ndjson_passthrough: flush_writer == :duckdb
end

if size = System.get_env("SMOLQUERY_WRITE_POOL_SIZE") do
  config :smolquery, Smolquery.BufferService,
    write_pool_size:
      Smolquery.RuntimeConfig.integer_in_range!("SMOLQUERY_WRITE_POOL_SIZE", size, 1, 32)
end

# The two knobs that size one member of the `:duckdb` write pool. Both are
# optional: without them the pool divides `Smolquery.Engine`'s thread count by
# its own size and leaves the memory limit to the application config.
#
# `SMOLQUERY_WRITE_ENGINE_THREADS` replaces the division with a number the
# operator chooses. The division is a sensible default only while the pool is
# smaller than the thread count; above that it reaches its floor of one and
# stops describing a budget, and an operator who wants a different shape needs
# to say so rather than read the arithmetic.
if threads = System.get_env("SMOLQUERY_WRITE_ENGINE_THREADS") do
  config :smolquery, Smolquery.BufferService,
    write_engine_threads:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_WRITE_ENGINE_THREADS", threads)
end

# `SMOLQUERY_READ_ENGINE_THREADS` is the read-side counterpart: each query
# job's private engine takes it as DuckDB `threads`. Left unset, the job
# engine takes DuckDB's own core detection, which a cgroup-limited container
# can misreport. A sealed-tier scan overlaps its httpfs range requests per
# thread, so this value is also the scan's request concurrency ceiling.
if threads = System.get_env("SMOLQUERY_READ_ENGINE_THREADS") do
  config :smolquery, Smolquery.QueryService,
    read_engine_threads:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_READ_ENGINE_THREADS", threads)
end

# `SMOLQUERY_EXTENSION_DIRECTORY` (PL-50) is where DuckDB finds and installs
# its extensions. The image sets it to a directory it ships pre-installed —
# off `/data`, which is an emptyDir that a pod roll wipes, so a fresh pod
# never downloads an extension on its first query.
if directory = System.get_env("SMOLQUERY_EXTENSION_DIRECTORY") do
  config :smolquery, :extension_directory, directory
end

# `SMOLQUERY_WARM_ENGINES` (PL-50) is how many job engines the query service
# keeps bootstrapped ahead of demand; `0` starts every engine cold.
if count = System.get_env("SMOLQUERY_WARM_ENGINES") do
  config :smolquery, Smolquery.QueryService,
    warm_engines: Smolquery.RuntimeConfig.non_negative_integer!("SMOLQUERY_WARM_ENGINES", count)
end

# Distributed queries (PL-49) are on by default: a decomposable aggregate
# query scatters its file list across several DuckDB instances — the query
# service's group members when clustering is on,
# `SMOLQUERY_DISTRIBUTED_LOCAL_WORKERS` local instances when it is off.
# `SMOLQUERY_DISTRIBUTED_QUERY=false` is the kill switch; a job's own
# `distributed` option overrides either way. A query under
# `SMOLQUERY_DISTRIBUTED_MIN_FILES` shardable files, or one that does not
# decompose, runs the normal single-engine path; so does any distributed
# failure. Each worker engine takes `SMOLQUERY_DISTRIBUTED_WORKER_MEMORY_LIMIT`
# (default: the job memory limit, whole) and
# `SMOLQUERY_DISTRIBUTED_WORKER_THREADS` (default: the read engine threads) —
# a node's declared scatter budget is workers x that limit on top of the job
# engine's own, and nothing divides a size string for you.
distributed_query =
  Enum.flat_map(
    [
      {"SMOLQUERY_DISTRIBUTED_QUERY", :enabled, :boolean},
      {"SMOLQUERY_DISTRIBUTED_MIN_FILES", :min_files, :positive_integer},
      {"SMOLQUERY_DISTRIBUTED_LOCAL_WORKERS", :local_workers, :positive_integer},
      {"SMOLQUERY_DISTRIBUTED_WORKER_MEMORY_LIMIT", :worker_memory_limit, :string},
      {"SMOLQUERY_DISTRIBUTED_WORKER_THREADS", :worker_threads, :positive_integer}
    ],
    fn {variable, key, kind} ->
      case {kind, System.get_env(variable)} do
        {_kind, nil} ->
          []

        {:boolean, value} ->
          [{key, Smolquery.RuntimeConfig.boolean!(variable, value)}]

        {:positive_integer, value} ->
          [{key, Smolquery.RuntimeConfig.positive_integer!(variable, value)}]

        {:string, value} ->
          [{key, value}]
      end
    end
  )

if distributed_query != [] do
  config :smolquery, Smolquery.QueryService, distributed: distributed_query
end

# `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT` is the one that cannot be derived: a
# DuckDB memory limit is a size string with its own grammar, so the pool cannot
# divide it the way it divides threads. Left unset, every member inherits
# `Smolquery.Engine`'s limit whole and a node declares `write_pool_size ×` it.
if limit = System.get_env("SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT") do
  config :smolquery, Smolquery.BufferService, write_engine_memory_limit: limit
end

if partitions = System.get_env("SMOLQUERY_WRITE_PARTITIONS") do
  count = Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_WRITE_PARTITIONS", partitions)

  config :smolquery, Smolquery.IngestService, write_partitions: count
  config :smolquery, Smolquery.QueryService, write_partitions: count
end

if hot_server_port = System.get_env("SMOLQUERY_HOT_SERVER_PORT") do
  port = Smolquery.RuntimeConfig.port!("SMOLQUERY_HOT_SERVER_PORT", hot_server_port)

  config :smolquery, Smolquery.BufferService, hot_server_port: port
  config :smolquery, Smolquery.QueryService, buffer_hot_port: port
  config :smolquery, Smolquery.StorageService, buffer_hot_port: port
end

if gen_rpc_port = System.get_env("GEN_RPC_PORT") do
  port = Smolquery.RuntimeConfig.port!("GEN_RPC_PORT", gen_rpc_port)

  config :gen_rpc, tcp_server_port: port, tcp_client_port: port
end

if channels = System.get_env("GEN_RPC_BULK_CHANNELS") do
  config :smolquery, Smolquery.BufferService.Transport.GenRpc,
    bulk_channels: Smolquery.RuntimeConfig.positive_integer!("GEN_RPC_BULK_CHANNELS", channels)
end

if catalog_database_url = System.get_env("CATALOG_DATABASE_URL") do
  db = Smolquery.DatabaseUrl.parse!(catalog_database_url)

  config :smolquery, Smolquery.Cluster,
    enabled: true,
    postgres: [
      hostname: db.hostname,
      port: db.port,
      username: db.username,
      password: db.password,
      database: db.database
    ]

  config :smolquery, Smolquery.Catalog.DuckLake,
    metadata: Smolquery.DatabaseUrl.libpq_metadata(db)

  config :smolquery, Smolquery.BufferService, hot_server_ip: {0, 0, 0, 0}, epoch_fencing: true
end

if hot_server_ip = System.get_env("SMOLQUERY_HOT_SERVER_IP") do
  ip = Smolquery.RuntimeConfig.ip!("SMOLQUERY_HOT_SERVER_IP", hot_server_ip)

  config :smolquery, Smolquery.BufferService, hot_server_ip: ip
end

if s3_bucket = System.get_env("SMOLQUERY_S3_BUCKET") do
  s3_options =
    [
      bucket: s3_bucket,
      access_key_id: System.get_env("SMOLQUERY_S3_ACCESS_KEY_ID"),
      secret_access_key: System.get_env("SMOLQUERY_S3_SECRET_ACCESS_KEY"),
      staging_dir:
        System.get_env("SMOLQUERY_S3_STAGING_DIR") ||
          Path.join(System.get_env("SMOLQUERY_DATA_DIR", "priv/data"), "sealed-staging"),
      endpoint: System.get_env("SMOLQUERY_S3_ENDPOINT"),
      region: System.get_env("SMOLQUERY_S3_REGION"),
      url_style: System.get_env("SMOLQUERY_S3_URL_STYLE")
    ]
    |> Enum.reject(fn {_option, value} -> is_nil(value) end)

  config :smolquery, Smolquery.StorageService, store: {Smolquery.Segments.Store.S3, s3_options}
  config :smolquery, Smolquery.QueryService, store: {Smolquery.Segments.Store.S3, s3_options}
end

if tls = System.get_env("GEN_RPC_TLS") do
  if Smolquery.RuntimeConfig.boolean!("GEN_RPC_TLS", tls) do
    tls_dir = System.get_env("GEN_RPC_TLS_DIR") || "/etc/smolquery/gen-rpc-tls"

    pod_name =
      System.get_env("POD_NAME") || System.get_env("HOSTNAME") ||
        raise "GEN_RPC_TLS requires POD_NAME"

    ssl_options = [
      certfile: Path.join(tls_dir, pod_name <> ".pem"),
      keyfile: Path.join(tls_dir, pod_name <> ".key"),
      cacertfile: Path.join(tls_dir, "ca.pem")
    ]

    ssl_port =
      Smolquery.RuntimeConfig.port!(
        "GEN_RPC_SSL_PORT",
        System.get_env("GEN_RPC_SSL_PORT") || "5870"
      )

    config :gen_rpc,
      default_client_driver: :ssl,
      tcp_server_port: false,
      ssl_server_port: ssl_port,
      ssl_client_port: ssl_port,
      ssl_client_options: ssl_options,
      ssl_server_options: ssl_options
  end
end

if metadata = System.get_env("SMOLQUERY_CATALOG") do
  config :smolquery, Smolquery.Catalog.DuckLake, metadata: metadata
end

if migrate = System.get_env("SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION") do
  config :smolquery, Smolquery.Catalog.DuckLake,
    automatic_migration:
      Smolquery.RuntimeConfig.boolean!("SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION", migrate)
end

if keep = System.get_env("SMOLQUERY_SNAPSHOT_KEEP_MS") do
  config :smolquery, Smolquery.StorageService,
    snapshot_keep_ms:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SNAPSHOT_KEEP_MS", keep)
end
