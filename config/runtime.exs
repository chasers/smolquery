import Config

if roles = System.get_env("SMOLQUERY_ROLES") do
  config :smolquery, roles: Smolquery.Roles.parse!(roles)
end

if auth_mode = System.get_env("SMOLQUERY_AUTH_MODE") do
  mode =
    Smolquery.RuntimeConfig.enum!("SMOLQUERY_AUTH_MODE", auth_mode, [
      {"static", :static},
      {"oidc", :oidc}
    ])

  config :smolquery, SmolqueryApi, auth_mode: mode
  config :smolquery, SmolqueryWeb, auth_mode: mode
end

if issuer = System.get_env("SMOLQUERY_OIDC_ISSUER") do
  config :smolquery, Smolquery.Auth.OIDC.Config, issuer: issuer
end

if audience = System.get_env("SMOLQUERY_OIDC_API_AUDIENCE") do
  config :smolquery, Smolquery.Auth.OIDC.Config, api_audience: audience
end

if client_id = System.get_env("SMOLQUERY_OIDC_WEB_CLIENT_ID") do
  config :smolquery, Smolquery.Auth.OIDC.Config, web_client_id: client_id
end

if client_secret = System.get_env("SMOLQUERY_OIDC_WEB_CLIENT_SECRET") do
  config :smolquery, Smolquery.Auth.OIDC.Config, web_client_secret: client_secret
end

if auth_method = System.get_env("SMOLQUERY_OIDC_WEB_CLIENT_AUTH_METHOD") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    web_client_auth_method:
      Smolquery.RuntimeConfig.enum!("SMOLQUERY_OIDC_WEB_CLIENT_AUTH_METHOD", auth_method, [
        {"none", :none},
        {"client_secret_basic", :client_secret_basic}
      ])
end

if origin = System.get_env("SMOLQUERY_OIDC_WEB_ORIGIN") do
  config :smolquery, Smolquery.Auth.OIDC.Config, web_origin: origin
end

if redirect_uri = System.get_env("SMOLQUERY_OIDC_WEB_REDIRECT_URI") do
  config :smolquery, Smolquery.Auth.OIDC.Config, web_redirect_uri: redirect_uri
end

if algorithms = System.get_env("SMOLQUERY_OIDC_ALGORITHMS") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    algorithms: Smolquery.RuntimeConfig.csv!("SMOLQUERY_OIDC_ALGORITHMS", algorithms)
end

if skew = System.get_env("SMOLQUERY_OIDC_CLOCK_SKEW") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    clock_skew: Smolquery.RuntimeConfig.non_negative_integer!("SMOLQUERY_OIDC_CLOCK_SKEW", skew)
end

if mappings = System.get_env("SMOLQUERY_OIDC_CLAIM_CAPABILITIES") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    claim_capabilities:
      Smolquery.RuntimeConfig.capability_mapping!("SMOLQUERY_OIDC_CLAIM_CAPABILITIES", mappings)
end

if token_types = System.get_env("SMOLQUERY_OIDC_TOKEN_TYPES") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    typ_allowlist: Smolquery.RuntimeConfig.csv!("SMOLQUERY_OIDC_TOKEN_TYPES", token_types)
end

if required_claims = System.get_env("SMOLQUERY_OIDC_REQUIRED_CLAIMS") do
  config :smolquery, Smolquery.Auth.OIDC.Config,
    required_claims:
      Smolquery.RuntimeConfig.string_lists!("SMOLQUERY_OIDC_REQUIRED_CLAIMS", required_claims)
end

for {env, key, max} <- [
      {"SMOLQUERY_OIDC_MAX_TOKEN_BYTES", :max_token_bytes, 1_048_576},
      {"SMOLQUERY_OIDC_MAX_TOKEN_SEGMENT_BYTES", :max_segment_bytes, 524_288},
      {"SMOLQUERY_OIDC_IAT_FUTURE_SECONDS", :iat_future_seconds, 86_400},
      {"SMOLQUERY_OIDC_DISCOVERY_MAX_AGE_MS", :discovery_max_age_ms, 86_400_000},
      {"SMOLQUERY_OIDC_JWKS_MAX_AGE_MS", :jwks_max_age_ms, 86_400_000},
      {"SMOLQUERY_OIDC_FORCED_REFRESH_COOLDOWN_MS", :forced_refresh_cooldown_ms, 86_400_000},
      {"SMOLQUERY_OIDC_CONNECT_TIMEOUT_MS", :connect_timeout_ms, 30_000},
      {"SMOLQUERY_OIDC_RECEIVE_TIMEOUT_MS", :receive_timeout_ms, 60_000},
      {"SMOLQUERY_OIDC_REQUEST_TIMEOUT_MS", :request_timeout_ms, 120_000},
      {"SMOLQUERY_OIDC_MAX_BODY_BYTES", :max_body_bytes, 10_485_760}
    ] do
  if value = System.get_env(env) do
    config :smolquery, Smolquery.Auth.OIDC.Config, [
      {key, Smolquery.RuntimeConfig.bounded_non_negative_integer!(env, value, max)}
    ]
  end
end

if api_key = System.get_env("SMOLQUERY_API_KEY") do
  config :smolquery, SmolqueryApi, api_key: api_key
end

if internal_secret = System.get_env("SMOLQUERY_INTERNAL_SECRET") do
  config :smolquery, :internal_secret, internal_secret
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

if bytes = System.get_env("SMOLQUERY_FLUSH_MAX_BYTES") do
  config :smolquery, Smolquery.BufferService,
    flush_max_bytes: Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_FLUSH_MAX_BYTES", bytes)
end

# T-244: both caps bound one DuckDB call's `read_parquet` input list. The seal
# batch grows with ingest throughput and the compaction group with backlog, so
# without a cap either can outrun the engine's call timeout — and a frozen seal
# claim then retries the same oversized merge forever.
if files = System.get_env("SMOLQUERY_SEAL_BATCH_MAX_FILES") do
  config :smolquery, Smolquery.BufferService,
    seal_batch_max_files:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_SEAL_BATCH_MAX_FILES", files)
end

if inputs = System.get_env("SMOLQUERY_COMPACT_MAX_INPUTS") do
  config :smolquery, Smolquery.StorageService,
    compact_max_inputs:
      Smolquery.RuntimeConfig.positive_integer!("SMOLQUERY_COMPACT_MAX_INPUTS", inputs)
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
