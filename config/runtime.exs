import Config

if roles = System.get_env("SMOLQUERY_ROLES") do
  config :smolquery, roles: Smolquery.Roles.parse!(roles)
end

if api_key = System.get_env("SMOLQUERY_API_KEY") do
  config :smolquery, SmolqueryApi, api_key: api_key
end

if internal_secret = System.get_env("SMOLQUERY_INTERNAL_SECRET") do
  config :smolquery, :internal_secret, internal_secret
end

if api_port = System.get_env("SMOLQUERY_API_PORT") do
  config :smolquery, SmolqueryApi.Endpoint, http: [port: String.to_integer(api_port)]
end

if api_ip = System.get_env("SMOLQUERY_API_IP") do
  {:ok, ip} = api_ip |> String.to_charlist() |> :inet.parse_address()

  config :smolquery, SmolqueryApi.Endpoint, http: [ip: ip]
end

if web_port = System.get_env("SMOLQUERY_WEB_PORT") do
  config :smolquery, SmolqueryWeb.Endpoint, http: [port: String.to_integer(web_port)]
end

if web_ip = System.get_env("SMOLQUERY_WEB_IP") do
  {:ok, ip} = web_ip |> String.to_charlist() |> :inet.parse_address()

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

if max_rows = System.get_env("SMOLQUERY_MAX_RESULT_ROWS") do
  ceiling =
    case max_rows do
      "infinity" -> :infinity
      rows -> String.to_integer(rows)
    end

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
  config :smolquery,
         Smolquery.BufferService,
         expected_nodes:
           buffer_nodes
           |> String.split(",", trim: true)
           |> Enum.map(&(&1 |> String.trim() |> String.to_atom()))
end

if replication = System.get_env("SMOLQUERY_BUFFER_REPLICATION") do
  config :smolquery, Smolquery.BufferService,
    replicator:
      {Smolquery.BufferService.Replicator.SegmentShipping,
       replication_factor: String.to_integer(replication)}
end

if interval = System.get_env("SMOLQUERY_FLUSH_INTERVAL_MS") do
  config :smolquery, Smolquery.BufferService, flush_interval_ms: String.to_integer(interval)
end

if interval = System.get_env("SMOLQUERY_FLUSH_IDLE_INTERVAL_MS") do
  config :smolquery, Smolquery.BufferService, flush_idle_interval_ms: String.to_integer(interval)
end

if siblings = System.get_env("SMOLQUERY_COMMIT_SIBLINGS") do
  config :smolquery, Smolquery.BufferService, commit_siblings: String.to_integer(siblings)
end

if concurrency = System.get_env("SMOLQUERY_ENCODE_CONCURRENCY") do
  config :smolquery, Smolquery.BufferService, encode_concurrency: String.to_integer(concurrency)
end

if bytes = System.get_env("SMOLQUERY_FLUSH_MAX_BYTES") do
  config :smolquery, Smolquery.BufferService, flush_max_bytes: String.to_integer(bytes)
end

if bytes = System.get_env("SMOLQUERY_MAX_BUFFERED_BYTES") do
  config :smolquery, Smolquery.BufferService, max_buffered_bytes: String.to_integer(bytes)
end

# One variable sets both halves, because they are one decision: the ingest edge
# only stops parsing if the buffer it forwards to can write the bytes, and a
# buffer that starts DuckDB instances for flushes nothing sends is waste.
if writer = System.get_env("SMOLQUERY_FLUSH_WRITER") do
  flush_writer = String.to_existing_atom(writer)

  config :smolquery, Smolquery.BufferService, flush_writer: flush_writer
  config :smolquery, Smolquery.IngestService, ndjson_passthrough: flush_writer == :duckdb
end

if size = System.get_env("SMOLQUERY_WRITE_POOL_SIZE") do
  config :smolquery, Smolquery.BufferService, write_pool_size: String.to_integer(size)
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
  config :smolquery, Smolquery.BufferService, write_engine_threads: String.to_integer(threads)
end

# `SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT` is the one that cannot be derived: a
# DuckDB memory limit is a size string with its own grammar, so the pool cannot
# divide it the way it divides threads. Left unset, every member inherits
# `Smolquery.Engine`'s limit whole and a node declares `write_pool_size ×` it.
if limit = System.get_env("SMOLQUERY_WRITE_ENGINE_MEMORY_LIMIT") do
  config :smolquery, Smolquery.BufferService, write_engine_memory_limit: limit
end

if partitions = System.get_env("SMOLQUERY_WRITE_PARTITIONS") do
  count = String.to_integer(partitions)

  config :smolquery, Smolquery.IngestService, write_partitions: count
  config :smolquery, Smolquery.QueryService, write_partitions: count
end

if hot_server_port = System.get_env("SMOLQUERY_HOT_SERVER_PORT") do
  port = String.to_integer(hot_server_port)

  config :smolquery, Smolquery.BufferService, hot_server_port: port
  config :smolquery, Smolquery.QueryService, buffer_hot_port: port
  config :smolquery, Smolquery.StorageService, buffer_hot_port: port
end

if gen_rpc_port = System.get_env("GEN_RPC_PORT") do
  port = String.to_integer(gen_rpc_port)

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
  {:ok, ip} = hot_server_ip |> String.to_charlist() |> :inet.parse_address()

  config :smolquery, Smolquery.BufferService, hot_server_ip: ip
end

if s3_bucket = System.get_env("SMOLQUERY_S3_BUCKET") do
  s3_options =
    [
      bucket: s3_bucket,
      access_key_id: System.fetch_env!("SMOLQUERY_S3_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("SMOLQUERY_S3_SECRET_ACCESS_KEY"),
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

if System.get_env("GEN_RPC_TLS") in ~w(true 1) do
  tls_dir = System.get_env("GEN_RPC_TLS_DIR") || "/etc/smolquery/gen-rpc-tls"

  pod_name =
    System.get_env("POD_NAME") || System.get_env("HOSTNAME") ||
      raise "GEN_RPC_TLS requires POD_NAME"

  ssl_options = [
    certfile: Path.join(tls_dir, pod_name <> ".pem"),
    keyfile: Path.join(tls_dir, pod_name <> ".key"),
    cacertfile: Path.join(tls_dir, "ca.pem")
  ]

  ssl_port = String.to_integer(System.get_env("GEN_RPC_SSL_PORT") || "5870")

  config :gen_rpc,
    default_client_driver: :ssl,
    tcp_server_port: false,
    ssl_server_port: ssl_port,
    ssl_client_port: ssl_port,
    ssl_client_options: ssl_options,
    ssl_server_options: ssl_options
end

if metadata = System.get_env("SMOLQUERY_CATALOG") do
  config :smolquery, Smolquery.Catalog.DuckLake, metadata: metadata
end

if keep = System.get_env("SMOLQUERY_SNAPSHOT_KEEP_MS") do
  config :smolquery, Smolquery.StorageService, snapshot_keep_ms: String.to_integer(keep)
end
