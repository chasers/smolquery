import Config

if roles = System.get_env("SMOLQUERY_ROLES") do
  config :smolquery, roles: Smolquery.Roles.parse!(roles)
end

if api_key = System.get_env("SMOLQUERY_API_KEY") do
  config :smolquery, Smolquery.Api, api_key: api_key
end

if internal_secret = System.get_env("SMOLQUERY_INTERNAL_SECRET") do
  config :smolquery, :internal_secret, internal_secret
end

if api_port = System.get_env("SMOLQUERY_API_PORT") do
  config :smolquery, Smolquery.Api, port: String.to_integer(api_port)
end

if api_ip = System.get_env("SMOLQUERY_API_IP") do
  {:ok, ip} = api_ip |> String.to_charlist() |> :inet.parse_address()

  config :smolquery, Smolquery.Api, ip: ip
end

if web_port = System.get_env("SMOLQUERY_WEB_PORT") do
  config :smolquery, SmolqueryWeb.Endpoint, http: [port: String.to_integer(web_port)]
end

if web_ip = System.get_env("SMOLQUERY_WEB_IP") do
  {:ok, ip} = web_ip |> String.to_charlist() |> :inet.parse_address()

  config :smolquery, SmolqueryWeb.Endpoint, http: [ip: ip]
end

if config_env() == :prod do
  secret_key_base =
    System.get_env("SMOLQUERY_SECRET_KEY_BASE") ||
      Base.encode64(:crypto.strong_rand_bytes(48))

  config :smolquery, SmolqueryWeb.Endpoint, secret_key_base: secret_key_base
end

if limit = System.get_env("SMOLQUERY_MEMORY_LIMIT") do
  config :smolquery, Smolquery.Engine, memory_limit: limit
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

if interval = System.get_env("SMOLQUERY_FLUSH_INTERVAL_MS") do
  config :smolquery, Smolquery.BufferService, flush_interval_ms: String.to_integer(interval)
end

if hot_server_port = System.get_env("SMOLQUERY_HOT_SERVER_PORT") do
  config :smolquery, Smolquery.BufferService, hot_server_port: String.to_integer(hot_server_port)
end

if gen_rpc_port = System.get_env("GEN_RPC_PORT") do
  port = String.to_integer(gen_rpc_port)

  config :gen_rpc, tcp_server_port: port, tcp_client_port: port
end

if metadata = System.get_env("SMOLQUERY_CATALOG") do
  config :smolquery, Smolquery.Catalog.DuckLake, metadata: metadata
end

if keep = System.get_env("SMOLQUERY_SNAPSHOT_KEEP_MS") do
  config :smolquery, Smolquery.StorageService, snapshot_keep_ms: String.to_integer(keep)
end
