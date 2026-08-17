import Config

duckdb_driver_version = "1.5.3"
duckdb_target = :erlang.system_info(:system_architecture) |> to_string()

duckdb_driver_url =
  cond do
    String.contains?(duckdb_target, "-darwin") ->
      "https://github.com/duckdb/duckdb/releases/download/v#{duckdb_driver_version}/libduckdb-osx-universal.zip"

    String.contains?(duckdb_target, "-linux-gnu") and
        String.starts_with?(duckdb_target, "aarch64-") ->
      "https://github.com/duckdb/duckdb/releases/download/v#{duckdb_driver_version}/libduckdb-linux-arm64.zip"

    String.contains?(duckdb_target, "-linux-gnu") and
        String.starts_with?(duckdb_target, "x86_64-") ->
      "https://github.com/duckdb/duckdb/releases/download/v#{duckdb_driver_version}/libduckdb-linux-amd64.zip"

    true ->
      nil
  end

# An unrecognized target falls back to adbc's own driver matrix so every Mix
# task still runs there; the engine then refuses to start until the pinned
# version's driver exists for that target.
duckdb_drivers =
  case duckdb_driver_url do
    nil -> [:duckdb]
    url -> [{:duckdb, version: duckdb_driver_version, url: url}]
  end

config :adbc, :drivers, duckdb_drivers
config :smolquery, :duckdb_driver_version, duckdb_driver_version

config :smolquery, Smolquery.Cluster, enabled: false

config :gen_rpc,
  tcp_server_port: 5369,
  tcp_client_port: 5369,
  rpc_module_control: :whitelist,
  rpc_module_list: [Smolquery.BufferService.Endpoint]

config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  extensions: [:httpfs, :json],
  max_result_rows: 100_000

config :smolquery, :data_dir, "priv/data"

config :smolquery, SmolqueryApi.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  http: [ip: {127, 0, 0, 1}, port: 4000],
  render_errors: [formats: [json: SmolqueryApi.ErrorJSON], layout: false],
  server: true

config :smolquery, SmolqueryWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  render_errors: [
    formats: [html: SmolqueryWeb.ErrorHTML, json: SmolqueryWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Smolquery.PubSub,
  live_view: [signing_salt: "M6MLueJD"],
  server: true

config :phoenix, :json_library, JSON

config :phoenix_live_view, root_tag_attribute: "phx-r"

config :esbuild,
  version: "0.25.4",
  smolquery: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

config :tailwind,
  version: "4.3.0",
  smolquery: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("..", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

config :smolquery, Smolquery.IngestService, schema_cache_ttl_ms: 60_000

config :smolquery, Smolquery.BufferService,
  dir: "priv/data/buffer",
  flush_interval_ms: 1_000,
  flush_idle_interval_ms: 5,
  commit_siblings: 5,
  flush_max_rows: 100_000,
  flush_max_bytes: 2_000_000,
  max_buffered_rows: 500_000,
  max_buffered_bytes: 64_000_000,
  ack_budget_ms: 5_000,
  write_timeout_ms: 15_000,
  seal_max_bytes: 67_108_864,
  seal_max_files: 64,
  seal_max_age_ms: 60_000,
  seal_retry_ms: 30_000,
  retire_grace_ms: 600_000,
  maintenance_interval_ms: 5_000,
  hot_server_ip: {127, 0, 0, 1},
  hot_server_port: 4001,
  seal_consumer: {Smolquery.StorageService.Client, []},
  row_validator: {Smolquery.IngestService.Validator, :validate}

config :smolquery, Smolquery.StorageService,
  dir: "priv/data/sealed",
  buffer_base_url: "http://127.0.0.1:4001",
  buffer_timeout_ms: 30_000,
  compression: :zstd,
  seal_row_group_size: 1_048_576,
  target_segment_bytes: 268_435_456,
  max_concurrent_seals: 2,
  gc_interval_ms: 300_000,
  gc_grace_ms: 3_600_000,
  handoff: {Smolquery.StorageService.Handoff.Seal, []}

config :smolquery, Smolquery.QueryService,
  buffer_base_url: "http://127.0.0.1:4001",
  buffer_timeout_ms: 30_000,
  max_concurrent_jobs: 8,
  default_timeout_ms: 60_000,
  job_memory_limit: "1GB",
  result_ttl_ms: 300_000,
  result_max_rows: 10_000

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"

import_config "#{config_env()}.exs"
