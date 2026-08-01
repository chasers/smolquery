import Config

config :adbc, :drivers, [:duckdb]

config :gen_rpc,
  tcp_server_port: 5369,
  tcp_client_port: 5369,
  rpc_module_control: :whitelist,
  rpc_module_list: [Smolquery.BufferService.Endpoint]

config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  threads: System.schedulers_online(),
  extensions: [:httpfs, :json],
  max_result_rows: 100_000

config :smolquery, :data_dir, "priv/data"

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
  hot_server_ip: {127, 0, 0, 1},
  hot_server_port: 4001

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"

import_config "#{config_env()}.exs"
