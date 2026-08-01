import Config

config :adbc, :drivers, [:duckdb]

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
  write_timeout_ms: 15_000

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"

import_config "#{config_env()}.exs"
