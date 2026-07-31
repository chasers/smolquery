import Config

config :adbc, :drivers, [:duckdb]

config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  threads: System.schedulers_online(),
  extensions: [:httpfs, :json]

import_config "#{config_env()}.exs"
