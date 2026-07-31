import Config

config :adbc, :drivers, [:duckdb]

import_config "#{config_env()}.exs"
