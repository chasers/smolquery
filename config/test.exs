import Config

config :logger, level: :warning

config :smolquery, roles: []

config :smolquery, Smolquery.Engine,
  memory_limit: "512MB",
  threads: 2,
  extensions: []
