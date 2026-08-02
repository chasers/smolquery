import Config

config :logger, level: :warning

config :gen_rpc, tcp_server_port: 15371, tcp_client_port: 15371

config :smolquery, roles: []

config :smolquery, SmolqueryApi.Endpoint, http: [ip: {127, 0, 0, 1}, port: 0], server: false

config :smolquery, SmolqueryWeb.ClusterLive.Index, pod_actions: false

config :smolquery, SmolqueryWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 0],
  secret_key_base: "vN2tBnLkDhX4wG9pQmZrY7cJfA1sE6uHb3TgVjR8xWqK5yMdC0aPoUiSlF2hNzEe",
  server: false

config :smolquery, Smolquery.BufferService,
  hot_server_port: 0,
  seal_consumer: {Smolquery.BufferService.SealLog, []}

config :smolquery, Smolquery.StorageService, engine_extensions: []

config :smolquery, Smolquery.QueryService, engine_extensions: []

config :smolquery, Smolquery.Engine,
  memory_limit: "512MB",
  threads: 2,
  extensions: []
