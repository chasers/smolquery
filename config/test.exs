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

# Explicitly pinned so the suite does not derive its shape from the host's
# scheduler count. `1`/`1` (`write_pool_size`/`encode_concurrency`) is the
# smallest shape that keeps the defaults' one-encode-per-member ratio.
# `commit_siblings: 0` holds the adaptive wait off, so `flush_interval_ms`
# keeps meaning what a test says; adaptive tests opt back in explicitly.
config :smolquery, Smolquery.BufferService,
  hot_server_port: 0,
  seal_consumer: {Smolquery.BufferService.SealLog, []},
  write_pool_size: 1,
  encode_concurrency: 1,
  commit_siblings: 0

config :smolquery, Smolquery.StorageService, engine_extensions: []

config :smolquery, Smolquery.QueryService, engine_extensions: []

config :smolquery, Smolquery.Engine,
  memory_limit: "512MB",
  threads: 2,
  extensions: []
