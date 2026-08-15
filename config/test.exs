import Config

config :logger, level: :warning

config :gen_rpc, tcp_server_port: 15371, tcp_client_port: 15371

config :smolquery, roles: []

config :smolquery, SmolqueryApi.Endpoint, http: [ip: {127, 0, 0, 1}, port: 0], server: false

config :smolquery, SmolqueryWeb, username: "smolquery", password: "smolquery"

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

# A credential-chain store starts :aws_credentials, whose default chain ends
# at EC2 instance metadata — a link-local address no test host answers, so
# every such test would stall on a connect timeout. The environment provider
# alone fails immediately and keeps the resolution offline.
config :aws_credentials, credential_providers: [:aws_credentials_env]

# Keep spill artifacts from failed tests outside the checkout and isolate
# concurrent test VMs from one another.
config :smolquery,
       :spill_dir,
       Path.join(System.tmp_dir!(), "smolquery-test-spill-#{System.pid()}")
