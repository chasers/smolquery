import Config

config :logger, level: :warning

config :gen_rpc, tcp_server_port: 15371, tcp_client_port: 15371

config :smolquery, roles: []

config :smolquery, Smolquery.BufferService,
  hot_server_port: 0,
  seal_consumer: {Smolquery.BufferService.SealLog, []}

config :smolquery, Smolquery.Engine,
  memory_limit: "512MB",
  threads: 2,
  extensions: []
