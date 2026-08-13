import Config

config :logger, level: :debug

config :smolquery, SmolqueryApi, api_key: "smolquery-dev"

config :smolquery, SmolqueryWeb,
  username: "smolquery",
  password: "smolquery"

config :smolquery, SmolqueryWeb.Endpoint,
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base: "CyR7KAD5FaUlQytjJl6g7t6keHCBkeGBQZP/w9GL96jTXy2d5kuEh4V8mZ5NFcQw",
  watchers: [
    esbuild: {Esbuild, :install_and_run, [:smolquery, ~w(--sourcemap=inline --watch)]},
    tailwind: {Tailwind, :install_and_run, [:smolquery, ~w(--watch)]}
  ],
  live_reload: [
    web_console_logger: true,
    patterns: [
      ~r"priv/static/(?!uploads/).*\.(js|css|png|jpeg|jpg|gif|svg)$"E,
      ~r"lib/smolquery_web/router\.ex$"E,
      ~r"lib/smolquery_web/(controllers|live|components)/.*\.(ex|heex)$"E
    ]
  ]

config :phoenix, :stacktrace_depth, 20

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  debug_heex_annotations: true,
  debug_attributes: true,
  enable_expensive_runtime_checks: true
