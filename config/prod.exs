import Config

config :logger, level: :info

config :smolquery, SmolqueryApi.Endpoint, http: [ip: {0, 0, 0, 0}, port: 4000]

config :smolquery, SmolqueryWeb.Endpoint, cache_static_manifest: "priv/static/cache_manifest.json"
