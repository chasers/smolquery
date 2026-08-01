import Config

config :logger, level: :info

config :smolquery, Smolquery.Api, ip: {0, 0, 0, 0}

config :smolquery, SmolqueryWeb.Endpoint, cache_static_manifest: "priv/static/cache_manifest.json"
