import Config

if roles = System.get_env("SMOLQUERY_ROLES") do
  config :smolquery, roles: Smolquery.Roles.parse!(roles)
end

if limit = System.get_env("SMOLQUERY_MEMORY_LIMIT") do
  config :smolquery, Smolquery.Engine, memory_limit: limit
end

if max_rows = System.get_env("SMOLQUERY_MAX_RESULT_ROWS") do
  ceiling =
    case max_rows do
      "infinity" -> :infinity
      rows -> String.to_integer(rows)
    end

  config :smolquery, Smolquery.Engine, max_result_rows: ceiling
end

if data_dir = System.get_env("SMOLQUERY_DATA_DIR") do
  config :smolquery, :data_dir, data_dir

  config :smolquery, Smolquery.Catalog.DuckLake,
    metadata: "sqlite:#{Path.join(data_dir, "catalog.sqlite")}",
    data_path: Path.join(data_dir, "ducklake")
end

if metadata = System.get_env("SMOLQUERY_CATALOG") do
  config :smolquery, Smolquery.Catalog.DuckLake, metadata: metadata
end
