defmodule Smolquery.DuckDBTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  describe "version/0" do
    test "returns the configured DuckDB driver version" do
      assert DuckDB.version() == Application.fetch_env!(:smolquery, :duckdb_driver_version)
    end
  end

  describe "ADBC configuration" do
    test "pins the packaged driver to an official asset of the same version" do
      assert [{:duckdb, [version: version, url: url]}] = Application.fetch_env!(:adbc, :drivers)

      assert version == DuckDB.version()
      assert url =~ "duckdb/releases/download/v#{DuckDB.version()}/libduckdb-"
    end
  end

  test "starts a database with the configured driver" do
    database =
      start_supervised!({
        DuckDB,
        version: "1.5.1", process_options: [name: __MODULE__.Database]
      })

    {:ok, connection} = Connection.start_link(database: database)

    assert {:ok, result} = Connection.query(connection, "SELECT version()")
    assert Result.one!(result) == "v" <> DuckDB.version()
  end
end
