defmodule Smolquery.DuckDBTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  describe "version/0" do
    test "returns the configured DuckDB driver version" do
      assert DuckDB.version() == "1.5.3"
    end
  end

  describe "ADBC configuration" do
    test "downloads the matching official driver asset" do
      assert [{:duckdb, [version: "1.5.3", url: url]}] = Application.fetch_env!(:adbc, :drivers)

      target = :erlang.system_info(:system_architecture) |> to_string()

      expected_asset =
        cond do
          String.contains?(target, "-darwin") -> "libduckdb-osx-universal.zip"
          String.starts_with?(target, "aarch64-") -> "libduckdb-linux-arm64.zip"
          String.starts_with?(target, "x86_64-") -> "libduckdb-linux-amd64.zip"
          true -> flunk("unsupported test target #{target}")
        end

      assert String.ends_with?(url, expected_asset)
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
    assert Result.one!(result) == "v1.5.3"
  end
end
