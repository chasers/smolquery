defmodule Smolquery.QueryService.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.StubCatalog

  @moduletag :tmp_dir

  defp start_service(context, opts \\ []) do
    name = :"query_sup_#{:erlang.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          name: name,
          catalog: [
            metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
            data_path: Path.join(context.tmp_dir, "lake")
          ]
        ],
        opts
      )

    start_supervised!({QueryService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  test "publishes its runtime for callers to read", context do
    name = start_service(context)

    assert {:ok, runtime} = Runtime.fetch(name)
    assert runtime.name == name
  end

  test "starts the catalog engine, the registry, and the runner supervisor", context do
    name = start_service(context)

    assert Process.whereis(Engine.connection_name(Runtime.catalog_engine(name)))
    assert Process.whereis(Runtime.registry(name))
    assert Process.whereis(Runtime.runners(name))
  end

  test "the catalog engine has the lake attached", context do
    name = start_service(context)

    assert {:ok, result} =
             Engine.query(
               Runtime.catalog_engine(name),
               "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'"
             )

    assert result.rows == [[1]]
  end

  test "starts no catalog engine when given a catalog handle outright", context do
    name = start_service(context, catalog: StubCatalog.new(self()))

    refute Process.whereis(Engine.connection_name(Runtime.catalog_engine(name)))
    assert Process.whereis(Runtime.registry(name))
  end
end
