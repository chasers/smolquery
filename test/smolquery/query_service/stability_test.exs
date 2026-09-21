defmodule Smolquery.QueryService.StabilityTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Stability

  @engine __MODULE__.Engine
  @conn Engine.connection_name(@engine)

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    Engine.query!(@engine, "CREATE MACRO theirs(x) AS x + 1")

    :ok
  end

  defp statement(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    result |> Result.one!() |> JSON.decode!()
  end

  defp one(sql) do
    {:ok, result} = Connection.query(@conn, "SELECT " <> sql)

    Result.one!(result)
  end

  test "function_names/1 is every function named, lower-cased and once" do
    sql = "SELECT Lower(name), lower(name), now() FROM t WHERE ts > fromUnixTimestamp64Milli(5)"

    assert Enum.sort(Stability.function_names(statement(sql))) ==
             ["fromunixtimestamp64milli", "lower", "now"]
  end

  test "checked/1 leaves out the ClickHouse macros, which are trusted by name" do
    assert Stability.checked(["lower", "fromunixtimestamp64milli", "theirs"]) == [
             "lower",
             "theirs"
           ]
  end

  test "the catalog calls a volatile function and a macro unstable, and the clock stable within a query" do
    names = ["lower", "now", "random", "theirs"]

    assert one(Stability.unstable_count_sql(names)) == 2
    assert Enum.sort(one(Stability.unstable_names_sql(names))) == ["random", "theirs"]
    assert one(Stability.within_query_names_sql(names)) == ["now"]
  end

  test "no names is nothing to ask" do
    assert Stability.unstable_count_sql([]) == "0"
    assert one(Stability.unstable_names_sql([])) == []
    assert one(Stability.within_query_names_sql([])) == []
  end
end
