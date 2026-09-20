defmodule SmolqueryClickHouse.VariantJsonTest do
  @moduledoc """
  A `VARIANT` column answers as ClickHouse's `JSON` (T-521), against real
  rows through the whole edge (`Smolquery.Test.FullNode`).

  HyperDX decides how to read a nested key from the column's type string.
  The statements are the ones `@hyperdx/app@2.39.1` builds for a column whose
  type starts with `JSON` (`packages/common-utils/src/queryParser.ts`): a
  path read as text, `toString(metadata.context.application)`, and a number
  test, `dynamicType(path) in (...) and path > n`. The document is shaped as
  a Logflare drain's `metadata` is: three levels deep, with leaves that are
  not strings, which is what `MAP(STRING, STRING)` has no room for.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Engine
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.Schema
  alias Smolquery.Test.FullNode
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @password "variant-json-password"
  @numbers "'Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64'"

  @logflare %{
    "cluster" => "prod-d",
    "level" => "warning",
    "attempts" => 5,
    "ratio" => 1.5,
    "context" => %{"application" => "logflare", "vm" => %{"node" => "n1"}},
    "tags" => ["a", "b"]
  }

  setup context do
    schema =
      Schema.new!([
        {"id", :int64, nullable: false},
        {"event_message", :string},
        {"metadata", :variant}
      ])

    node =
      FullNode.start(context, schema: schema, seal_max_files: 1_000, seal_max_age_ms: 600_000)

    rows = [
      %{"id" => 1, "event_message" => "query ran", "metadata" => @logflare},
      %{
        "id" => 2,
        "event_message" => "other app",
        "metadata" => %{"context" => %{"application" => "api"}}
      },
      %{"id" => 3, "event_message" => "no metadata"}
    ]

    body = Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n"

    {:ok, _ack} =
      BufferService.Client.write_batch(node.buffer, FullNode.table(), %{
        schema: schema,
        ndjson: body,
        row_count: length(rows),
        byte_size: byte_size(body)
      })

    name = :"ch_variant_json_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryClickHouse.Supervisor,
       name: name, password: @password, query_name: node.query, port: 0, catalog: node.catalog},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    {dataset, table} = FullNode.table()

    engine = :"variant_json_engine_#{:erlang.unique_integer([:positive])}"
    start_supervised!({Engine, name: engine, extensions: [:json]}, id: engine)

    Enum.each(
      ClickHouseFunctions.statements_for("JSONDynamicPathsWithTypes("),
      &Engine.query!(engine, &1)
    )

    %{name: name, from: "#{dataset}.#{table}", dataset: dataset, table: table, engine: engine}
  end

  defp json(name, sql) do
    response =
      conn(:post, "/", sql <> " FORMAT JSON")
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(name)

    assert response.status == 200, response.resp_body

    JSON.decode!(response.resp_body)
  end

  test "DESCRIBE and system.columns say JSON, the prefix HyperDX reads", context do
    described = json(context.name, "DESCRIBE #{context.from}")["data"]

    assert Enum.find(described, &(&1["name"] == "metadata"))["type"] == "JSON"

    assert [%{"type" => "JSON"}] =
             json(
               context.name,
               "SELECT type FROM system.columns WHERE database = '#{context.dataset}' " <>
                 "AND table = '#{context.table}' AND name = 'metadata'"
             )["data"]
  end

  test "a nested key reads as text, plain and backticked, and finds its row", context do
    path = "toString(metadata.context.application)"

    assert %{"meta" => [_id, %{"name" => "app", "type" => "Nullable(String)"}], "data" => data} =
             json(context.name, "SELECT id, #{path} AS app FROM #{context.from} ORDER BY id")

    assert data == [
             %{"id" => "1", "app" => "logflare"},
             %{"id" => "2", "app" => "api"},
             %{"id" => "3", "app" => nil}
           ]

    assert [%{"id" => "1"}] =
             json(context.name, "SELECT id FROM #{context.from} WHERE (#{path} = 'logflare')")[
               "data"
             ]

    assert [%{"node" => "n1"}] =
             json(
               context.name,
               "SELECT toString(`metadata`.`context`.`vm`.`node`) AS node FROM #{context.from} WHERE id = 1"
             )["data"]

    assert [%{"n" => "1"}] =
             json(
               context.name,
               "SELECT count() AS n FROM #{context.from} WHERE NOT (#{path} = 'logflare')"
             )["data"]
  end

  test "a number is told by dynamicType, as HyperDX asks, and compares as one", context do
    where = "(dynamicType(metadata.attempts) in (#{@numbers}) and metadata.attempts > 3)"

    assert [%{"id" => "1"}] =
             json(context.name, "SELECT id FROM #{context.from} WHERE #{where}")["data"]

    assert [] ==
             json(
               context.name,
               "SELECT id FROM #{context.from} WHERE (dynamicType(metadata.cluster) in (#{@numbers}) and metadata.cluster > 3)"
             )["data"]

    assert [types] =
             json(
               context.name,
               "SELECT dynamicType(metadata.attempts) AS i, dynamicType(metadata.ratio) AS f, " <>
                 "dynamicType(metadata.cluster) AS s, dynamicType(metadata.tags) AS a, " <>
                 "dynamicType(metadata.context) AS o, dynamicType(metadata.nowhere) AS none " <>
                 "FROM #{context.from} WHERE id = 1"
             )["data"]

    assert types == %{
             "i" => "Int64",
             "f" => "Float64",
             "s" => "String",
             "a" => "Array(Nullable(String))",
             "o" => "JSON",
             "none" => "None"
           }
  end

  test "a path read without toString is Dynamic, and a string it holds is valid JSON (review of T-521)",
       context do
    assert %{"meta" => meta, "data" => [row]} =
             json(
               context.name,
               "SELECT metadata.cluster AS c, metadata.attempts AS n, metadata.tags AS t, " <>
                 "metadata.context AS o FROM #{context.from} WHERE id = 1"
             )

    assert Enum.map(meta, & &1["type"]) == ["Dynamic", "Dynamic", "Dynamic", "JSON"]

    assert row == %{
             "c" => "prod-d",
             "n" => 5,
             "t" => ["a", "b"],
             "o" => %{"application" => "logflare", "vm" => %{"node" => "n1"}}
           }
  end

  test "the column itself answers typed JSON, as the nested document", context do
    assert %{"meta" => [_id, %{"name" => "metadata", "type" => "JSON"}], "data" => data} =
             json(context.name, "SELECT id, metadata FROM #{context.from} ORDER BY id")

    assert [
             %{"metadata" => @logflare},
             %{"metadata" => %{"context" => _app}},
             %{"metadata" => nil}
           ] =
             data
  end

  test "the keys of a JSON column list as HyperDX's getJSONKeys asks, for the filters sidebar",
       context do
    sql = """
    WITH all_paths AS
    (
        SELECT DISTINCT JSONDynamicPathsWithTypes(`metadata`) as paths
        FROM #{context.from}
        LIMIT 1000
        SETTINGS timeout_overflow_mode = 'break', max_execution_time = 2
    )
    SELECT groupUniqArrayMap(paths) as pathMap
    FROM all_paths;
    """

    response =
      conn(:post, "/?default_format=JSON", sql)
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(context.name)

    assert response.status == 200, response.resp_body

    assert %{
             "meta" => [%{"name" => "pathMap", "type" => "Map(String, Array(String))"}],
             "data" => [row]
           } =
             JSON.decode!(response.resp_body)

    assert row["pathMap"] == %{
             "cluster" => ["String"],
             "level" => ["String"],
             "attempts" => ["Int64"],
             "ratio" => ["Float64"],
             "context.application" => ["String"],
             "context.vm.node" => ["String"],
             "tags" => ["Array(Nullable(String))"]
           }

    assert %{"data" => [%{"pathMap" => %{}}]} =
             json(
               context.name,
               "WITH all_paths AS (SELECT DISTINCT JSONDynamicPathsWithTypes(metadata) as paths " <>
                 "FROM #{context.from} WHERE id > 99) SELECT groupUniqArrayMap(paths) as pathMap FROM all_paths"
             )
  end

  test "a key holding a bracket is listed, an array is one leaf whatever it holds, and a dotted key reads as ClickHouse reads it (review of T-521)",
       context do
    {:ok, %{rows: [[paths]]}} =
      Engine.query(
        context.engine,
        ~s|SELECT JSONDynamicPathsWithTypes('{"a[0]": 1, "g.h": 2, "tags": ["x", {"k": 1}], "o": {"n": 1.5}}'::JSON::VARIANT)|
      )

    assert paths == %{
             "a[0]" => "Int64",
             "g.h" => "Int64",
             "tags" => "Array(Nullable(String))",
             "o.n" => "Float64"
           }
  end
end
