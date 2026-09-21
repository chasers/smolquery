defmodule SmolqueryClickHouse.HyperdxSearchTest do
  @moduledoc """
  What HyperDX sends to open its Search page, run against real rows (T-495).

  The statements are the fixture's, verbatim from HyperDX's source, with its
  settings and its `param_*` values in the URL and the `FORMAT` clause
  clickhouse-js appends. The rows are written into the buffer and read back
  through the planner, so every step crosses the whole edge: parameters,
  quoting, the rewrite, the emulated catalog, the macros and the formats.
  The assertions are on what HyperDX reads: `meta` names and types, `data`
  and `rows`.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.Schema
  alias Smolquery.Test.FullNode
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @password "hyperdx-search-password"
  @fixture "test/support/fixtures/clickstack/hyperdx_search.json"
  @first ~N[2026-09-19 10:11:00]

  setup context do
    schema =
      Schema.new!([
        {"Timestamp", :timestamp_ns, nullable: false},
        {"ServiceName", :string},
        {"SeverityText", :string},
        {"Body", :string},
        {"LogAttributes", {:map, :string, :string}}
      ])

    node =
      FullNode.start(context, schema: schema, seal_max_files: 1_000, seal_max_age_ms: 600_000)

    :ok = Catalog.put_clustering(node.catalog, FullNode.table(), ["ServiceName", "Timestamp"])

    {:ok, _ack} =
      BufferService.Client.write_batch(node.buffer, FullNode.table(), %{
        schema: schema,
        rows: rows()
      })

    name = :"ch_hyperdx_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryClickHouse.Supervisor,
       name: name, password: @password, query_name: node.query, port: 0, catalog: node.catalog},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, fixture: @fixture |> File.read!() |> JSON.decode!()}
  end

  defp rows do
    for i <- 0..119 do
      error? = rem(i, 4) == 0

      %{
        "Timestamp" => NaiveDateTime.add(@first, i),
        "ServiceName" => if(rem(i, 2) == 0, do: "api", else: "worker"),
        "SeverityText" => if(error?, do: "error", else: "info"),
        "Body" =>
          if(error?, do: "payment failed, id=#{i} user_id=u#{i}", else: "user login ok id=#{i}"),
        "LogAttributes" => %{"http.status" => if(error?, do: "500", else: "200")}
      }
    end
  end

  defp run(%{name: name, fixture: fixture}, step) do
    %{"sql" => sql, "format" => format, "params" => params} =
      Enum.find(fixture["steps"], &(&1["step"] == step))

    query =
      fixture["settings"]
      |> Map.merge(Map.new(params, fn {key, value} -> {"param_" <> key, value} end))
      |> Map.put("query_id", "hyperdx-#{step}")

    response =
      conn(:post, "/?" <> URI.encode_query(query), sql <> " \nFORMAT " <> format)
      |> put_req_header("x-clickhouse-user", "default")
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(name)

    assert response.status == 200, "#{step}: #{response.resp_body}"

    response.resp_body
  end

  defp json(context, step), do: context |> run(step) |> JSON.decode!()

  defp lines(context, step),
    do: context |> run(step) |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

  test "the metadata HyperDX reads before its first query", context do
    assert json(context, "settings")["data"] == []
    assert [%{"version" => "24.8.1.1"}] = json(context, "version")["data"]
    assert json(context, "skip_indices")["data"] == []
    assert json(context, "cloud_probe")["data"] == [%{"is_cloud" => false}]

    assert Enum.map(json(context, "describe")["data"], &{&1["name"], &1["type"]}) == [
             {"Timestamp", "DateTime64(9)"},
             {"ServiceName", "Nullable(String)"},
             {"SeverityText", "Nullable(String)"},
             {"Body", "Nullable(String)"},
             {"LogAttributes", "Map(String, String)"}
           ]

    assert [%{"engine" => "MergeTree", "sorting_key" => "ServiceName, Timestamp"}] =
             json(context, "table_metadata")["data"]
  end

  test "the results table: names, types, then the newest rows first", context do
    assert [names, types | rows] = lines(context, "rows")

    assert names == ["Timestamp", "ServiceName", "SeverityText", "Body"]
    assert hd(types) =~ "DateTime64(9)"
    assert Enum.count(rows) == 120

    assert hd(rows) == [
             "2026-09-19T10:12:59.000000000Z",
             "worker",
             "info",
             "user login ok id=119"
           ]

    assert List.last(rows) == [
             "2026-09-19T10:11:00.000000000Z",
             "api",
             "error",
             "payment failed, id=0 user_id=u0"
           ]
  end

  test "the histogram: count() by severity and minute", context do
    assert %{"meta" => meta, "data" => data, "rows" => 4} = json(context, "histogram")

    assert Enum.map(meta, & &1["name"]) == ["count()", "SeverityText", "__hdx_time_bucket"]

    assert Enum.map(meta, & &1["type"]) == ["Int64", "Nullable(String)", "DateTime64(6)"],
           "HyperDX's chart looks for a date-typed column in meta and does not unwrap Nullable (T-510)"

    assert Enum.sort_by(data, &{&1["__hdx_time_bucket"], &1["SeverityText"]}) == [
             %{
               "count()" => "15",
               "SeverityText" => "error",
               "__hdx_time_bucket" => "2026-09-19T10:11:00.000000Z"
             },
             %{
               "count()" => "45",
               "SeverityText" => "info",
               "__hdx_time_bucket" => "2026-09-19T10:11:00.000000Z"
             },
             %{
               "count()" => "15",
               "SeverityText" => "error",
               "__hdx_time_bucket" => "2026-09-19T10:12:00.000000Z"
             },
             %{
               "count()" => "45",
               "SeverityText" => "info",
               "__hdx_time_bucket" => "2026-09-19T10:12:00.000000Z"
             }
           ]
  end

  test "a search term finds its token whatever its case", context do
    assert [_names, _types | rows] = lines(context, "rows_term")

    assert Enum.count(rows) == 30

    assert Enum.all?(rows, fn [_ts, _service, severity, body] ->
             severity == "error" and body =~ "payment"
           end)
  end

  test "field, existence and map-key filters count the rows they name", context do
    assert [%{"count()" => "30"}] = json(context, "count_field_filters")["data"]
  end

  test "the filters sidebar: a map's keys and each field's values (T-496)", context do
    assert [%{"keysArr" => ["http.status"]}] = json(context, "map_keys")["data"]

    assert %{"meta" => meta, "data" => [values]} = json(context, "key_values")

    assert Enum.map(meta, & &1["type"]) == ["Array(Nullable(String))", "Array(Nullable(String))"]
    assert Enum.sort(values["param0"]) == ["api", "worker"]
    assert Enum.sort(values["param1"]) == ["error", "info"]
  end

  test "a term with an underscore matches it, not any character (T-496)", context do
    assert [_names, _types | rows] = lines(context, "rows_underscore_term")

    assert Enum.count(rows) == 30
    assert Enum.all?(rows, fn [_ts, _service, _severity, body] -> body =~ "user_id=" end)
  end

  test "EXPLAIN ESTIMATE of the search answers the rows its plan reads (T-506)", context do
    %{"sql" => sql, "params" => params} =
      Enum.find(context.fixture["steps"], &(&1["step"] == "rows"))

    query =
      context.fixture["settings"]
      |> Map.merge(Map.new(params, fn {key, value} -> {"param_" <> key, value} end))

    response =
      conn(
        :post,
        "/?" <> URI.encode_query(query),
        "EXPLAIN ESTIMATE " <> sql <> " \nFORMAT JSONEachRow"
      )
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(context.name)

    assert response.status == 200, response.resp_body
    assert %{"rows" => "120", "parts" => "1"} = JSON.decode!(String.trim(response.resp_body))
  end

  test "the onboarding checklist's row count is the table's rows, hot tier included (T-507)",
       context do
    sql =
      "SELECT sum(total_rows) as total_rows FROM {d:Identifier}.{t:Identifier} " <>
        "WHERE ((table = 'events' AND database = 'analytics')) \nFORMAT JSON"

    query =
      Map.merge(context.fixture["settings"], %{"param_d" => "system", "param_t" => "tables"})

    response =
      conn(:post, "/?" <> URI.encode_query(query), sql)
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(context.name)

    assert response.status == 200, response.resp_body
    assert [%{"total_rows" => 120}] = JSON.decode!(response.resp_body)["data"]
  end

  describe "total_rows is the statement's own (review of T-507)" do
    defp catalog(context, sql, query \\ %{}) do
      response =
        conn(:post, "/?" <> URI.encode_query(query), sql <> " FORMAT JSONCompact")
        |> put_req_header("x-clickhouse-key", @password)
        |> Router.call(context.name)

      {response.status, response}
    end

    defp data(context, sql) do
      {200, response} = catalog(context, sql)

      JSON.decode!(response.resp_body)["data"]
    end

    test "one statement's counts are not the next statement's", context do
      assert data(context, "SELECT total_rows FROM system.tables WHERE name = 'events'") == [
               ["120"]
             ]

      assert data(context, "SELECT name, engine FROM system.tables WHERE name = 'events'") == [
               ["events", "MergeTree"]
             ]

      {200, response} = catalog(context, "SELECT * FROM system.tables WHERE name = 'events'")
      [row] = JSON.decode!(response.resp_body)["data"]
      names = Enum.map(JSON.decode!(response.resp_body)["meta"], & &1["name"])

      assert Enum.at(row, Enum.find_index(names, &(&1 == "total_rows"))) == nil
    end

    test "the tables counted are the rows the statement's own WHERE selects", context do
      for where <- [
            "database = 'analytics'",
            "name LIKE 'eve%'",
            "name != 'nothing'",
            "database != 'system' AND name = 'events'",
            "1 = 1"
          ] do
        assert data(context, "SELECT sum(total_rows) AS n FROM system.tables WHERE #{where}") == [
                 [120]
               ],
               where
      end

      assert data(
               context,
               "SELECT sum(total_rows) AS n FROM system.tables WHERE name != 'events'"
             ) == [[nil]]
    end

    test "a qualified or quoted total_rows, and a spaced or quoted table name, are read the same",
         context do
      for sql <- [
            "SELECT t.total_rows FROM system.tables AS t WHERE t.name = 'events'",
            ~s|SELECT "total_rows" FROM system.tables WHERE name = 'events'|,
            ~s|SELECT total_rows FROM "system" . "tables" WHERE name = 'events'|
          ] do
        assert data(context, sql) == [["120"]], sql
      end
    end
  end

  describe "rand(), which HyperDX samples with (T-526)" do
    defp ask(%{name: name}, sql) do
      response =
        conn(:post, "/", sql <> " FORMAT JSON")
        |> put_req_header("x-clickhouse-key", @password)
        |> Router.call(name)

      assert response.status == 200, response.resp_body

      JSON.decode!(response.resp_body)
    end

    test "a window relative to now() compares with a DateTime64(9) column (T-542)", context do
      {dataset, table} = FullNode.table()
      from = "FROM #{dataset}.#{table}"

      assert %{"data" => [%{"n" => "120"}]} =
               ask(context, "SELECT count() AS n #{from} WHERE Timestamp <= now()")

      assert %{"data" => [%{"n" => "0"}]} =
               ask(
                 context,
                 "SELECT count() AS n #{from} WHERE Timestamp >= now() + INTERVAL 1 DAY"
               )

      assert %{"data" => [%{"n" => "120"}]} =
               ask(
                 context,
                 "SELECT count() AS n #{from} WHERE Timestamp BETWEEN now() - INTERVAL 100 YEAR AND now()"
               )
    end

    test "Event Patterns and Event Deltas order a sample by it, over a real table and under a LIMIT",
         context do
      {dataset, table} = FullNode.table()
      sql = "SELECT Body, Timestamp FROM #{dataset}.#{table} ORDER BY rand() DESC LIMIT 10"

      assert %{"rows" => 10, "data" => first} = ask(context, sql)
      assert %{"rows" => 10, "data" => second} = ask(context, sql)

      assert Enum.all?(first ++ second, &is_binary(&1["Body"]))

      samples = for _run <- 1..4, do: ask(context, sql)["data"]
      refute match?([_one_order], Enum.uniq([first, second | samples]))

      assert %{"rows" => 120} =
               ask(context, "SELECT Body FROM #{dataset}.#{table} ORDER BY rand() LIMIT 1000")
    end

    test "the sidebar thins a large table with cityHash64(ts, rand()) % n, and keeps a share of it",
         context do
      {dataset, table} = FullNode.table()

      assert %{"data" => [%{"kept" => kept}]} =
               ask(
                 context,
                 "WITH tableStats AS (SELECT 2 AS sample_factor) SELECT count() AS kept " <>
                   "FROM #{dataset}.#{table} " <>
                   "WHERE cityHash64(Timestamp, rand()) % (SELECT sample_factor FROM tableStats) = 0"
               )

      kept = String.to_integer(kept)
      assert kept > 20 and kept < 100
    end
  end
end
