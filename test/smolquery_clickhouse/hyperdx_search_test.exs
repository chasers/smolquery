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
        "Body" => if(error?, do: "payment failed, id=#{i}", else: "user login ok id=#{i}"),
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

    assert hd(rows) == ["2026-09-19T10:12:59.000000Z", "worker", "info", "user login ok id=119"]

    assert List.last(rows) == [
             "2026-09-19T10:11:00.000000Z",
             "api",
             "error",
             "payment failed, id=0"
           ]
  end

  test "the histogram: count() by severity and minute", context do
    assert %{"meta" => meta, "data" => data, "rows" => 4} = json(context, "histogram")

    assert Enum.map(meta, & &1["name"]) == ["count()", "SeverityText", "__hdx_time_bucket"]

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
end
