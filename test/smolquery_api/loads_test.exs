defmodule SmolqueryApi.LoadControllerTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @moduletag :tmp_dir

  @key "loads-test-key"
  @path "/v1/datasets/analytics/tables/events/load"

  setup context do
    buffer = :"api_load_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, {"analytics", "events"}, schema())

    ingest = :"api_load_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, api_key: @key, catalog: catalog, ingest_name: ingest)
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, buffer: buffer}
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}, {"ts", :timestamp}])
  end

  defp load(name, body, content_type, path \\ @path) do
    conn(:post, path, body)
    |> put_req_header("content-type", content_type)
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp row_count(buffer) do
    {:ok, entries} = BufferService.Client.hot_manifest(buffer, {"analytics", "events"})

    Enum.sum(Enum.map(entries, & &1.row_count))
  end

  test "loads ndjson through the insert path", %{name: name, buffer: buffer} do
    body = """
    {"id": 1, "name": "a", "ts": "2026-08-01T10:00:00Z"}

    {"id": 2}
    """

    response = load(name, body, "application/x-ndjson")

    assert response.status == 200

    assert JSON.decode!(response.resp_body) == %{
             "insertedRows" => 2,
             "errorCount" => 0,
             "insertErrors" => []
           }

    assert row_count(buffer) == 2
  end

  test "a broken ndjson line is a per-row error at its line index", %{name: name} do
    body = """
    {"id": 1}
    not json at all
    {"id": "junk"}
    """

    response = load(name, body, "application/x-ndjson")

    assert %{"insertedRows" => 1, "errorCount" => 2, "insertErrors" => errors} =
             JSON.decode!(response.resp_body)

    assert Enum.map(errors, & &1["index"]) |> Enum.sort() == [1, 2]
  end

  test "loads csv typed by the table's schema", %{name: name, buffer: buffer} do
    body = """
    id,name,ts
    1,alpha,2026-08-01T10:00:00
    2,beta,2026-08-01T11:00:00
    """

    response = load(name, body, "text/csv")

    assert JSON.decode!(response.resp_body)["insertedRows"] == 2
    assert row_count(buffer) == 2
  end

  test "loads parquet", %{name: name, buffer: buffer} do
    body =
      [
        {"id", Series.from_list([1, 2, 3], dtype: {:s, 64})},
        {"name", Series.from_list(["a", "b", "c"])},
        {"ts", Series.from_list([nil, nil, nil], dtype: {:naive_datetime, :microsecond})}
      ]
      |> DataFrame.new()
      |> DataFrame.dump_parquet!()

    response = load(name, body, "application/vnd.apache.parquet")

    assert JSON.decode!(response.resp_body)["insertedRows"] == 3
    assert row_count(buffer) == 3
  end

  test "an unparseable csv is a 400", %{name: name} do
    response = load(name, "id,name,ts\n\"unclosed", "text/csv")

    assert response.status == 400
    assert %{"error" => %{"status" => "INVALID_ARGUMENT"}} = JSON.decode!(response.resp_body)
  end

  test "a json body on the load route is a 415 naming the load formats", %{name: name} do
    response = load(name, ~s({"rows": []}), "application/json")

    assert response.status == 415
    assert %{"error" => %{"message" => message}} = JSON.decode!(response.resp_body)
    assert message =~ "ndjson"
  end

  test "a body over the cap is a 413", %{name: name} do
    {:ok, runtime} = Runtime.fetch(name)
    Runtime.put(%{runtime | load_max_bytes: 10})

    response = load(name, ~s({"id": 1, "name": "far too long"}\n), "application/x-ndjson")

    assert response.status == 413
  end

  test "an unknown table is a 404", %{name: name} do
    response =
      load(
        name,
        ~s({"id": 1}\n),
        "application/x-ndjson",
        "/v1/datasets/analytics/tables/nope/load"
      )

    assert response.status == 404
  end

  describe "a table with a map column" do
    @attributed "/v1/datasets/analytics/tables/attributed/load"

    setup %{name: name} do
      {:ok, runtime} = Runtime.fetch(name)

      schema =
        Schema.new!([{"id", :int64, nullable: false}, {"attrs", {:map, :string, :string}}])

      :ok = Catalog.create_table(runtime.catalog, {"analytics", "attributed"}, schema)
      :ok
    end

    test "refuses a parquet load with 400 instead of crashing the reader", %{name: name} do
      body =
        [{"id", Series.from_list([1], dtype: {:s, 64})}]
        |> DataFrame.new()
        |> DataFrame.dump_parquet!()

      response = load(name, body, "application/vnd.apache.parquet", @attributed)

      assert response.status == 400
      assert JSON.decode!(response.resp_body)["error"]["message"] =~ "parquet load cannot carry"
    end

    test "loads a csv that leaves the map column out", %{name: name} do
      response = load(name, "id\n1\n2\n", "text/csv", @attributed)

      assert JSON.decode!(response.resp_body)["insertedRows"] == 2
    end
  end
end
