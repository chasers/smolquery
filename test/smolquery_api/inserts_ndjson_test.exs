defmodule SmolqueryApi.InsertControllerNdjsonTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime, as: ApiRuntime

  @moduletag :tmp_dir

  @key "inserts-ndjson-test-key"
  @path "/v1/datasets/analytics/tables/events/insert"
  @table {"analytics", "events"}

  setup context do
    previous = Application.get_env(:smolquery, :data_dir)
    data_dir = Path.join(context.tmp_dir, "data")
    File.mkdir_p!(Path.join(data_dir, "tmp"))
    Application.put_env(:smolquery, :data_dir, data_dir)

    on_exit(fn ->
      if previous do
        Application.put_env(:smolquery, :data_dir, previous)
      else
        Application.delete_env(:smolquery, :data_dir)
      end
    end)

    buffer = :"api_ndjson_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_max_rows: 1,
       flush_writer: :duckdb},
      id: buffer
    )

    on_exit(fn -> Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
      )

    ingest = :"api_ndjson_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"api_ndjson_#{:erlang.unique_integer([:positive])}"
    runtime = ApiRuntime.new(name: name, api_key: @key, catalog: catalog, ingest_name: ingest)
    ApiRuntime.put(runtime)
    on_exit(fn -> ApiRuntime.delete(name) end)

    %{name: name, buffer: buffer, data_dir: data_dir}
  end

  defp post_ndjson(name, body) do
    conn(:post, @path, body)
    |> put_req_header("content-type", "application/x-ndjson")
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp segment_frame(buffer) do
    {:ok, [entry]} = BufferService.Client.hot_manifest(buffer, @table)
    {:ok, runtime} = Runtime.fetch(buffer)
    DataFrame.from_parquet!(Store.location(runtime.store, entry.key))
  end

  test "ndjson insert answers 200 with insertedRows and empty insertErrors", %{
    name: name,
    buffer: buffer
  } do
    body = ~s({"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n)
    response = post_ndjson(name, body)

    assert response.status == 200

    assert JSON.decode!(response.resp_body) == %{
             "insertedRows" => 2,
             "insertErrors" => []
           }

    frame = segment_frame(buffer)
    assert DataFrame.to_columns(frame)["id"] == [1, 2]
    assert DataFrame.to_columns(frame)["name"] == ["a", "b"]
  end

  test "a body whose last line has no trailing newline still reports two rows", %{name: name} do
    body = ~s({"id": 1}\n{"id": 2})
    response = post_ndjson(name, body)

    assert response.status == 200
    assert JSON.decode!(response.resp_body)["insertedRows"] == 2
  end

  # The newline count admits the body; what is reported back is the segment's
  # own Parquet footer count, so a line DuckDB skipped is not acked as durable.
  test "a blank line is not counted as an inserted row", %{name: name, buffer: buffer} do
    body = ~s({"id": 1, "name": "a"}\n\n{"id": 2, "name": "b"}\n)
    response = post_ndjson(name, body)

    assert response.status == 200
    assert JSON.decode!(response.resp_body)["insertedRows"] == 2

    frame = segment_frame(buffer)
    assert DataFrame.n_rows(frame) == 2
  end

  test "the spooled file is gone after a successful flush", %{
    name: name,
    data_dir: data_dir
  } do
    body = ~s({"id": 1, "name": "a"}\n)
    assert post_ndjson(name, body).status == 200

    tmp = Path.join(data_dir, "tmp")
    leftovers = File.ls!(tmp) |> Enum.filter(&String.starts_with?(&1, "insert-"))
    assert leftovers == []
  end
end

defmodule SmolqueryApi.InsertControllerPolarsUnaffectedTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime, as: ApiRuntime

  @moduletag :tmp_dir

  @key "inserts-polars-key"
  @path "/v1/datasets/analytics/tables/events/insert"

  setup context do
    buffer = :"api_polars_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        {"analytics", "events"},
        Schema.new!([{"id", :int64, nullable: false}])
      )

    ingest = :"api_polars_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"api_polars_#{:erlang.unique_integer([:positive])}"
    runtime = ApiRuntime.new(name: name, api_key: @key, catalog: catalog, ingest_name: ingest)
    ApiRuntime.put(runtime)
    on_exit(fn -> ApiRuntime.delete(name) end)

    %{name: name, buffer: buffer}
  end

  test "the default flush_writer starts no write-pool Engine children", %{buffer: buffer} do
    {:ok, runtime} = Runtime.fetch(buffer)

    assert runtime.flush_writer == :polars
    assert Runtime.engines(runtime) == [Runtime.engine(buffer, 0)]
    refute Process.whereis(Runtime.engine(buffer, 0))
    refute Process.whereis(Engine.supervisor_name(Runtime.engine(buffer, 0)))
  end

  test "a JSON insert still validates and returns per-index insertErrors", %{name: name} do
    conn =
      conn(:post, @path, JSON.encode!(%{"rows" => [%{"id" => 1}, %{"id" => "junk"}]}))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{@key}")

    response = ApiEndpoint.request(name, conn)

    assert response.status == 200

    assert %{
             "insertedRows" => 1,
             "insertErrors" => [%{"index" => 1, "errors" => [%{"message" => message}]}]
           } = JSON.decode!(response.resp_body)

    assert message =~ "INT64"
  end
end
