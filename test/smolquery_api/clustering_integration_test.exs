defmodule SmolqueryApi.ClusteringIntegrationTest do
  @moduledoc """
  End-to-end proof that a clustering key reaches the write path: `PATCH` the
  key, insert rows out of order, and the flushed micro-segment is sorted.

  The pieces are unit-tested apart — the controller persists the key, the
  catalog reads it back, the writer sorts on it — but only the whole chain
  proves the ingest schema cache hands the *fresh* key to the buffer, which is
  the link a `PATCH` has to invalidate to be worth anything.

  `flush_max_rows` matches the batch size on purpose: an insert acks only once
  its rows are durable, so a batch that does not fill the accumulator would
  wait out `flush_interval_ms` before the request returns.
  """

  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @moduletag :tmp_dir

  @key "clustering-integration-key"
  @table {"analytics", "events"}
  @table_path "/v1/datasets/analytics/tables/events"
  @batch_rows 4
  @schema_json [
    %{"name" => "project_id", "type" => "INT64", "nullable" => false},
    %{"name" => "id", "type" => "INT64", "nullable" => true}
  ]

  setup context do
    buffer = :"clustering_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_max_rows: @batch_rows,
       flush_interval_ms: 60_000},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"project_id", :int64, nullable: false}, {"id", :int64}])
      )

    ingest = :"clustering_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"clustering_api_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, api_key: @key, catalog: catalog, ingest_name: ingest))
    on_exit(fn -> Runtime.delete(name) end)

    api_request(name, :post, "/v1/datasets/analytics/tables", %{
      "id" => "events",
      "schema" => @schema_json
    })

    %{name: name, buffer: buffer}
  end

  defp api_request(name, method, path, body) do
    method
    |> conn(path, JSON.encode!(body))
    |> put_req_header("content-type", "application/json")
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp patch_clustering(name, key) do
    response = api_request(name, :patch, @table_path, %{"clustering" => key})

    assert response.status == 200, response.resp_body
    assert JSON.decode!(response.resp_body)["clustering"] == key
  end

  # Table CRUD stays application/json; /insert takes ndjson only (T-190).
  defp insert(name, rows) do
    response =
      :post
      |> conn("#{@table_path}/insert", Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")
      |> put_req_header("content-type", "application/x-ndjson")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> then(&ApiEndpoint.request(name, &1))

    assert response.status == 200, response.resp_body

    assert JSON.decode!(response.resp_body) == %{
             "insertedRows" => @batch_rows,
             "insertErrors" => []
           }
  end

  defp flushed(buffer) do
    :ok = BufferService.Client.flush(buffer, @table)

    {:ok, [entry]} = BufferService.Client.hot_manifest(buffer, @table)
    {:ok, runtime} = BufferService.Runtime.fetch(buffer)

    frame = DataFrame.from_parquet!(Store.location(runtime.store, entry.key))

    {Series.to_list(frame["project_id"]), Series.to_list(frame["id"])}
  end

  @unsorted [
    %{"project_id" => 2, "id" => 1},
    %{"project_id" => 1, "id" => 3},
    %{"project_id" => 1, "id" => nil},
    %{"project_id" => 2, "id" => 2}
  ]

  test "PATCH clustering then insert unsorted rows yields a sorted micro-segment", %{
    name: name,
    buffer: buffer
  } do
    patch_clustering(name, ["project_id", "id"])
    insert(name, @unsorted)

    assert flushed(buffer) == {[1, 1, 2, 2], [3, nil, 1, 2]}
  end

  test "clearing the key returns the flush path to arrival order", %{
    name: name,
    buffer: buffer
  } do
    patch_clustering(name, ["project_id", "id"])
    patch_clustering(name, [])
    insert(name, @unsorted)

    assert flushed(buffer) == {[2, 1, 1, 2], [1, 3, nil, 2]}
  end
end
