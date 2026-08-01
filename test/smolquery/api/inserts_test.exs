defmodule Smolquery.Api.InsertsTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Api.Router
  alias Smolquery.Api.Runtime
  alias Smolquery.BufferService
  alias Smolquery.BufferService.Load
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.MapCatalog

  @moduletag :tmp_dir

  @key "inserts-test-key"
  @path "/v1/datasets/analytics/tables/events/insert"

  setup context do
    buffer = :"api_insert_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        {"analytics", "events"},
        Schema.new!([{"id", :int64, nullable: false}])
      )

    ingest = :"api_insert_ingest_#{:erlang.unique_integer([:positive])}"

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

  defp post_rows(name, rows) do
    conn(:post, @path, JSON.encode!(%{"rows" => rows}))
    |> put_req_header("content-type", "application/json")
    |> put_req_header("authorization", "Bearer #{@key}")
    |> Router.call(Router.init(name))
  end

  test "acked rows answer 200 with no insertErrors", %{name: name, buffer: buffer} do
    response = post_rows(name, [%{"id" => 1}, %{"id" => 2}])

    assert response.status == 200

    assert JSON.decode!(response.resp_body) == %{
             "insertedRows" => 2,
             "insertErrors" => []
           }

    {:ok, entries} = BufferService.Client.hot_manifest(buffer, {"analytics", "events"})
    assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
  end

  test "partial failure is a 200 that names the rejected rows", %{name: name} do
    response = post_rows(name, [%{"id" => 1}, %{"id" => "junk"}])

    assert response.status == 200

    assert %{
             "insertedRows" => 1,
             "insertErrors" => [%{"index" => 1, "errors" => [%{"message" => message}]}]
           } = JSON.decode!(response.resp_body)

    assert message =~ "INT64"
  end

  test "an unknown table is a 404", %{name: name} do
    response =
      conn(:post, "/v1/datasets/analytics/tables/nope/insert", JSON.encode!(%{"rows" => [%{}]}))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> Router.call(Router.init(name))

    assert response.status == 404
  end

  test "a body without rows is a 400", %{name: name} do
    conn =
      conn(:post, @path, JSON.encode!(%{"data" => []}))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{@key}")

    response = Router.call(conn, Router.init(name))

    assert response.status == 400

    assert %{"error" => %{"message" => message}} = JSON.decode!(response.resp_body)
    assert message =~ "rows"
  end

  test "an overloaded buffer is a 429 whose retry-after is the prediction", %{
    name: name,
    buffer: buffer
  } do
    rows = for i <- 1..1_000, do: %{"id" => i}
    assert post_rows(name, rows).status == 200

    [{_pid, load}] =
      Registry.lookup(BufferService.Runtime.registry(buffer), {"analytics", "events"})

    for _crush <- 1..80,
        do: Load.sample_rate(load, 1_000, 1_000_000_000)

    response = post_rows(name, for(i <- 1..10, do: %{"id" => i}))

    assert response.status == 429

    assert %{"error" => %{"status" => "RESOURCE_EXHAUSTED", "message" => message}} =
             JSON.decode!(response.resp_body)

    assert message =~ "overloaded"
    assert [retry_after] = Plug.Conn.get_resp_header(response, "retry-after")
    assert String.to_integer(retry_after) >= 1
  end

  test "a buffer that is not running is a 503", %{name: name} do
    {:ok, runtime} = Runtime.fetch(name)
    {:ok, ingest_runtime} = IngestService.Runtime.fetch(runtime.ingest_name)
    IngestService.Runtime.put(%{ingest_runtime | buffer_name: :never_started_buffer})

    response = post_rows(name, [%{"id" => 1}])

    assert response.status == 503
    assert %{"error" => %{"status" => "UNAVAILABLE"}} = JSON.decode!(response.resp_body)
  end

  describe "insertId (T-41)" do
    defp post_body(name, body) do
      conn(:post, @path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> Router.call(Router.init(name))
    end

    defp hot_rows(buffer) do
      {:ok, entries} =
        Smolquery.BufferService.Client.hot_manifest(buffer, {"analytics", "events"})

      Enum.sum_by(entries, & &1.row_count)
    end

    test "a retried request with the same insertId cannot double-count", %{
      name: name,
      buffer: buffer
    } do
      body = %{"rows" => [%{"id" => 1}, %{"id" => 2}], "insertId" => "req-1"}

      assert post_body(name, body).status == 200
      retried = post_body(name, body)

      assert retried.status == 200
      assert JSON.decode!(retried.resp_body)["insertedRows"] == 2
      assert hot_rows(buffer) == 2
    end

    test "requests with different insertIds write separately", %{name: name, buffer: buffer} do
      assert post_body(name, %{"rows" => [%{"id" => 1}], "insertId" => "req-a"}).status == 200
      assert post_body(name, %{"rows" => [%{"id" => 2}], "insertId" => "req-b"}).status == 200
      assert hot_rows(buffer) == 2
    end

    test "a malformed insertId is a 400", %{name: name} do
      assert post_body(name, %{"rows" => [%{"id" => 1}], "insertId" => 42}).status == 400
      assert post_body(name, %{"rows" => [%{"id" => 1}], "insertId" => ""}).status == 400

      oversized = String.duplicate("x", 129)
      assert post_body(name, %{"rows" => [%{"id" => 1}], "insertId" => oversized}).status == 400
    end
  end
end
