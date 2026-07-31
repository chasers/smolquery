defmodule Smolquery.BufferService.HotServerTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Test.MemoryStore

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @manifest_path "/v1/datasets/analytics/tables/events/manifest"

  defp schema, do: Schema.new!([{"id", :int64, nullable: false}])
  defp batch(range), do: %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}

  defp start_buffer_service(context, opts \\ []) do
    opts =
      Keyword.merge(
        [
          name: :"hot_server_#{:erlang.unique_integer([:positive])}",
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp get(name, path), do: HotServer.call(conn(:get, path), name)

  defp segment_path(id), do: "/v1/datasets/analytics/tables/events/segments/#{id}.parquet"

  describe "manifest" do
    test "lists entries with a url built from the request's own host and port", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      response = HotServer.call(conn(:get, "http://buffer.internal:9999" <> @manifest_path), name)

      assert response.status == 200
      assert [entry] = JSON.decode!(response.resp_body)
      assert entry["id"] == ack.segment_id
      assert entry["row_count"] == 3
      assert entry["url"] == "http://buffer.internal:9999" <> segment_path(ack.segment_id)
      refute Map.has_key?(entry, "op")
      refute Map.has_key?(entry, "key")
    end

    test "reports a shared store's own location instead of a url", context do
      name = start_buffer_service(context, store: MemoryStore.new())
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      [entry] = get(name, @manifest_path).resp_body |> JSON.decode!()

      assert entry["url"] == "memory://analytics/events/#{ack.segment_id}.parquet"
    end

    test "an unknown table's manifest is an empty list", context do
      name = start_buffer_service(context)

      response = get(name, "/v1/datasets/analytics/tables/absent/manifest")

      assert response.status == 200
      assert JSON.decode!(response.resp_body) == []
    end

    test "503s when no buffer service runs under that name" do
      response = get(:"not_a_buffer_#{:erlang.unique_integer([:positive])}", @manifest_path)

      assert response.status == 503
    end
  end

  describe "segment bytes" do
    test "serves a segment's bytes with an immutable cache header", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..2))

      response = get(name, segment_path(ack.segment_id))

      assert response.status == 200
      assert {"cache-control", "public, max-age=31536000, immutable"} in response.resp_headers
      assert byte_size(response.resp_body) > 0
    end

    test "404s an id the manifest does not hold", context do
      name = start_buffer_service(context)

      assert get(name, segment_path("01ARZ3NDEKTSV4RRFFQ69G5FAV")).status == 404
    end

    test "404s a non-parquet extension", context do
      name = start_buffer_service(context)

      assert get(name, "/v1/datasets/analytics/tables/events/segments/whatever.txt").status == 404
    end

    test "404s an id smuggling a path rather than resolving it through the manifest", context do
      name = start_buffer_service(context)

      path = "/v1/datasets/analytics/tables/events/segments/..%2F..%2Fmix.exs.parquet"

      assert get(name, path).status == 404
    end
  end

  test "404s an unmatched route", context do
    name = start_buffer_service(context)

    assert get(name, "/").status == 404
  end

  describe "read-your-writes, end to end" do
    @describetag :integration

    test "a written, acked batch is queryable through httpfs immediately", context do
      engine_name = :"#{__MODULE__}.Engine#{:erlang.unique_integer([:positive])}"
      start_supervised!({Engine, name: engine_name, extensions: [:httpfs]})

      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      manifest_url = HotServer.base_url(name) <> @manifest_path
      %{status: 200, body: [entry]} = Req.get!(manifest_url)

      assert entry["id"] == ack.segment_id

      result =
        Engine.query!(
          engine_name,
          "SELECT count(*) AS n, min(id) AS lo, max(id) AS hi FROM read_parquet($1)",
          [entry["url"]]
        )

      assert result.rows == [[3, 1, 3]]
    end
  end
end
