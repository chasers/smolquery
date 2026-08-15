defmodule Smolquery.BufferService.HotServerTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.Eventually
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

  defp authed(conn) do
    Plug.Conn.put_req_header(
      conn,
      Smolquery.InternalSecret.header(),
      Smolquery.InternalSecret.value()
    )
  end

  defp get(name, path), do: HotServer.call(authed(conn(:get, path)), name)

  defp segment_path(id), do: "/v1/datasets/analytics/tables/events/segments/#{id}.parquet"

  defp parse_rfc7231(value) do
    case :httpd_util.convert_request_date(String.to_charlist(value)) do
      {{_year, _month, _day}, {_hour, _minute, _second}} = datetime -> {:ok, datetime}
      :bad_date -> :error
    end
  end

  describe "manifest" do
    test "lists entries with a url built from the request's own host and port", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      response =
        HotServer.call(authed(conn(:get, "http://buffer.internal:9999" <> @manifest_path)), name)

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

    test "exposes claim_keys, which is what a planner dedups on", context do
      name = start_buffer_service(context, seal_max_files: 1, seal_retry_ms: 60_000)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      assert Eventually.until(fn ->
               [entry] = get(name, @manifest_path).resp_body |> JSON.decode!()

               entry["claim_keys"] != []
             end)

      [entry] = get(name, @manifest_path).resp_body |> JSON.decode!()

      assert entry["id"] == ack.segment_id
      assert [key] = entry["claim_keys"]
      assert String.starts_with?(key, "analytics/events/")
      assert String.ends_with?(key, ".parquet")
    end

    test "reports an unclaimed entry with no claim keys", context do
      name = start_buffer_service(context)
      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      [entry] = get(name, @manifest_path).resp_body |> JSON.decode!()

      assert entry["claim_keys"] == []
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

    test "serves a parseable last-modified, so httpfs cache validation never overflows",
         context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..2))

      response = get(name, segment_path(ack.segment_id))

      assert [last_modified] =
               for({"last-modified", value} <- response.resp_headers, do: value)

      assert {:ok, _datetime} = parse_rfc7231(last_modified)
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

    test "404s a segment the sweep deleted after the manifest lookup", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      {:ok, runtime} = Runtime.fetch(name)
      [entry] = HotManifest.entries(runtime.manifest, @table)
      File.rm!(Store.location(runtime.store, entry.key))

      assert get(name, segment_path(ack.segment_id)).status == 404
    end

    test "503s when no buffer service runs under that name" do
      name = :"not_a_buffer_#{:erlang.unique_integer([:positive])}"

      assert get(name, segment_path("01ARZ3NDEKTSV4RRFFQ69G5FAV")).status == 503
    end

    test "answers HEAD without a body", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..2))

      response = HotServer.call(authed(conn(:head, segment_path(ack.segment_id))), name)

      assert response.status == 200
      assert {"accept-ranges", "bytes"} in response.resp_headers
      assert response.resp_body == ""
    end
  end

  describe "ranged reads" do
    defp get_range(name, path, range) do
      HotServer.call(
        conn(:get, path) |> Plug.Conn.put_req_header("range", range) |> authed(),
        name
      )
    end

    setup context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))
      whole = get(name, segment_path(ack.segment_id)).resp_body

      %{name: name, path: segment_path(ack.segment_id), whole: whole}
    end

    test "serves a first-last range as a 206 with content-range", %{
      name: name,
      path: path,
      whole: whole
    } do
      response = get_range(name, path, "bytes=0-3")

      assert response.status == 206
      assert response.resp_body == binary_part(whole, 0, 4)

      assert {"content-range", "bytes 0-3/#{byte_size(whole)}"} in response.resp_headers
    end

    test "serves the footer-first suffix range httpfs opens with", %{
      name: name,
      path: path,
      whole: whole
    } do
      size = byte_size(whole)
      response = get_range(name, path, "bytes=-8")

      assert response.status == 206
      assert response.resp_body == binary_part(whole, size - 8, 8)
      assert {"content-range", "bytes #{size - 8}-#{size - 1}/#{size}"} in response.resp_headers
    end

    test "clamps a last past the end to the file's size", %{
      name: name,
      path: path,
      whole: whole
    } do
      size = byte_size(whole)
      response = get_range(name, path, "bytes=4-99999999")

      assert response.status == 206
      assert response.resp_body == binary_part(whole, 4, size - 4)
    end

    test "an open-ended range reads to the end", %{name: name, path: path, whole: whole} do
      response = get_range(name, path, "bytes=8-")

      assert response.status == 206
      assert response.resp_body == binary_part(whole, 8, byte_size(whole) - 8)
    end

    test "416s a range past the end, naming the size", %{name: name, path: path, whole: whole} do
      response = get_range(name, path, "bytes=#{byte_size(whole)}-")

      assert response.status == 416
      assert {"content-range", "bytes */#{byte_size(whole)}"} in response.resp_headers
    end

    test "ignores a range it does not understand and serves the whole file", %{
      name: name,
      path: path,
      whole: whole
    } do
      for range <- ["bytes=0-1,4-5", "bytes=junk", "rows=0-1", "bytes=-x"] do
        response = get_range(name, path, range)

        assert response.status == 200
        assert response.resp_body == whole
      end
    end
  end

  describe "behind a reverse proxy" do
    test "builds urls from the forwarded proto, host, and port", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      response =
        conn(:get, "http://buffer.internal:4001" <> @manifest_path)
        |> Plug.Conn.put_req_header("x-forwarded-proto", "https")
        |> Plug.Conn.put_req_header("x-forwarded-host", "hot.example.com")
        |> Plug.Conn.put_req_header("x-forwarded-port", "443")
        |> authed()
        |> HotServer.call(name)

      assert [entry] = JSON.decode!(response.resp_body)
      assert entry["url"] == "https://hot.example.com:443" <> segment_path(ack.segment_id)
    end
  end

  test "401s a request without the internal secret, before routing", context do
    name = start_buffer_service(context)

    assert HotServer.call(conn(:get, @manifest_path), name).status == 401

    wrong =
      conn(:get, @manifest_path)
      |> Plug.Conn.put_req_header(Smolquery.InternalSecret.header(), "wrong")

    assert HotServer.call(wrong, name).status == 401
  end

  test "404s an unmatched route", context do
    name = start_buffer_service(context)

    assert get(name, "/").status == 404
  end

  describe "read-your-writes, end to end" do
    @describetag :integration

    test "a written, acked batch is queryable through httpfs immediately", context do
      engine_name = :"#{__MODULE__}.Engine#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {Engine,
         name: engine_name,
         extensions: [:httpfs],
         statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
      )

      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      manifest_url = HotServer.base_url(name) <> @manifest_path

      %{status: 200, body: [entry]} =
        Req.get!(manifest_url,
          headers: [{Smolquery.InternalSecret.header(), Smolquery.InternalSecret.value()}]
        )

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
