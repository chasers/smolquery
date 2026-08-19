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

  defp post(name, path, body) do
    conn(:post, path, JSON.encode!(body))
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> authed()
    |> HotServer.call(name)
  end

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

  describe "scoped manifest (T-316)" do
    test "answers only the ids asked for, whatever else the node holds", context do
      name = start_buffer_service(context)
      {:ok, wanted} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _rest} = Client.write_batch(name, @table, batch(2..2))
      {:ok, _more} = Client.write_batch(name, @table, batch(3..3))

      response = post(name, @manifest_path, %{"ids" => [wanted.segment_id]})

      assert response.status == 200
      assert [entry] = JSON.decode!(response.resp_body)
      assert entry["id"] == wanted.segment_id
      assert entry["url"] =~ segment_path(wanted.segment_id)
    end

    test "carries the stats a pruning caller asks for", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      [entry] =
        post(name, @manifest_path, %{"ids" => [ack.segment_id]}).resp_body |> JSON.decode!()

      assert entry["stats"]["id"]["min"] == 1
      assert entry["stats"]["id"]["max"] == 3
    end

    test "leaves the stats out for a caller that does not prune", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      body = %{"ids" => [ack.segment_id], "stats" => false}
      [entry] = post(name, @manifest_path, body).resp_body |> JSON.decode!()

      refute Map.has_key?(entry, "stats")
      assert entry["row_count"] == 3
      assert entry["claim_keys"] == []
      assert entry["url"] =~ segment_path(ack.segment_id)
    end

    test "an id the node no longer holds is absent, not an error", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      body = %{"ids" => [ack.segment_id, "01KYWPEEGAM8FQVQS5S2QF26SV"]}
      response = post(name, @manifest_path, body)

      assert response.status == 200
      assert [entry] = JSON.decode!(response.resp_body)
      assert entry["id"] == ack.segment_id
    end

    test "an empty id list is an empty manifest", context do
      name = start_buffer_service(context)
      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      response = post(name, @manifest_path, %{"ids" => []})

      assert response.status == 200
      assert JSON.decode!(response.resp_body) == []
    end

    test "400s a body that names no ids, rather than reading the whole backlog", context do
      name = start_buffer_service(context)
      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      assert post(name, @manifest_path, %{}).status == 400
      assert post(name, @manifest_path, %{"ids" => "01KYWPEEGAM8FQVQS5S2QF26SV"}).status == 400
      assert post(name, @manifest_path, %{"ids" => [1, 2]}).status == 400
    end

    test "401s without the internal secret", context do
      name = start_buffer_service(context)

      response =
        conn(:post, @manifest_path, JSON.encode!(%{"ids" => []}))
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> HotServer.call(name)

      assert response.status == 401
    end

    test "503s when no buffer service runs under that name" do
      name = :"not_a_buffer_#{:erlang.unique_integer([:positive])}"

      assert post(name, @manifest_path, %{"ids" => []}).status == 503
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

  describe "metrics (T-315)" do
    setup do
      handler = "hot-server-test-#{:erlang.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:smolquery, :hot_server, :request],
        fn _event, measurements, meta, _config ->
          send(test, {:hot_server_request, measurements, meta})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    test "prices a whole manifest read by its bytes and its entries", context do
      name = start_buffer_service(context)
      {:ok, _first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _second} = Client.write_batch(name, @table, batch(2..2))

      response = get(name, @manifest_path)

      assert_receive {:hot_server_request, measurements, meta}
      assert meta == %{route: :manifest, method: "GET", status: 200}
      assert measurements.entries == 2
      assert measurements.response_bytes == byte_size(response.resp_body)
      assert measurements.duration_us >= 0
    end

    test "prices a scoped read against the whole one it replaces", context do
      name = start_buffer_service(context)
      {:ok, wanted} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _rest} = Client.write_batch(name, @table, batch(2..2))

      get(name, @manifest_path)
      assert_receive {:hot_server_request, whole, %{route: :manifest}}

      post(name, @manifest_path, %{"ids" => [wanted.segment_id], "stats" => false})
      assert_receive {:hot_server_request, scoped, %{route: :manifest_scoped, status: 200}}

      assert scoped.entries == 1
      assert whole.entries == 2
      assert scoped.response_bytes < whole.response_bytes
    end

    test "counts a segment read's bytes, and a HEAD's as none", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))
      {:ok, runtime} = Runtime.fetch(name)
      {:ok, entry} = HotManifest.entry(runtime.manifest, @table, ack.segment_id)
      size = File.stat!(Store.location(runtime.store, entry.key)).size

      get(name, segment_path(ack.segment_id))
      assert_receive {:hot_server_request, whole, %{route: :segment, method: "GET", status: 200}}
      assert whole.response_bytes == size

      HotServer.call(authed(conn(:head, segment_path(ack.segment_id))), name)
      assert_receive {:hot_server_request, head, %{route: :segment, method: "HEAD"}}
      assert head.response_bytes == 0
    end

    test "a HEAD on the manifest builds every entry and sends none of them", context do
      name = start_buffer_service(context)
      {:ok, _first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _second} = Client.write_batch(name, @table, batch(2..2))

      HotServer.call(authed(conn(:head, @manifest_path)), name)

      assert_receive {:hot_server_request, measurements, meta}
      assert meta == %{route: :manifest, method: "HEAD", status: 200}
      assert measurements.entries == 2
      assert measurements.response_bytes == 0
    end

    test "names the method a scoped read arrives on", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      post(name, @manifest_path, %{"ids" => [ack.segment_id]})

      assert_receive {:hot_server_request, _measurements,
                      %{route: :manifest_scoped, method: "POST", status: 200}}
    end

    test "labels a range read 206, so an input's round trips are countable", context do
      name = start_buffer_service(context)
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      conn(:get, segment_path(ack.segment_id))
      |> Plug.Conn.put_req_header("range", "bytes=0-3")
      |> authed()
      |> HotServer.call(name)

      assert_receive {:hot_server_request, measurements,
                      %{route: :segment, method: "GET", status: 206}}

      assert measurements.response_bytes == 4
    end

    test "counts a request it never routed as unknown", context do
      name = start_buffer_service(context)

      HotServer.call(conn(:get, @manifest_path), name)

      assert_receive {:hot_server_request, _measurements,
                      %{route: :unknown, method: "GET", status: 401}}
    end
  end

  describe "a malformed scoped read over a real socket (T-316 review)" do
    @describetag :integration

    setup context do
      name = start_buffer_service(context)
      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      %{name: name, port: bandit_port(name)}
    end

    defp bandit_port(name) do
      {:ok, {_address, port}} =
        name |> HotServer.listener() |> ThousandIsland.listener_info()

      port
    end

    defp request(socket, method, body) do
      :gen_tcp.send(socket, [
        "#{method} #{@manifest_path} HTTP/1.1\r\n",
        "host: 127.0.0.1\r\n",
        "content-type: application/json\r\n",
        "#{Smolquery.InternalSecret.header()}: #{Smolquery.InternalSecret.value()}\r\n",
        "content-length: #{byte_size(body)}\r\n\r\n"
      ])

      Process.sleep(120)
      :gen_tcp.send(socket, body)
    end

    test "answers 400 and leaves the connection usable", %{port: port} do
      {:ok, socket} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

      request(socket, "POST", JSON.encode!(%{"ids" => [1, 2, 3]}))

      assert {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
      assert response =~ "400 Bad Request"

      # The bug this pins: the 400 path returned a pre-read conn, so Bandit tried
      # to drain a body already off the wire and held the connection until its
      # read timeout. A second request on the same socket is the proof it did not.
      request(socket, "POST", JSON.encode!(%{"ids" => []}))

      assert {:ok, second} = :gen_tcp.recv(socket, 0, 5_000)
      assert second =~ "200 OK"

      :gen_tcp.close(socket)
    end
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
