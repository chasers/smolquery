defmodule Smolquery.BufferService.HotServer do
  @moduledoc """
  Serves a buffer node's hot tier to DuckDB over HTTP.

  Three routes, all read-only, all requiring the internal secret
  (`Smolquery.InternalSecret` — PL-8 D6 closed PL-3 D12's unauthenticated
  window). The two `GET`s answer `HEAD` as well — `httpfs` sends a `HEAD`
  first, to learn a segment's size before it starts issuing ranged reads
  against it:

      GET  /v1/datasets/:dataset/tables/:table/manifest
      POST /v1/datasets/:dataset/tables/:table/manifest
      GET  /v1/datasets/:dataset/tables/:table/segments/:id.parquet

  ## The scoped manifest read, and why it is a POST

  The `GET` answers with every micro-segment the node holds for the table. Its
  cost grows with the unsealed backlog at every step: a full scan of the table's
  ETS entries, a sort, a record per entry, and one `JSON.encode!` over the lot
  held in memory. The query planner wants exactly that, because it prunes on
  each entry's flush-time bounds.

  The sealer does not. It holds a claim — at most 1,024 ids — and threw the rest
  away after paying for it, on the same pod that is committing, while the
  backlog it is trying to drain makes each attempt more expensive than the last
  (T-316). The `POST` takes the ids it actually wants:

      {"ids": ["01J...", "01J..."], "stats": false}

  It is a `POST` because the ids do not fit in a URL: 1,024 ULIDs are about
  28 KB of request line, and a caller that has to chunk its own read is a caller
  that will read the backlog twice again. It is still a read — nothing about the
  node's state changes — and `stats: false` skips building the bounds a
  non-pruning caller does not use, which is most of an entry's bytes.

  ## Metrics

  Every request emits `[:smolquery, :hot_server, :request]` with its duration,
  the bytes it produced, and the entries a manifest read answered with, labelled
  by route and method (T-315). Bytes are the series that matters: the routes
  differ by orders of magnitude, so a request count alone hides which one is
  spending the node.

  A `HEAD` counts zero bytes. `httpfs` sends one before every segment read, and
  counting the size it asked about would double every segment. The method label
  is what keeps that honest rather than confusing: a `HEAD` still pays for the
  work — a `HEAD` on the manifest route builds the whole document and then
  discards it — so its duration and its entry count are real while its bytes are
  zero. Reading the two together says how much of a route's cost never reaches
  the wire.

  The segment route honors single-part `Range` requests with a `206` — that is
  the whole reason httpfs is worth serving: DuckDB reads a Parquet footer and
  then individual column chunks, not whole files. A range it cannot satisfy is
  a `416`; a `Range` header it does not understand (multipart, malformed) is
  ignored and answered with the full `200`, as RFC 9110 allows.

  A segment id from a request is validated with `Smolquery.Segments.Id.valid?/1`
  and resolved *through the manifest* — never by joining request input into a
  path — so there is no traversal surface even before auth exists. A segment the
  grace-period sweep deleted between the manifest lookup and the read is a `404`,
  not a crash: to a reader holding a stale manifest, deleted and never-there are
  the same answer.

  ## Where a segment's URL comes from

  The manifest response is what makes the hot tier queryable off-node: each
  entry carries a `"url"`, built from the *request's own* scheme, host, and
  port rather than from static configuration. That is what lets the same
  listener serve correct links whether it bound its configured port or an
  OS-assigned one (`0`, what tests use to run many instances side by side), and
  whether it sits behind a reverse proxy or not — whoever reached the manifest
  used the right address to do it, and the segments live beside it. A proxy
  that terminates TLS or rewrites the port says so in `x-forwarded-proto`,
  `x-forwarded-host`, and `x-forwarded-port`, which are honored before any URL
  is built.

  An entry backed by a shared store (`Smolquery.Segments.Store.shared?/1`)
  reports that store's own location instead of a URL through this route —
  a future object-store hot tier's segments never round-trip through here at
  all, which is the whole reason `location/2` and `shared?/1` are separate
  answers.
  """

  @behaviour Plug

  import Plug.Conn

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store

  @rewrite_on Plug.RewriteOn.init([:x_forwarded_proto, :x_forwarded_host, :x_forwarded_port])

  @impl Plug
  def init(name), do: name

  @impl Plug
  def call(conn, name) do
    started_at = System.monotonic_time()

    try do
      if Smolquery.InternalSecret.proven?(conn) do
        conn |> Plug.RewriteOn.call(@rewrite_on) |> route(name)
      else
        respond(conn, 401, "missing or invalid internal secret")
      end
    catch
      kind, reason ->
        measure(%{conn | status: 500}, started_at)

        :erlang.raise(kind, reason, __STACKTRACE__)
    else
      answered -> measure(answered, started_at)
    end
  end

  defp route(conn, name) do
    case {conn.method, conn.path_info} do
      {method, ["v1", "datasets", dataset, "tables", table, "manifest"]}
      when method in ["GET", "HEAD"] ->
        conn
        |> put_private(:hot_server_route, :manifest)
        |> manifest(name, {dataset, table}, :all, stats: true)

      {"POST", ["v1", "datasets", dataset, "tables", table, "manifest"]} ->
        conn
        |> put_private(:hot_server_route, :manifest_scoped)
        |> scoped_manifest(name, {dataset, table})

      {method, ["v1", "datasets", dataset, "tables", table, "segments", filename]}
      when method in ["GET", "HEAD"] ->
        conn
        |> put_private(:hot_server_route, :segment)
        |> segment(name, {dataset, table}, filename)

      _unmatched ->
        respond(conn, 404, "not found")
    end
  end

  defp measure(conn, started_at) do
    duration_us =
      System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{
        duration_us: duration_us,
        response_bytes: conn.private[:hot_server_bytes] || 0,
        entries: conn.private[:hot_server_entries] || 0
      },
      %{
        route: conn.private[:hot_server_route] || :unknown,
        method: conn.method,
        status: conn.status
      }
    )

    conn
  end

  @doc """
  The pid of a running instance's HTTP listener.

  Not needed to serve requests — only for a test discovering the port Bandit
  bound when `hot_server_port` is `0`.
  """
  @spec listener(atom()) :: pid() | nil
  def listener(name) do
    case Process.whereis(Runtime.supervisor(name)) do
      nil -> nil
      supervisor -> find_listener(supervisor, Runtime.hot_server(name))
    end
  end

  @doc """
  The base URL a running instance's `HotServer` is listening on.
  """
  @spec base_url(atom()) :: String.t()
  def base_url(name) do
    {:ok, {_address, port}} = name |> listener() |> ThousandIsland.listener_info()

    "http://127.0.0.1:#{port}"
  end

  defp manifest(conn, name, table_ref, ids, opts) do
    case runtime(name) do
      {:ok, runtime} ->
        entries = HotManifest.entries(runtime.manifest, table_ref, ids)

        body =
          entries
          |> Enum.map(&entry_json(&1, conn, runtime, table_ref, opts))
          |> JSON.encode!()

        conn
        |> put_private(:hot_server_entries, length(entries))
        |> put_resp_content_type("application/json")
        |> respond(200, body)

      {:error, :unavailable} ->
        respond(conn, 503, "buffer service unavailable")
    end
  end

  defp scoped_manifest(conn, name, table_ref) do
    case read_scope(conn) do
      {:ok, conn, ids, opts} -> manifest(conn, name, table_ref, ids, opts)
      {:error, conn} -> respond(conn, 400, ~s(expected a JSON body with an "ids" array))
    end
  end

  # The body is read before the `with`, not inside it: `with` does not export a
  # clause binding into `else`, so an `else` returning `conn` would hand back the
  # pre-read connection. Bandit then tries to drain a body that is already off
  # the wire and blocks the connection process until its read timeout.
  defp read_scope(conn) do
    case read_body(conn) do
      {:ok, body, conn} -> decode_scope(conn, body)
      {:more, _partial, conn} -> {:error, conn}
      {:error, _reason} -> {:error, conn}
    end
  end

  defp decode_scope(conn, body) do
    with {:ok, %{"ids" => ids} = scope} when is_list(ids) <- JSON.decode(body),
         true <- Enum.all?(ids, &is_binary/1) do
      {:ok, conn, ids, stats: scope["stats"] != false}
    else
      _malformed -> {:error, conn}
    end
  end

  defp segment(conn, name, table_ref, filename) do
    with {:ok, id} <- segment_id(filename),
         {:ok, runtime} <- runtime(name),
         {:ok, entry} <- HotManifest.entry(runtime.manifest, table_ref, id) do
      serve_segment(conn, Store.location(runtime.store, entry.key))
    else
      {:error, :unavailable} -> respond(conn, 503, "buffer service unavailable")
      _not_found -> respond(conn, 404, "not found")
    end
  end

  defp respond(conn, status, body) do
    conn |> record_bytes(IO.iodata_length(body)) |> send_resp(status, body)
  end

  defp record_bytes(%Plug.Conn{method: "HEAD"} = conn, _bytes),
    do: put_private(conn, :hot_server_bytes, 0)

  defp record_bytes(conn, bytes), do: put_private(conn, :hot_server_bytes, bytes)

  defp runtime(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, :unavailable}
    end
  end

  defp serve_segment(conn, path) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{size: size, mtime: mtime}} -> serve_bytes(conn, path, size, mtime)
      {:error, _reason} -> respond(conn, 404, "not found")
    end
  end

  defp serve_bytes(conn, path, size, mtime) do
    conn =
      conn
      |> put_resp_header("cache-control", "public, max-age=31536000, immutable")
      |> put_resp_header("accept-ranges", "bytes")
      |> put_resp_header("last-modified", rfc7231(mtime))

    case requested_range(conn, size) do
      :whole ->
        conn
        |> record_bytes(size)
        |> send_file_or_gone(200, path, 0, :all)

      {:partial, offset, length} ->
        conn
        |> record_bytes(length)
        |> put_resp_header("content-range", "bytes #{offset}-#{offset + length - 1}/#{size}")
        |> send_file_or_gone(206, path, offset, length)

      :unsatisfiable ->
        conn
        |> put_resp_header("content-range", "bytes */#{size}")
        |> respond(416, "")
    end
  end

  defp send_file_or_gone(conn, status, path, offset, length) do
    send_file(conn, status, path, offset, length)
  rescue
    File.Error -> respond(conn, 404, "not found")
  end

  defp rfc7231(posix_seconds) do
    posix_seconds
    |> DateTime.from_unix!()
    |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")
  end

  defp requested_range(conn, size) do
    case get_req_header(conn, "range") do
      ["bytes=" <> spec] -> parse_range(spec, size)
      _absent_or_not_understood -> :whole
    end
  end

  defp parse_range(spec, size) do
    if String.contains?(spec, ",") do
      :whole
    else
      spec |> String.trim() |> String.split("-", parts: 2) |> bound(size)
    end
  end

  defp bound(["", suffix], size) do
    case Integer.parse(suffix) do
      {n, ""} when n > 0 and size > 0 -> {:partial, max(size - n, 0), min(n, size)}
      {n, ""} when n >= 0 -> :unsatisfiable
      _junk -> :whole
    end
  end

  defp bound([start, ""], size) do
    case Integer.parse(start) do
      {offset, ""} when offset < size -> {:partial, offset, size - offset}
      {_past_the_end, ""} -> :unsatisfiable
      _junk -> :whole
    end
  end

  defp bound([start, last], size) do
    with {offset, ""} <- Integer.parse(start),
         {last, ""} when last >= offset <- Integer.parse(last) do
      if offset < size do
        {:partial, offset, min(last, size - 1) - offset + 1}
      else
        :unsatisfiable
      end
    else
      _junk -> :whole
    end
  end

  defp bound(_junk, _size), do: :whole

  defp segment_id(filename) do
    case Path.extname(filename) do
      ".parquet" ->
        id = Path.rootname(filename)
        if Id.valid?(id), do: {:ok, id}, else: :error

      _other ->
        :error
    end
  end

  defp entry_json(%Entry{} = entry, conn, runtime, table_ref, opts) do
    entry
    |> Entry.to_manifest(opts)
    |> Map.put("url", url(entry, conn, runtime, table_ref))
  end

  defp url(%Entry{} = entry, conn, runtime, {dataset, table}) do
    if Store.shared?(runtime.store) do
      Store.location(runtime.store, entry.key)
    else
      "#{conn.scheme}://#{conn.host}:#{conn.port}" <>
        "/v1/datasets/#{dataset}/tables/#{table}/segments/#{entry.id}.parquet"
    end
  end

  defp find_listener(supervisor, id) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^id, pid, _type, _modules} -> pid
      _child -> nil
    end)
  end
end
