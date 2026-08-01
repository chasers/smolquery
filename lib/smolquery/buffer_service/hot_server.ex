defmodule Smolquery.BufferService.HotServer do
  @moduledoc """
  Serves a buffer node's hot tier to DuckDB over HTTP.

  Two routes, both read-only and unauthenticated in v1 (see
  `Smolquery.BufferService.Runtime` for why, and Milestone 6 for when that
  changes). Both answer `HEAD` as well as `GET` — `httpfs` sends a `HEAD`
  first, to learn a segment's size before it starts issuing ranged reads
  against it:

      GET /v1/datasets/:dataset/tables/:table/manifest
      GET /v1/datasets/:dataset/tables/:table/segments/:id.parquet

  A segment id from a request is validated with `Smolquery.Segments.Id.valid?/1`
  and resolved *through the manifest* — never by joining request input into a
  path — so there is no traversal surface even before auth exists.

  ## Where a segment's URL comes from

  The manifest response is what makes the hot tier queryable off-node: each
  entry carries a `"url"`, built from the *request's own* scheme, host, and
  port rather than from static configuration. That is what lets the same
  listener serve correct links whether it bound its configured port or an
  OS-assigned one (`0`, what tests use to run many instances side by side), and
  whether it sits behind a reverse proxy or not — whoever reached the manifest
  used the right address to do it, and the segments live beside it.

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

  @impl Plug
  def init(name), do: name

  @impl Plug
  def call(conn, name) do
    case {conn.method, conn.path_info} do
      {method, ["v1", "datasets", dataset, "tables", table, "manifest"]}
      when method in ["GET", "HEAD"] ->
        manifest(conn, name, {dataset, table})

      {method, ["v1", "datasets", dataset, "tables", table, "segments", filename]}
      when method in ["GET", "HEAD"] ->
        segment(conn, name, {dataset, table}, filename)

      _unmatched ->
        send_resp(conn, 404, "not found")
    end
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

  defp manifest(conn, name, table_ref) do
    case Runtime.fetch(name) do
      {:ok, runtime} ->
        entries = HotManifest.entries(runtime.manifest, table_ref)
        body = entries |> Enum.map(&entry_json(&1, conn, runtime, table_ref)) |> JSON.encode!()

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)

      :error ->
        send_resp(conn, 503, "buffer service unavailable")
    end
  end

  defp segment(conn, name, table_ref, filename) do
    with {:ok, id} <- segment_id(filename),
         {:ok, runtime} <- Runtime.fetch(name),
         {:ok, entry} <- HotManifest.entry(runtime.manifest, table_ref, id) do
      conn
      |> put_resp_header("cache-control", "public, max-age=31536000, immutable")
      |> send_file(200, Store.location(runtime.store, entry.key))
    else
      _not_found -> send_resp(conn, 404, "not found")
    end
  end

  defp segment_id(filename) do
    case Path.extname(filename) do
      ".parquet" ->
        id = Path.rootname(filename)
        if Id.valid?(id), do: {:ok, id}, else: :error

      _other ->
        :error
    end
  end

  defp entry_json(%Entry{} = entry, conn, runtime, table_ref) do
    entry
    |> Entry.to_record()
    |> Map.drop(["op", "key"])
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
