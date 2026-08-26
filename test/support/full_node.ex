defmodule Smolquery.Test.FullNode do
  @moduledoc """
  A production-wired single node for black-box integration tests.

  Stands up the three services exactly as a deployment does: a
  `BufferService` whose seal consumer is the real
  `Smolquery.StorageService.Client`, a full `StorageService.Supervisor`
  running the real `Handoff.Seal` against a real DuckLake catalog, and a
  `QueryService` reading through the planner. A test drives the edges —
  `BufferService.Client.write_batch/3` in, `QueryService.Client.query/3`
  out — and stages races only with snabbkaffe nemesis (`inject_crash`,
  `force_ordering`) over the production tracepoints, never by calling an
  internal step out of band.
  """

  import ExUnit.Callbacks, only: [start_supervised!: 2, on_exit: 1]

  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.StorageService
  alias Smolquery.StorageService.Runtime, as: StorageRuntime

  @table {"analytics", "events"}

  def table, do: @table

  def schema, do: Schema.new!([{"id", :int64}])

  @doc """
  Starts buffer + storage + query wired together. `buffer_opts` sets the
  valves and cadences the scenario needs; everything else is the production
  shape.
  """
  def start(context, buffer_opts) do
    unique = :erlang.unique_integer([:positive])
    buffer = :"node_buffer_#{unique}"
    storage = :"node_storage_#{unique}"
    query = :"node_query_#{unique}"

    start_buffer(context, buffer, storage, buffer_opts)

    hot_port = bound_hot_port(buffer)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "ducklake")

    start_supervised!(
      {StorageService.Supervisor,
       name: storage,
       dir: Path.join(context.tmp_dir, "sealed"),
       buffer_name: buffer,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       seal_backoff_base_ms: 0,
       catalog: [metadata: metadata, data_path: data_path]},
      id: storage
    )

    on_exit(fn -> StorageRuntime.delete(storage) end)

    {:ok, storage_runtime} = StorageRuntime.fetch(storage)
    catalog = storage_runtime.catalog
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_name: buffer,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    %{buffer: buffer, storage: storage, query: query, catalog: catalog, hot_port: hot_port}
  end

  @doc """
  Restarts the buffer under new options — the production way a claim frozen
  under old valves meets new ones (a deploy). Same directory, so recovery
  replays the manifest log, and the same hot-server port, so the base URL
  the storage and query services hold stays valid — in production the port
  is fixed configuration.
  """
  def restart_buffer(context, node, buffer_opts) do
    :ok = ExUnit.Callbacks.stop_supervised(node.buffer)

    start_buffer(
      context,
      node.buffer,
      node.storage,
      Keyword.put(buffer_opts, :hot_server_port, node.hot_port)
    )

    node
  end

  defp bound_hot_port(buffer) do
    %URI{port: port} = buffer |> HotServer.base_url() |> URI.parse()

    port
  end

  defp start_buffer(context, buffer, storage, buffer_opts) do
    opts =
      Keyword.merge(
        [
          name: buffer,
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25,
          maintenance_interval_ms: 50,
          seal_consumer: {StorageService.Client, [name: storage]}
        ],
        buffer_opts
      )

    start_supervised!({BufferService.Supervisor, opts}, id: buffer)
    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    :ok
  end

  @doc """
  The rows as a query sees them, through the planner and a real job engine.
  """
  def query_ids(node) do
    case QueryService.Client.query(node.query, "SELECT id FROM analytics.events ORDER BY id") do
      {:ok, _job, %Explorer.DataFrame{} = frame} ->
        frame |> Explorer.DataFrame.to_columns() |> Map.get("id", [])

      {:ok, job, nil} ->
        {:query_failed, job.error}
    end
  end

  @doc """
  The registered sealed segments at the current snapshot.
  """
  def sealed_count(node) do
    {:ok, sealed} = Catalog.segments(node.catalog, @table, :current)

    length(sealed)
  end
end
