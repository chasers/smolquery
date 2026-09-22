defmodule Smolquery.Test.VictoriaMetricsStack do
  @moduledoc """
  A VictoriaMetrics edge over a real write and read path, for the query
  tests of PL-70: a DuckLake catalog, a buffer, an ingest service, a query
  service reading the buffer's hot tier over HTTP, and an edge runtime
  wired to them. Samples go in through the edge's own remote-write route,
  as uncompressed protobuf, so each `write/2` is one commit and one hot
  micro-segment.

  The table is created before the query service starts, with the schema
  and clustering `SmolqueryVictoriaMetrics.Write` gives it: a job engine
  attaching the catalog's SQLite file while the edge creates the table on
  its first write can find the file locked. Creating it on the first write
  is `SmolqueryVictoriaMetrics.WriteTest`'s.
  """

  import Bitwise
  import ExUnit.Callbacks, only: [start_supervised!: 2, on_exit: 1]
  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test, only: [conn: 3]

  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.IngestService
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias SmolqueryVictoriaMetrics.Router
  alias SmolqueryVictoriaMetrics.Runtime

  @password "victoriametrics-stack-password"
  @table {"metrics", "samples"}
  @schema Schema.new!([
            {"name", :string, nullable: false},
            {"series", :int64, nullable: false},
            {"labels", {:map, :string, :string}},
            {"ts", :timestamp, nullable: false},
            {"value", :float64, nullable: false}
          ])

  @doc """
  Starts the stack under the test's supervisor. `opts` are extra
  `SmolqueryVictoriaMetrics.Runtime` options, such as the query ceilings.
  """
  def start(context, opts \\ []) do
    unique = :erlang.unique_integer([:positive])
    dir = Path.join(context.tmp_dir, "vm_stack_#{unique}")
    lake = :"vm_stack_lake_#{unique}"
    metadata = "sqlite:#{Path.join(dir, "catalog.sqlite")}"
    data_path = Path.join(dir, "data")
    File.mkdir_p!(dir)

    start_supervised!({DuckLake, name: lake, metadata: metadata, data_path: data_path}, id: lake)
    catalog = DuckLake.new(engine: lake)
    :ok = Catalog.create_dataset(catalog, "metrics")
    :ok = Catalog.create_table(catalog, @table, @schema)
    :ok = Catalog.put_clustering(catalog, @table, ["name", "ts"])

    buffer = :"vm_stack_buffer_#{unique}"

    start_supervised!(
      {BufferService.Supervisor, name: buffer, dir: Path.join(dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    ingest = :"vm_stack_ingest_#{unique}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    query = :"vm_stack_query_#{unique}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_name: buffer,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    name = :"vm_stack_edge_#{unique}"

    runtime =
      Runtime.new(
        [
          name: name,
          password: @password,
          ingest_name: ingest,
          query_name: query,
          catalog: catalog
        ] ++ opts
      )

    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, runtime: runtime, buffer: buffer, query: query, catalog: catalog}
  end

  @doc """
  The password every request to the stack's edge presents.
  """
  def password, do: @password

  @doc """
  Writes `series`, each `{labels, samples}` with `__name__` among the
  labels and samples as `{timestamp_ms, value}`, in one remote write.
  """
  def write(stack, series) do
    body = Enum.map_join(series, &timeseries/1)

    response =
      conn(:post, "/api/v1/write", body)
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("authorization", "Bearer " <> @password)
      |> Router.call(stack.name)

    204 = response.status
    :ok
  end

  @doc """
  Sends a request to the stack's edge, with the password.
  """
  def request(stack, method, path, body \\ nil, headers \\ []) do
    headers
    |> Enum.reduce(conn(method, path, body), fn {key, value}, conn ->
      put_req_header(conn, key, value)
    end)
    |> put_req_header("authorization", "Bearer " <> @password)
    |> Router.call(stack.name)
  end

  defp timeseries({labels, samples}) do
    labels = Enum.map_join(Enum.sort(labels), fn {name, value} -> label(name, value) end)
    bytes(1, labels <> Enum.map_join(samples, fn {ts, value} -> sample(ts, value) end))
  end

  defp varint(n) when n < 0x80, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 0x7F::7, varint(n >>> 7)::binary>>

  defp bytes(field, value), do: varint(field <<< 3 ||| 2) <> varint(byte_size(value)) <> value
  defp label(name, value), do: bytes(1, bytes(1, name) <> bytes(2, value))

  defp sample(timestamp, value),
    do: bytes(2, varint(9) <> <<value * 1.0::float-little-64>> <> varint(16) <> varint(timestamp))
end
