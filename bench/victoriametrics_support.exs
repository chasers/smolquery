Code.require_file("support.exs", __DIR__)

defmodule Bench.VictoriaMetricsSupport do
  @moduledoc """
  What `bench/victoriametrics.exs` and `bench/victoriametrics_cardinality.exs`
  share: the private node, the read rows timed through the edge's HTTP
  routes, and the SQL controls submitted by hand through the query service.

  A private node is started beside the application's own: a DuckLake
  catalog, a buffer, an ingest service, a query service reading the
  buffer's hot tier and the lake, and the edge on a port the OS picks. No
  storage service, so nothing seals. The edge's series and sample ceilings
  are raised past anything measured here, so a wide filter is measured
  rather than refused, and its query deadline to ten minutes.

  A read row is one route run once to warm, then `reps` times; the table
  has the medians. `fetch` is the time in `SmolqueryVictoriaMetrics.Samples`
  reading raw samples by SQL, from the `fetch_us` of
  `[:smolquery, :victoriametrics, :query]`, and `sweep` is the rest of the
  query's own time: the rollup sweep and evaluation in the node. `rest` is
  the wall time past both: rendering the JSON answer and HTTP; the driver
  does not decode it. The label routes emit no query event, so they have a
  wall time only. A route that does not answer 200 prints its status and
  the error instead of a row, since a refusal at a ceiling is a finding.

  The SQL controls (T-583) are the fetch as
  `SmolqueryVictoriaMetrics.Samples.select_query/3` wrote it at T-583, copied
  here so the control stays the same shape as the edge's own query changes
  (an aggregate is pushed down from T-568 on), with its predicate swapped: the MAP lookup the edge writes, against the integer fingerprints
  of the same series (`series IN (...)`, the shape a series table would
  give T-569, and `series = $n` for one), and a `count(*)` of each to
  separate the filter from grouping and copying the samples out.
  """

  import Bench.Support, except: [table: 0, schema: 0]

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.IngestService
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.Schema

  @password "bench-victoriametrics"
  @lake :bench_vm_lake
  @query_name :bench_vm_query
  @step_s 15

  def password, do: @password
  def lake, do: @lake
  def query_name, do: @query_name
  def step_s, do: @step_s

  @doc """
  Starts the node in `dir` and answers the edge's base URL. `lake_opts` are
  extra `Smolquery.Engine` options for the lake's engine, and `extra_dirs`
  other directories the query service may read segments from.
  """
  def start_node!(dir, lake_opts \\ [], extra_dirs \\ [], edge_opts \\ []) do
    metadata = "sqlite:#{Path.join(dir, "catalog.sqlite")}"
    data_path = Path.join(dir, "data")

    {:ok, _pid} =
      DuckLake.start_link(
        [name: @lake, metadata: metadata, data_path: data_path, max_result_rows: :infinity] ++
          lake_opts
      )

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "metrics")
    :ok = Catalog.create_table(catalog, {"metrics", "samples"}, samples_schema())
    :ok = Catalog.put_clustering(catalog, {"metrics", "samples"}, ["name", "ts"])

    {:ok, _pid} =
      BufferService.Supervisor.start_link(
        name: :bench_vm_buffer,
        dir: Path.join(dir, "buffer"),
        catalog: [metadata: metadata, data_path: data_path],
        hot_server_port: 0
      )

    {:ok, _pid} =
      IngestService.Supervisor.start_link(
        name: :bench_vm_ingest,
        catalog: catalog,
        buffer_name: :bench_vm_buffer
      )

    {:ok, _pid} =
      QueryService.Supervisor.start_link(
        name: @query_name,
        catalog: catalog,
        buffer_name: :bench_vm_buffer,
        buffer_base_url: HotServer.base_url(:bench_vm_buffer),
        engine_extensions: [:httpfs],
        allowed_directories: [dir | extra_dirs],
        default_timeout_ms: 600_000,
        job_bootstrap: [
          DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
        ]
      )

    {:ok, _pid} =
      [
        name: :bench_vm_edge,
        password: @password,
        port: 0,
        ingest_name: :bench_vm_ingest,
        query_name: @query_name,
        catalog: catalog,
        max_series: 1_000_000,
        max_samples: 1_000_000_000,
        max_samples_per_query: 1_000_000_000,
        max_query_duration_ms: 600_000
      ]
      |> Keyword.merge(edge_opts)
      |> SmolqueryVictoriaMetrics.Supervisor.start_link()

    {:ok, {_ip, port}} = SmolqueryVictoriaMetrics.Supervisor.bound(:bench_vm_edge)
    "http://127.0.0.1:#{port}"
  end

  defp samples_schema do
    Schema.new!([
      {"name", :string, nullable: false},
      {"series", :int64, nullable: false},
      {"labels", {:map, :string, :string}},
      {"ts", :timestamp, nullable: false},
      {"value", :float64, nullable: false}
    ])
  end

  def read_header(width) do
    IO.puts(
      label("query", width) <>
        pad("series", 8) <>
        pad("samples", 10) <>
        pad("wall ms", 9) <> pad("fetch ms", 10) <> pad("sweep ms", 10) <> pad("rest ms", 9)
    )
  end

  @doc "A `query_range` request for `query` over `[start, stop]` at the step."
  def range(query, start, stop) do
    {"/api/v1/query_range",
     %{"query" => query, "start" => start, "end" => stop, "step" => "#{@step_s}s"}}
  end

  def read_row(req, name, path, params, reps, width) do
    handler = "bench-victoriametrics-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler, [:smolquery, :victoriametrics, :query], &handle_query/4, self())

    runs =
      Enum.reduce_while(0..reps//1, [], fn _rep, runs -> run_once(req, path, params, runs) end)

    :telemetry.detach(handler)

    case runs do
      {:error, status, body} ->
        IO.puts(label(name, width) <> "  #{status}: #{String.slice(body, 0, 160)}")

      [_warm | measured] ->
        median = fn key -> measured |> Enum.map(&Map.get(&1, key)) |> median() end

        IO.puts(
          label(name, width) <>
            pad(median.(:series), 8) <>
            pad(median.(:samples), 10) <>
            pad(ms(median.(:wall_us)), 9) <>
            pad(opt_ms(median.(:fetch_us)), 10) <>
            pad(opt_ms(median.(:sweep_us)), 10) <>
            pad(opt_ms(median.(:rest_us)), 9)
        )
    end
  end

  @doc false
  def handle_query(_event, measurements, _meta, parent), do: send(parent, {:query, measurements})

  defp run_once(req, path, params, runs) do
    case once(req, path, params) do
      {:error, _status, _body} = error -> {:halt, error}
      run -> {:cont, runs ++ [run]}
    end
  end

  defp once(req, path, params) do
    drain()
    started = System.monotonic_time(:microsecond)

    response =
      Req.post!(req, url: path, form: params, decode_body: false, receive_timeout: 1_200_000)

    outcome(response, System.monotonic_time(:microsecond) - started)
  end

  defp drain do
    receive do
      {:query, _measurements} -> drain()
    after
      0 -> :ok
    end
  end

  defp outcome(%{status: 200, body: ~s({"status":"success",) <> _rest}, wall) do
    receive do
      {:query, m} ->
        %{
          wall_us: wall,
          series: m.series,
          samples: m.samples,
          fetch_us: m.fetch_us,
          sweep_us: m.duration_us - m.fetch_us,
          rest_us: wall - m.duration_us
        }
    after
      0 -> %{wall_us: wall, series: "", samples: "", fetch_us: nil, sweep_us: nil, rest_us: nil}
    end
  end

  defp outcome(%{status: status, body: body}, _wall), do: {:error, status, body}

  @doc """
  The SQL controls over `[from_s, to_s]`: `host` is one series' fingerprint,
  `group` the fingerprints of the series `group_label` selects, and
  `wide_label` one matching many series, counted per step in SQL the way a
  pushed-down `count(...)` would be (T-568). The grouped query is also run
  without its `labels` column for the fingerprint list, which is what the
  fetch costs once a series table holds the labels (T-569).
  """
  def sql_controls(from_s, to_s, {host_label, host}, {group_label, group}, wide_label, reps) do
    {:ok, from} = SmolqueryVictoriaMetrics.Samples.time(from_s * 1_000)
    {:ok, to} = SmolqueryVictoriaMetrics.Samples.time(to_s * 1_000)
    {host_name, host_value} = host_label
    {group_name, group_value} = group_label
    {wide_name, wide_value} = wide_label
    in_list = "series IN (#{Enum.join(group, ", ")})"

    IO.puts("")
    IO.puts(label("sql (same range, name = 'm')", 60) <> pad("rows", 8) <> pad("wall ms", 9))
    sql_row("count(*), no label filter", count_sql("TRUE"), ["m", from, to], reps)

    predicates = [
      {"labels['#{host_name}'] = '#{host_value}'", "labels[$4] = $5", [host_name, host_value]},
      {"series = <#{host_value}>", "series = $4", [host]},
      {"labels['#{group_name}'] = '#{group_value}'", "labels[$4] = $5",
       [group_name, group_value]},
      {"series IN (<#{length(group)} of #{group_value}>)", in_list, []}
    ]

    for {shape, sql} <- [{"count(*)", &count_sql/1}, {"grouped", &grouped_sql(&1, :labels)}],
        {name, predicate, extra} <- predicates do
      sql_row("#{shape} WHERE #{name}", sql.(predicate), ["m", from, to | extra], reps)
    end

    sql_row(
      "grouped, no labels column, WHERE series IN (<#{length(group)} of #{group_value}>)",
      grouped_sql(in_list, :no_labels),
      ["m", from, to],
      reps
    )

    sql_row(
      "count(DISTINCT series) by step WHERE labels['#{wide_name}'] = '#{wide_value}'",
      pushed_sql("labels[$4] = $5"),
      ["m", from, to, wide_name, wide_value],
      reps
    )
  end

  defp count_sql(predicate),
    do: "SELECT count(*) AS n FROM metrics.samples WHERE #{where(predicate)}"

  defp grouped_sql(predicate, labels) do
    "SELECT series, any_value(name) AS name, #{labels_column(labels)}" <>
      "list(epoch_ms(ts) ORDER BY ts, value) AS timestamps, " <>
      "list(value ORDER BY ts, value) AS samples, " <>
      "CAST(sum(count(*)) OVER () AS BIGINT) AS total " <>
      "FROM metrics.samples WHERE #{where(predicate)} GROUP BY series LIMIT 1000001"
  end

  defp labels_column(:labels), do: "any_value(labels) AS labels, "
  defp labels_column(:no_labels), do: ""

  defp pushed_sql(predicate) do
    "SELECT time_bucket(INTERVAL '#{@step_s} seconds', ts) AS step, " <>
      "count(DISTINCT series) AS n FROM metrics.samples WHERE #{where(predicate)} " <>
      "GROUP BY step ORDER BY step"
  end

  defp where(predicate), do: "name = $1 AND ts BETWEEN $2 AND $3 AND #{predicate}"

  defp sql_row(name, sql, params, reps) do
    run_sql(sql, params)
    runs = for _rep <- 1..reps, do: run_sql(sql, params)
    rows = runs |> Enum.map(&elem(&1, 1)) |> median()
    wall = runs |> Enum.map(&elem(&1, 0)) |> median()
    IO.puts(label(name, 60) <> pad(rows, 8) <> pad(ms(wall), 9))
  end

  defp run_sql(sql, params) do
    started = System.monotonic_time(:microsecond)

    {:ok, %{id: id, state: :done}, frame} =
      Client.query(@query_name, sql,
        params: params,
        result_max_rows: 1_000_001,
        timeout_ms: 600_000
      )

    wall = System.monotonic_time(:microsecond) - started
    :ok = Client.release(@query_name, id)
    {wall, if(frame, do: DataFrame.n_rows(frame), else: 0)}
  end

  def median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    case {rem(count, 2), Enum.at(sorted, middle - 1), Enum.at(sorted, middle)} do
      {0, low, high} when is_number(low) and is_number(high) and low != high -> (low + high) / 2
      {_parity, _low, high} -> high
    end
  end

  defp opt_ms(nil), do: ""
  defp opt_ms(us), do: ms(us)
end
