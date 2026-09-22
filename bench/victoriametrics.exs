Code.require_file("support.exs", __DIR__)

defmodule Bench.VictoriaMetrics do
  @moduledoc """
  The VictoriaMetrics edge end to end over HTTP (PL-70, T-567): Prometheus
  remote write in, MetricsQL and the label routes out.

  A private node is started beside the application's own: a DuckLake
  catalog, a buffer, an ingest service, a query service reading the
  buffer's hot tier, and the edge on a port the OS picks. No storage
  service, so nothing seals and every sample stays in the hot tier: the
  depth a write lands at is the samples already there.

  Three sections:

    * **write** — `WriteRequest` bodies of `SERIES` series with
      `10,000 / SERIES` samples each, 10,000 samples a block as vmagent sends them,
      protobuf encoded and snappy framed as Prometheus remote write 1.0,
      posted by `WRITERS` concurrent clients with `Req`. Samples per second
      and the p50 and p99 ack latency, first into an empty table, then again
      once the read set below is loaded. A 429 is retried after its
      `retry-after`, as vmagent would, and counted.
    * **load** — `SERIES` series of `m` over `HOURS` hours at 15 s, with a
      `job` label of ten values and an `instance` per series. The read set.
    * **read** — `rate(m[5m])` over one hour and over `HOURS`, `sum by
      (job) (rate(m[5m]))` over both, `default_rollup` of one series over
      `HOURS`, all at a 15 s step, and `/api/v1/labels` and
      `/api/v1/label/job/values` over `HOURS`. Each is run once to warm,
      then `REPS` times; the table has the medians. `fetch` is the time in
      `SmolqueryVictoriaMetrics.Samples` reading raw samples by SQL, from
      the `fetch_us` of `[:smolquery, :victoriametrics, :query]`, and
      `sweep` is the rest of the query's own time: the rollup sweep and
      evaluation in the node. `rest` is the wall time past both: rendering
      the JSON answer and HTTP; the driver does not decode it. The label
      routes emit no query event, so they have a wall time only.

      mix run bench/victoriametrics.exs 2>/dev/null
      SERIES=1000 HOURS=6 WRITERS=8 WRITE_REQUESTS=100 REPS=5 mix run bench/victoriametrics.exs 2>/dev/null

  The driver runs on the node's machine, so the write numbers are a node
  ceiling with a co-resident client. Redirecting stderr keeps ADBC's
  per-query deprecation warning out of the tables.
  """

  import Bench.Support, except: [table: 0, schema: 0]
  import Bitwise

  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.IngestService
  alias Smolquery.QueryService
  alias Smolquery.Schema

  @password "bench-victoriametrics"
  @step_s 15
  @block 10_000
  @jobs 10

  def main do
    Logger.configure(level: :warning)
    Supervisor.terminate_child(Smolquery.Supervisor, Smolquery.StorageService.Supervisor)

    config = %{
      series: env("SERIES", 1_000),
      hours: env("HOURS", 6),
      writers: env("WRITERS", 8),
      requests: env("WRITE_REQUESTS", 100),
      reps: env("REPS", 5)
    }

    with_tmp_dir("victoriametrics", fn dir ->
      base = start_node!(dir)
      req = Req.new(base_url: base, auth: {:bearer, @password}, retry: false)
      now_s = div(System.system_time(:second), @step_s) * @step_s

      schedulers()
      heading("Remote write — #{@block} samples a request, #{config.writers} writers")

      IO.puts(
        label("depth (samples)", 18) <>
          pad("requests", 10) <>
          pad("samples/s", 12) <> pad("p50 ms", 9) <> pad("p99 ms", 9) <> pad("429s", 7)
      )

      write_round(req, config, 0, now_s, 0)

      heading("Load — #{config.series} series of m over #{config.hours} h at #{@step_s} s")
      load(req, config, now_s)

      write_round(req, config, config.requests * @block + loaded(config), now_s, 1)

      read(req, config, now_s)
    end)
  end

  defp start_node!(dir) do
    lake = :bench_vm_lake
    metadata = "sqlite:#{Path.join(dir, "catalog.sqlite")}"
    data_path = Path.join(dir, "data")

    {:ok, _pid} = DuckLake.start_link(name: lake, metadata: metadata, data_path: data_path)
    catalog = DuckLake.new(engine: lake)
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
        name: :bench_vm_query,
        catalog: catalog,
        buffer_name: :bench_vm_buffer,
        buffer_base_url: HotServer.base_url(:bench_vm_buffer),
        engine_extensions: [:httpfs],
        allowed_directories: [dir],
        job_bootstrap: [
          DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
        ]
      )

    {:ok, _pid} =
      SmolqueryVictoriaMetrics.Supervisor.start_link(
        name: :bench_vm_edge,
        password: @password,
        port: 0,
        ingest_name: :bench_vm_ingest,
        query_name: :bench_vm_query,
        catalog: catalog
      )

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

  defp loaded(config), do: config.series * div(config.hours * 3_600, @step_s)

  defp write_round(req, config, depth, now_s, round) do
    per_series = div(@block, config.series)

    bodies =
      for n <- 1..config.requests do
        first_ms = (now_s - 86_400 * (round + 1)) * 1_000 + n * per_series * 1_000

        config.series
        |> write_series("bench_write", first_ms, per_series, 1_000)
        |> encode()
      end

    started = System.monotonic_time(:microsecond)
    results = post_all(req, bodies, config.writers)
    elapsed = System.monotonic_time(:microsecond) - started
    latency = percentiles(Enum.map(results, &elem(&1, 0)))
    retried = Enum.sum_by(results, &elem(&1, 1))
    rate = Float.round(config.requests * @block * 1_000_000 / elapsed, 0)

    IO.puts(
      label(depth, 18) <>
        pad(config.requests, 10) <>
        pad(rate, 12) <> pad(latency.p50, 9) <> pad(latency.p99, 9) <> pad(retried, 7)
    )
  end

  defp load(req, config, now_s) do
    per_series = div(@block, config.series)
    points = div(config.hours * 3_600, @step_s)
    first_ms = (now_s - config.hours * 3_600) * 1_000

    bodies =
      for chunk <- 0..(div(points, per_series) - 1) do
        config.series
        |> read_series(first_ms + chunk * per_series * @step_s * 1_000, per_series)
        |> encode()
      end

    started = System.monotonic_time(:microsecond)
    results = post_all(req, bodies, config.writers)
    elapsed = System.monotonic_time(:microsecond) - started

    IO.puts(
      "  #{length(bodies)} requests, #{length(bodies) * @block} samples in " <>
        "#{ms(elapsed)} ms, #{Enum.sum_by(results, &elem(&1, 1))} retried after a 429"
    )
  end

  defp write_series(count, name, first_ms, per_series, spacing_ms) do
    for i <- 1..count do
      labels = [{"__name__", name}, {"instance", "host-#{i}"}, {"job", "job-#{rem(i, @jobs)}"}]
      {labels, for(j <- 0..(per_series - 1), do: {first_ms + j * spacing_ms, j * 1.0})}
    end
  end

  defp read_series(count, first_ms, per_series) do
    for i <- 1..count do
      labels = [{"__name__", "m"}, {"instance", "host-#{i}"}, {"job", "job-#{rem(i, @jobs)}"}]
      first_n = div(first_ms, @step_s * 1_000)

      {labels,
       for j <- 0..(per_series - 1) do
         {first_ms + j * @step_s * 1_000, (first_n + j) * 1.0 * rem(i, 7) + 1.0}
       end}
    end
  end

  defp post_all(req, bodies, writers) do
    bodies
    |> Task.async_stream(&post(req, &1, 0), max_concurrency: writers, timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp post(req, body, retried) do
    started = System.monotonic_time(:microsecond)

    response =
      Req.post!(req,
        url: "/api/v1/write",
        body: body,
        headers: [
          {"content-type", "application/x-protobuf"},
          {"content-encoding", "snappy"},
          {"x-prometheus-remote-write-version", "0.1.0"}
        ]
      )

    case response.status do
      204 ->
        {System.monotonic_time(:microsecond) - started, retried}

      429 ->
        [seconds] = Req.Response.get_header(response, "retry-after")
        Process.sleep(String.to_integer(seconds) * 1_000)
        post(req, body, retried + 1)

      status ->
        raise "remote write answered #{status}: #{inspect(response.body)}"
    end
  end

  defp read(req, config, now_s) do
    handler = "bench-victoriametrics"
    parent = self()

    :telemetry.attach(
      handler,
      [:smolquery, :victoriametrics, :query],
      fn _event, measurements, _meta, _config -> send(parent, {:query, measurements}) end,
      nil
    )

    hour = now_s - 3_600
    whole = now_s - config.hours * 3_600

    heading(
      "Reads — #{config.series} series, #{config.hours} h at #{@step_s} s; median of #{config.reps}"
    )

    IO.puts(
      label("query", 44) <>
        pad("series", 8) <>
        pad("samples", 10) <>
        pad("wall ms", 9) <> pad("fetch ms", 10) <> pad("sweep ms", 10) <> pad("rest ms", 9)
    )

    range = fn query, start ->
      {"/api/v1/query_range",
       %{"query" => query, "start" => start, "end" => now_s, "step" => "#{@step_s}s"}}
    end

    for {name, {path, params}} <- [
          {"rate(m[5m]) 1h", range.("rate(m[5m])", hour)},
          {"rate(m[5m]) #{config.hours}h", range.("rate(m[5m])", whole)},
          {"sum by (job) (rate(m[5m])) 1h", range.("sum by (job) (rate(m[5m]))", hour)},
          {"sum by (job) (rate(m[5m])) #{config.hours}h",
           range.("sum by (job) (rate(m[5m]))", whole)},
          {~s|m{instance="host-1"} #{config.hours}h|, range.(~s|m{instance="host-1"}|, whole)},
          {"/api/v1/labels #{config.hours}h",
           {"/api/v1/labels", %{"start" => whole, "end" => now_s}}},
          {"/api/v1/label/job/values #{config.hours}h",
           {"/api/v1/label/job/values", %{"start" => whole, "end" => now_s}}}
        ] do
      read_row(req, name, path, params, config.reps)
    end

    :telemetry.detach(handler)
  end

  defp read_row(req, name, path, params, reps) do
    once(req, path, params)
    runs = for _rep <- 1..reps, do: once(req, path, params)
    median = fn key -> runs |> Enum.map(&Map.get(&1, key)) |> median() end

    IO.puts(
      label(name, 44) <>
        pad(median.(:series), 8) <>
        pad(median.(:samples), 10) <>
        pad(ms(median.(:wall_us)), 9) <>
        pad(opt_ms(median.(:fetch_us)), 10) <>
        pad(opt_ms(median.(:sweep_us)), 10) <>
        pad(opt_ms(median.(:rest_us)), 9)
    )
  end

  defp once(req, path, params) do
    started = System.monotonic_time(:microsecond)
    response = Req.post!(req, url: path, form: params, decode_body: false)
    wall = System.monotonic_time(:microsecond) - started
    %{status: 200, body: ~s({"status":"success",) <> _rest} = response

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

  defp median(values), do: values |> Enum.sort() |> Enum.at(div(length(values), 2))

  defp opt_ms(nil), do: ""
  defp opt_ms(us), do: ms(us)

  defp encode(series) do
    series
    |> Enum.map_join(&timeseries/1)
    |> snappy()
  end

  defp timeseries({labels, samples}) do
    bytes(
      1,
      Enum.map_join(labels, fn {name, value} -> bytes(1, bytes(1, name) <> bytes(2, value)) end) <>
        Enum.map_join(samples, fn {ts, value} ->
          bytes(2, <<9, value::float-little-64, 16>> <> varint(ts))
        end)
    )
  end

  defp snappy(data) do
    size = byte_size(data)
    varint(size) <> <<63 <<< 2, size - 1::little-32>> <> data
  end

  defp varint(n) when n < 0x80, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 0x7F::7, varint(n >>> 7)::binary>>

  defp bytes(field, value), do: varint(field <<< 3 ||| 2) <> varint(byte_size(value)) <> value
end

Bench.VictoriaMetrics.main()
