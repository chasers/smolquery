Code.require_file("victoriametrics_support.exs", __DIR__)

defmodule Bench.VictoriaMetrics do
  @moduledoc """
  The VictoriaMetrics edge end to end over HTTP (PL-70, T-567, T-583):
  Prometheus remote write in, MetricsQL and the label routes out, on the
  private node `Bench.VictoriaMetricsSupport` starts. Nothing seals, so
  every sample stays in the hot tier: the depth a write lands at is the
  samples already there.

  Four sections:

    * **write** — `WriteRequest` bodies of `SERIES` series (10,000 at
      most) with `10,000 / SERIES` samples each, 10,000 samples a block as
      vmagent sends them, protobuf encoded and snappy framed as Prometheus
      remote write 1.0, posted by `WRITERS` concurrent clients with `Req`.
      Samples per second and the p50 and p99 ack latency, first into an
      empty table, then again once the read set below is loaded. A 429 is
      retried after its `retry-after`, as vmagent would, and counted.
    * **load** — `SERIES` series of `m` over `HOURS` hours at 15 s, with a
      `job` label of ten values, a `pod` label of `SERIES / 100` values
      (one at least), and an `instance` per series. The read set. Past
      10,000 series a block holds one sample of 10,000 series, and the
      series are sent in slices.
    * **read** — `rate(m[5m])` over one hour and over `HOURS`, `sum by
      (job) (rate(m[5m]))` over both, `default_rollup` of one series over
      `HOURS`, all at a 15 s step, and `/api/v1/labels`,
      `/api/v1/label/job/values` and `/api/v1/label/pod/values` over
      `HOURS`. `READS=labels` skips this section, whose wide matrices
      render for minutes past 1,000 series.
    * **label filters** (T-583) — `count(m{...})` over `HOURS` for each
      matcher shape on `pod`, `job` and `instance`: an equality that
      matches about 100 series, a regular expression, an inequality that
      matches nearly all, and the one-series `instance`; every sample of
      `m` in the range is what the scan reads, since nothing prunes on a
      label. Then the SQL controls: the edge's grouped query with the MAP
      lookup against the same series' fingerprints.

      mix run bench/victoriametrics.exs 2>/dev/null
      SERIES=1000 HOURS=6 WRITERS=8 WRITE_REQUESTS=100 REPS=5 mix run bench/victoriametrics.exs 2>/dev/null
      SERIES=10000 HOURS=6 READS=labels REPS=3 mix run bench/victoriametrics.exs 2>/dev/null

  The driver runs on the node's machine, so the write numbers are a node
  ceiling with a co-resident client. Redirecting stderr keeps ADBC's
  per-query deprecation warning out of the tables. For a billion series
  see `bench/victoriametrics_cardinality.exs`.
  """

  import Bench.Support, except: [table: 0, schema: 0]
  import Bench.VictoriaMetricsSupport
  import Bitwise

  alias SmolqueryVictoriaMetrics.Write

  @block 10_000
  @jobs 10
  @series_per_pod 100

  def main do
    Logger.configure(level: :warning)
    Supervisor.terminate_child(Smolquery.Supervisor, Smolquery.StorageService.Supervisor)

    config = %{
      series: env("SERIES", 1_000),
      hours: env("HOURS", 6),
      writers: env("WRITERS", 8),
      requests: env("WRITE_REQUESTS", 100),
      reps: env("REPS", 5),
      reads: reads()
    }

    with_tmp_dir("victoriametrics", fn dir ->
      base = start_node!(dir)
      req = Req.new(base_url: base, auth: {:bearer, password()}, retry: false)
      now_s = div(System.system_time(:second), step_s()) * step_s()

      schedulers()
      heading("Remote write — #{@block} samples a request, #{config.writers} writers")

      IO.puts(
        label("depth (samples)", 18) <>
          pad("requests", 10) <>
          pad("samples/s", 12) <> pad("p50 ms", 9) <> pad("p99 ms", 9) <> pad("429s", 7)
      )

      write_round(req, config, 0, now_s, 0)

      heading("Load — #{config.series} series of m over #{config.hours} h at #{step_s()} s")
      load(req, config, now_s)

      write_round(req, config, config.requests * @block + loaded(config), now_s, 1)

      if config.reads == :all, do: read(req, config, now_s)
      label_filters(req, config, now_s)
    end)
  end

  defp loaded(config), do: config.series * div(config.hours * 3_600, step_s())

  defp reads do
    case System.get_env("READS", "all") do
      "all" -> :all
      "labels" -> :labels
      other -> raise ArgumentError, "READS must be all or labels; got #{inspect(other)}"
    end
  end

  defp pods(config), do: max(div(config.series, @series_per_pod), 1)

  defp pod(config), do: "pod-#{rem(1, pods(config))}"

  defp write_round(req, config, depth, now_s, round) do
    series = min(config.series, @block)
    per_series = div(@block, series)

    bodies =
      for n <- 1..config.requests do
        first_ms = (now_s - 86_400 * (round + 1)) * 1_000 + n * per_series * 1_000

        1..series
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
    slice = min(config.series, @block)
    per_series = div(@block, slice)
    points = div(config.hours * 3_600, step_s())
    first_ms = (now_s - config.hours * 3_600) * 1_000

    chunks = div(points, per_series)
    slices = div(config.series + slice - 1, slice)

    bodies =
      Stream.flat_map(0..(chunks - 1)//1, fn chunk ->
        Stream.map(1..config.series//slice, fn first ->
          first..min(first + slice - 1, config.series)
          |> read_series(config, first_ms + chunk * per_series * step_s() * 1_000, per_series)
          |> encode()
        end)
      end)

    started = System.monotonic_time(:microsecond)
    results = post_all(req, bodies, config.writers)
    elapsed = System.monotonic_time(:microsecond) - started

    IO.puts(
      "  #{chunks * slices} requests, #{chunks * slices * @block} samples in " <>
        "#{ms(elapsed)} ms, #{Enum.sum_by(results, &elem(&1, 1))} retried after a 429"
    )
  end

  defp write_series(range, name, first_ms, per_series, spacing_ms) do
    for i <- range do
      labels = [{"__name__", name}, {"instance", "host-#{i}"}, {"job", "job-#{rem(i, @jobs)}"}]
      {labels, for(j <- 0..(per_series - 1), do: {first_ms + j * spacing_ms, j * 1.0})}
    end
  end

  defp read_series(range, config, first_ms, per_series) do
    first_n = div(first_ms, step_s() * 1_000)

    for i <- range do
      {[{"__name__", "m"} | read_labels(config, i)],
       for j <- 0..(per_series - 1) do
         {first_ms + j * step_s() * 1_000, (first_n + j) * 1.0 * rem(i, 7) + 1.0}
       end}
    end
  end

  defp read_labels(config, i),
    do: [
      {"instance", "host-#{i}"},
      {"job", "job-#{rem(i, @jobs)}"},
      {"pod", "pod-#{rem(i, pods(config))}"}
    ]

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
    hour = now_s - 3_600
    whole = now_s - config.hours * 3_600

    heading(
      "Reads — #{config.series} series, #{config.hours} h at #{step_s()} s; median of #{config.reps}"
    )

    read_header(44)

    for {name, {path, params}} <- [
          {"rate(m[5m]) 1h", range("rate(m[5m])", hour, now_s)},
          {"rate(m[5m]) #{config.hours}h", range("rate(m[5m])", whole, now_s)},
          {"sum by (job) (rate(m[5m])) 1h", range("sum by (job) (rate(m[5m]))", hour, now_s)},
          {"sum by (job) (rate(m[5m])) #{config.hours}h",
           range("sum by (job) (rate(m[5m]))", whole, now_s)},
          {~s|m{instance="host-1"} #{config.hours}h|,
           range(~s|m{instance="host-1"}|, whole, now_s)},
          {"/api/v1/labels #{config.hours}h",
           {"/api/v1/labels", %{"start" => whole, "end" => now_s}}},
          {"/api/v1/label/job/values #{config.hours}h",
           {"/api/v1/label/job/values", %{"start" => whole, "end" => now_s}}},
          {"/api/v1/label/pod/values #{config.hours}h",
           {"/api/v1/label/pod/values", %{"start" => whole, "end" => now_s}}}
        ] do
      read_row(req, name, path, params, config.reps, 44)
    end
  end

  defp label_filters(req, config, now_s) do
    whole = now_s - config.hours * 3_600

    heading(
      "Label filters — #{config.series} series, #{pods(config)} pods, " <>
        "#{config.hours} h; #{loaded(config)} samples of m in range; median of #{config.reps}"
    )

    read_header(30)

    pod = pod(config)

    for selector <- [
          ~s|m{instance="host-1"}|,
          ~s|m{pod="#{pod}"}|,
          ~s|m{pod=~"pod-1.*"}|,
          ~s|m{pod!="#{pod}"}|,
          ~s|m{job="job-1"}|
        ] do
      {path, params} = range("count(#{selector})", whole, now_s)
      read_row(req, selector, path, params, config.reps, 30)
    end

    pod_series =
      for i <- 1..config.series,
          rem(i, pods(config)) == rem(1, pods(config)),
          do: fingerprint(config, i)

    sql_controls(
      whole,
      now_s,
      {{"instance", "host-1"}, fingerprint(config, 1)},
      {{"pod", pod}, pod_series},
      {"job", "job-1"},
      config.reps
    )
  end

  defp fingerprint(config, i), do: Write.fingerprint("m", read_labels(config, i))

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
