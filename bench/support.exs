Code.require_file("../test/support/segment_fixture.ex", __DIR__)

defmodule Bench.Support do
  @moduledoc """
  Shared fixtures and timing helpers for the scripts in `bench/`.

  Loaded with `Code.require_file("support.exs", __DIR__)` rather than compiled,
  so nothing here is part of the application.
  """

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture

  @dataset "analytics"
  @table {"analytics", "events"}

  @doc """
  The table this suite measures against.
  """
  def table, do: @table

  @doc """
  The schema every fixture uses — one column per logical type worth pruning on.
  """
  def schema do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}}
    ])
  end

  @doc """
  Runs `fun` with a fresh temporary directory, removing it afterwards.
  """
  def with_tmp_dir(label, fun) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-bench-#{label}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(dir, "segments"))

    try do
      fun.(dir)
    after
      File.rm_rf!(dir)
    end
  end

  @doc """
  Starts a DuckLake-backed engine in `dir` and returns its catalog handle.

  `:max_result_rows` defaults to `:infinity` here, unlike the application default:
  these scripts exist to measure the cost of converting a large result, which the
  ceiling is there to prevent a real caller from paying by accident.
  """
  def start_lake!(name, dir, opts \\ []) do
    defaults = [
      name: name,
      metadata: "sqlite:#{Path.join(dir, "catalog.sqlite")}",
      data_path: Path.join(dir, "data"),
      max_result_rows: :infinity
    ]

    {:ok, _pid} = DuckLake.start_link(Keyword.merge(defaults, opts))

    catalog = DuckLake.new(engine: name)
    :ok = Catalog.create_dataset(catalog, @dataset)
    :ok = Catalog.create_table(catalog, @table, schema())

    catalog
  end

  @doc """
  Writes `count` segments of `rows` rows each, one day apart so their `ts`
  ranges are disjoint and a range predicate has something to prune.
  """
  def write_segments!(dir, count, rows) do
    schema = schema()
    base = ~N[2026-01-01 00:00:00]
    span = min(rows, 80_000)

    for day <- 1..count do
      day_start = NaiveDateTime.add(base, (day - 1) * 86_400, :second)

      values =
        for i <- 1..rows do
          %{
            "id" => day * 1_000_000 + i,
            "ts" => NaiveDateTime.add(day_start, rem(i, span), :second),
            "name" => "row-#{i}",
            "amount" => Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")
          }
        end

      {:ok, segment} =
        SegmentFixture.write(values, schema, store: Local.new(dir: Path.join(dir, "segments")))

      segment
    end
  end

  @doc """
  The `ts` bounds covering days `from` through `to`, exclusive of `to`.
  """
  def bounds(from, to) do
    base = ~N[2026-01-01 00:00:00]

    {NaiveDateTime.add(base, (from - 1) * 86_400, :second),
     NaiveDateTime.add(base, (to - 1) * 86_400, :second)}
  end

  @doc """
  Runs `fun` once to warm, then `reps` times, reporting min and median.
  """
  def timed(fun, reps) do
    value = fun.()

    times =
      Enum.sort(
        for _ <- 1..reps do
          {us, _result} = :timer.tc(fun)
          us
        end
      )

    %{value: value, min: hd(times), median: Enum.at(times, div(reps, 2))}
  end

  @doc """
  Millisecond percentiles over microsecond samples.

  A sustained-load script reports a distribution rather than a median: the
  interesting number in a tail latency is where it stops being typical.
  """
  def percentiles([]), do: %{n: 0, p50: nil, p95: nil, p99: nil, max: nil}

  def percentiles(samples) do
    sorted = Enum.sort(samples)
    count = length(sorted)
    at = fn fraction -> ms(Enum.at(sorted, min(count - 1, trunc(fraction * count)))) end

    %{n: count, p50: at.(0.50), p95: at.(0.95), p99: at.(0.99), max: at.(1.0)}
  end

  @doc """
  How many files a query's scan actually opened, per `EXPLAIN ANALYZE`.

  This is the number a pruning measurement lives on. Wall clock alone hides a
  lost prune whenever the fixture is small enough for a full scan to be fast.
  """
  def files_read(engine, sql, params \\ []) do
    case Engine.query(engine, "EXPLAIN ANALYZE " <> sql, params) do
      {:ok, result} ->
        text = result.rows |> List.flatten() |> Enum.map_join("\n", &to_string/1)

        case Regex.run(~r/Total Files Read:\s*([\d,]+)/, text) do
          [_match, count] -> count |> String.replace(",", "") |> String.to_integer()
          nil -> :not_reported
        end

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @doc """
  An integer environment variable, or `default`.
  """
  def env(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  @doc """
  Prints how much parallelism this VM actually has.

  Every throughput number in `bench/` is against this, and "10 cores" is not the
  whole story: DuckDB gets its own thread pool (`:threads`), and the BEAM has three
  scheduler classes, only one of which shows up in `:scheduler_wall_time`'s
  denominator alongside the normal ones.
  """
  def schedulers do
    heading("Runtime parallelism")

    for {key, value} <- [
          {"logical processors", :erlang.system_info(:logical_processors_available)},
          {"schedulers (online)",
           "#{:erlang.system_info(:schedulers)} (#{:erlang.system_info(:schedulers_online)})"},
          {"dirty CPU schedulers (online)",
           "#{:erlang.system_info(:dirty_cpu_schedulers)} " <>
             "(#{:erlang.system_info(:dirty_cpu_schedulers_online)})"},
          {"dirty IO schedulers", :erlang.system_info(:dirty_io_schedulers)},
          {"DuckDB threads per engine", engine_threads()}
        ] do
      IO.puts("  #{label(key, 32)} #{value}")
    end
  end

  defp engine_threads do
    :smolquery |> Application.get_env(Smolquery.Engine, []) |> Keyword.get(:threads, :default)
  end

  @doc """
  A point to measure CPU consumption from; pair with `cpu_since/1`.
  """
  def cpu_snapshot do
    :erlang.system_flag(:scheduler_wall_time, true)

    %{
      at: System.monotonic_time(:microsecond),
      cpu_us: os_cpu_microseconds(),
      scheduler: :erlang.statistics(:scheduler_wall_time)
    }
  end

  @doc """
  CPU used since `before`: whole cores consumed, and BEAM scheduler busy percent.

  `cores` comes from the OS process's CPU time, so it counts DuckDB's native
  threads — which no BEAM statistic sees — and it counts the co-resident driver too.
  `scheduler` covers the normal schedulers only: the dirty pools sit idle here and
  would dilute the ratio toward meaninglessness.
  """
  def cpu_since(before) do
    now = cpu_snapshot()
    wall = now.at - before.at

    %{
      cores: Float.round((now.cpu_us - before.cpu_us) / wall, 2),
      scheduler: scheduler_busy(before.scheduler, now.scheduler)
    }
  end

  defp scheduler_busy(before, now) when is_list(before) and is_list(now) do
    normal = :erlang.system_info(:schedulers)

    {active, total} =
      Enum.zip(Enum.sort(before), Enum.sort(now))
      |> Enum.take(normal)
      |> Enum.reduce({0, 0}, fn {{id, a0, t0}, {id, a1, t1}}, {active, total} ->
        {active + (a1 - a0), total + (t1 - t0)}
      end)

    case total do
      0 -> nil
      _busy -> Float.round(active / total * 100, 1)
    end
  end

  defp scheduler_busy(_before, _now), do: nil

  defp os_cpu_microseconds do
    {output, 0} = System.cmd("ps", ["-o", "cputime=", "-p", System.pid()])

    seconds =
      output
      |> String.trim()
      |> String.split(":")
      |> Enum.reduce(0.0, fn part, acc -> acc * 60 + parse_seconds(part) end)

    round(seconds * 1_000_000)
  end

  defp parse_seconds(part) do
    {value, _rest} = Float.parse(part)

    value
  end

  @doc """
  A comma-separated list of integers from an environment variable, or `default`.

  For the knobs a script sweeps rather than sets — `WRITERS=4,8,16,32`.
  """
  def sweep_env(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)
    end
  end

  def ms(microseconds), do: Float.round(microseconds / 1000, 1)

  def mib(bytes), do: Float.round(bytes / 1_048_576, 1)

  def pad(value, width) when is_float(value) and abs(value) >= 1000,
    do: value |> :erlang.float_to_binary(decimals: 1) |> String.pad_leading(width)

  def pad(value, width), do: value |> to_string() |> String.pad_leading(width)

  def label(value, width), do: value |> to_string() |> String.pad_trailing(width)

  def heading(text) do
    IO.puts("\n" <> text)
    IO.puts(String.duplicate("─", String.length(text)))
  end
end
