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
  alias Smolquery.Segments.Writer

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
        Writer.write(values, schema, store: Local.new(dir: Path.join(dir, "segments")))

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

  def ms(microseconds), do: Float.round(microseconds / 1000, 1)

  def mib(bytes), do: Float.round(bytes / 1_048_576, 1)

  def pad(value, width), do: value |> to_string() |> String.pad_leading(width)

  def label(value, width), do: value |> to_string() |> String.pad_trailing(width)

  def heading(text) do
    IO.puts("\n" <> text)
    IO.puts(String.duplicate("─", String.length(text)))
  end
end
