Code.require_file("support.exs", __DIR__)

defmodule Bench.Planner do
  @moduledoc """
  Should the sealed tier scan DuckLake, or plan around it?

  This is the measurement T-6 was decided on. Three ways to answer the same
  query, compared at three selectivities:

    A native   — scan `lake.analytics.events` and let DuckLake prune
    B planner  — `ducklake_list_files` + min-max from the metadata database +
                 prune in Elixir + `read_parquet` of what survives, per query
    C planner+ — the same, with the file list and stats cached per snapshot, so
                 only the prune and the scan are on the query path

  The answer was A: DuckLake's own pruning works, cold metadata planning loses at
  every selectivity, and the cached planner only wins on point lookups while
  losing badly on large scans because `read_parquet` takes its file list as SQL
  text. Re-run this before revisiting that, or after a DuckLake upgrade.

      mix run bench/planner.exs
      SEGMENTS=1500 ROWS=2000 REPS=7 mix run bench/planner.exs

  """

  import Bench.Support

  alias Smolquery.Catalog
  alias Smolquery.Engine

  @engine __MODULE__.Lake

  def run do
    segment_count = env("SEGMENTS", 300)
    row_count = env("ROWS", 20_000)
    reps = env("REPS", 7)

    with_tmp_dir("planner", fn dir ->
      catalog = start_lake!(@engine, dir)
      segments = write_segments!(dir, segment_count, row_count)
      {:ok, snapshot} = Catalog.register_segments(catalog, table(), segments)

      heading("fixture")

      IO.puts(
        "DuckDB #{Engine.version(@engine)} — #{segment_count} segments × #{row_count} rows " <>
          "= #{segment_count * row_count} rows, snapshot #{snapshot}"
      )

      metadata_costs(catalog)
      cached = snapshot_index(catalog)

      for {name, days} <- selectivities(segment_count) do
        compare(catalog, cached, name, days, segment_count, reps)
      end
    end)
  end

  defp selectivities(segment_count) do
    [
      {"1 of #{segment_count}", 1},
      {"10%", max(div(segment_count, 10), 1)},
      {"all", segment_count}
    ]
  end

  defp metadata_costs(catalog) do
    heading("planner metadata side, cost of each piece")

    {list_us, {:ok, files}} = :timer.tc(fn -> Catalog.segments(catalog, table(), :current) end)
    IO.puts("  ducklake_list_files (#{length(files)} files) : #{ms(list_us)} ms")

    {stats_us, stats} = :timer.tc(fn -> file_stats() end)

    IO.puts(
      "  min-max for every file from metadata   : #{ms(stats_us)} ms (#{map_size(stats)} files)"
    )
  end

  defp compare(catalog, cached, name, days, segment_count, reps) do
    {from, to} = bounds(1, 1 + days)
    heading("selectivity: #{name}")

    native = timed(fn -> native(from, to) end, reps)
    cold = timed(fn -> plan_and_scan(snapshot_index(catalog), from, to) end, reps)
    warm = timed(fn -> plan_and_scan(cached, from, to) end, reps)

    {cold_files, cold_value} = cold.value
    {warm_files, _warm_value} = warm.value

    IO.puts(
      "  A native                   #{report(native)}  #{pad(files_read(@engine, native_sql(), [from, to]), 4)} files"
    )

    IO.puts("  B planner, cold metadata   #{report(cold)}  #{pad(cold_files, 4)} files")
    IO.puts("  C planner, snapshot-cached #{report(warm)}  #{pad(warm_files, 4)} files")
    IO.puts("  of #{segment_count} segments, and all three agree: #{native.value == cold_value}")
  end

  defp report(%{min: min, median: median}),
    do: "min #{pad(ms(min), 7)} ms  median #{pad(ms(median), 7)} ms"

  defp native_sql do
    ~s|SELECT count(*), sum(id) FROM lake."analytics"."events" WHERE ts >= $1 AND ts < $2|
  end

  defp native(from, to), do: Engine.query!(@engine, native_sql(), [from, to]).rows

  defp plan_and_scan(index, from, to) do
    pruned = for {path, bounds} <- index, overlaps?(bounds, from, to), do: path

    {length(pruned), scan(pruned, from, to)}
  end

  defp scan([], _from, _to), do: [[0, nil]]

  defp scan(files, from, to) do
    list = Enum.map_join(files, ", ", &Smolquery.Identifier.sql_string/1)

    Engine.query!(
      @engine,
      "SELECT count(*), sum(id) FROM read_parquet([#{list}]) WHERE ts >= $1 AND ts < $2",
      [from, to]
    ).rows
  end

  defp snapshot_index(catalog) do
    {:ok, files} = Catalog.segments(catalog, table(), :current)
    stats = file_stats()

    files
    |> Enum.with_index(1)
    |> Enum.map(fn {path, id} -> {path, Map.get(stats, id)} end)
  end

  defp file_stats do
    sql = """
    SELECT s.data_file_id, MIN(s.min_value), MAX(s.max_value)
      FROM __ducklake_metadata_lake.main.ducklake_file_column_stats s
      JOIN __ducklake_metadata_lake.main.ducklake_column c
        ON c.table_id = s.table_id AND c.column_id = s.column_id
      JOIN __ducklake_metadata_lake.main.ducklake_table t
        ON t.table_id = s.table_id
     WHERE t.table_name = $1 AND c.column_name = 'ts'
     GROUP BY s.data_file_id
    """

    case Engine.query(@engine, sql, ["events"]) do
      {:ok, result} ->
        Map.new(result.rows, fn [id, min, max] -> {id, {parse(min), parse(max)}} end)

      {:error, _error} ->
        %{}
    end
  end

  defp parse(nil), do: nil

  defp parse(value) do
    case NaiveDateTime.from_iso8601(String.replace(value, " ", "T")) do
      {:ok, timestamp} -> timestamp
      _other -> nil
    end
  end

  defp overlaps?(nil, _from, _to), do: true
  defp overlaps?({nil, _max}, _from, _to), do: true
  defp overlaps?({_min, nil}, _from, _to), do: true

  defp overlaps?({min, max}, from, to) do
    NaiveDateTime.compare(min, to) == :lt and NaiveDateTime.compare(max, from) != :lt
  end
end

Bench.Planner.run()
