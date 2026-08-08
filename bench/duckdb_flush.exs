Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.DuckdbFlush do
  @moduledoc """
  The two ways a flush can turn NDJSON bytes into a Parquet segment, timed against
  each other on the same bytes.

  PR #105 proposed replacing the write path's Elixir terms with one DuckDB
  statement and measured half the CPU per row for it. That measurement was taken
  against `main`, which materializes every value as a term twice — a baseline
  T-160/T-161 has since replaced with one native `load_ndjson` plus column casts.
  So the question this answers is not #105's ("is DuckDB cheaper than building
  terms?", which nobody doubts) but the one that decides whether porting it is
  worth its weaker contract:

      is DuckDB's COPY cheaper than the Polars path we ship today?

  Both arms start from the same NDJSON bytes and end at a Parquet file holding
  the same rows, sorted the same way:

    * **polars** — `ColumnarValidator.validate/2` (one `load_ndjson` + per-column
      casts) then `Writer.write/3` (sort on the clustering key, Parquet encode).
      This is exactly what a group commit runs today.
    * **duckdb** — the bytes go to a file and one
      `COPY (SELECT * FROM read_json(...) ORDER BY ...) TO ... (FORMAT PARQUET)`
      does the rest, as #105's `Writer.write({:ndjson, paths}, ...)` does.

  The DuckDB arm is charged for writing the spool file, because the API pays that
  cost in #105's design and it is not free.

      mix run bench/duckdb_flush.exs
      ROWS=2000 BATCHES=8 REPS=20 CLUSTERING=1 mix run bench/duckdb_flush.exs

  `BATCHES` models the group commit: a flush merges several request bodies, so
  the DuckDB arm reads N spool files in one `COPY` and the Polars arm concatenates
  N frames before encoding.
  """

  alias Smolquery.Engine
  alias Smolquery.IngestService.ColumnarValidator
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @engine __MODULE__.Engine
  @compression :zstd

  def run do
    rows = Bench.Support.env("ROWS", 2_000)
    batches = Bench.Support.env("BATCHES", 4)
    reps = Bench.Support.env("REPS", 10)
    clustered? = Bench.Support.env("CLUSTERING", 0) == 1

    dir = Path.join(System.tmp_dir!(), "duckdb-flush-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    threads = Bench.Support.env("THREADS", System.schedulers_online())

    {:ok, _pid} =
      Engine.start_link(name: @engine, max_result_rows: :infinity, threads: threads)

    schema = schema(clustered?)
    bodies = bodies(rows, batches)
    wire = bodies |> Enum.map(&byte_size/1) |> Enum.sum()

    IO.puts("""

    #{batches} batch(es) of #{rows} rows — #{Float.round(wire / 1_048_576, 2)} MB of NDJSON, \
    #{length(Bench.Otel.columns())} columns, clustering #{if clustered?, do: "on", else: "off"}
    #{reps} reps, compression #{@compression}, duckdb threads #{threads} (#{System.schedulers_online()} schedulers online)
    """)

    polars = measure(reps, fn i -> polars_flush(bodies, schema, store(dir, i)) end)
    duckdb = measure(reps, fn i -> duckdb_flush(bodies, schema, store(dir, i), dir, i) end)

    report(polars, duckdb, rows * batches, wire)

    File.rm_rf!(dir)
  end

  # Both arms must produce the same rows or the timing is meaningless, so each
  # returns its segment's row count and the caller asserts they agree.
  defp polars_flush(bodies, schema, store) do
    frames =
      Enum.map(bodies, fn body ->
        {:ok, frame} = ColumnarValidator.validate(schema, body)
        frame
      end)

    {:ok, merged} = Writer.merge_chunks(frames, schema)
    {:ok, segment} = Writer.write(merged, schema, store: store, compression: @compression)

    segment.row_count
  end

  defp duckdb_flush(bodies, schema, store, dir, index) do
    paths =
      bodies
      |> Enum.with_index()
      |> Enum.map(fn {body, j} ->
        path = Path.join(dir, "spool-#{index}-#{j}.ndjson")
        File.write!(path, body)
        path
      end)

    {:ok, segment} =
      Writer.write({:ndjson, paths}, schema,
        store: store,
        engine: @engine,
        compression: @compression
      )

    Enum.each(paths, &File.rm/1)

    segment.row_count
  end

  defp measure(reps, fun) do
    counts = for i <- 1..reps, do: {i, fun.(i)}
    [{_i, expected} | _rest] = counts

    for {i, count} <- counts, count != expected do
      raise "rep #{i} wrote #{count} rows, rep 1 wrote #{expected} — the arms disagree"
    end

    times =
      for i <- 1..reps do
        {micros, _result} = :timer.tc(fn -> fun.(reps + i) end)
        micros / 1_000
      end

    %{rows: expected, times: Enum.sort(times)}
  end

  defp report(polars, duckdb, rows, wire) do
    if polars.rows != duckdb.rows do
      raise "polars wrote #{polars.rows} rows, duckdb wrote #{duckdb.rows}"
    end

    IO.puts("  both arms wrote #{polars.rows} rows\n")
    IO.puts("  arm      median      min      max     rows/s      MB/s")

    for {name, stats} <- [{"polars", polars}, {"duckdb", duckdb}] do
      median = median(stats.times)

      IO.puts(
        "  #{String.pad_trailing(name, 8)}" <>
          "#{pad(median)}ms#{pad(hd(stats.times))}ms#{pad(List.last(stats.times))}ms" <>
          "#{String.pad_leading(number(rows / (median / 1_000)), 11)}" <>
          "#{String.pad_leading(Float.to_string(Float.round(wire / 1_048_576 / (median / 1_000), 1)), 10)}"
      )
    end

    ratio = median(polars.times) / median(duckdb.times)

    IO.puts("""

      duckdb is #{Float.round(ratio, 2)}x the polars path\
    #{if ratio < 1.0, do: " (slower)", else: ""}
    """)
  end

  defp pad(value), do: String.pad_leading(Float.to_string(Float.round(value, 1)), 9)

  defp number(value) do
    value
    |> round()
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  defp median(sorted), do: Enum.at(sorted, div(length(sorted) - 1, 2))

  defp store(dir, index) do
    path = Path.join(dir, "segments-#{index}")
    File.mkdir_p!(path)
    Store.Local.new(dir: path, fsync: false)
  end

  # The rig creates its tables without a clustering key, so the sort is a no-op
  # there and `CLUSTERING=0` is the configuration every throughput number in
  # PL-21 was measured under. `CLUSTERING=1` prices the sort both arms would pay
  # on a table that has one.
  defp schema(false), do: Bench.Otel.table_schema()

  defp schema(true) do
    %{Bench.Otel.table_schema() | clustering: ["service_name", "timestamp"]}
  end

  defp bodies(rows, batches) do
    pool = Bench.Otel.pool()

    for i <- 0..(batches - 1) do
      pool
      |> Bench.Otel.rows(rows, i * rows)
      |> Enum.map_join("\n", &JSON.encode!/1)
      |> Kernel.<>("\n")
    end
  end
end

Bench.DuckdbFlush.run()
