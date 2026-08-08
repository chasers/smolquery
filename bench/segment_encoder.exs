defmodule Bench.SegmentEncoder do
  @moduledoc """
  Polars against DuckDB for the one job `Smolquery.Segments.Writer` does: turn a
  flush into one sorted Parquet segment.

      MIX_ENV=prod mix run --no-start bench/segment_encoder.exs

  Three arms, because "replace Polars with DuckDB" is two different changes and
  they have to be priced separately.

    * `polars` — `Writer.write({:columns, …}, schema, store: …)`, unchanged. Input
      is a column-major Elixir batch, which is what the validator produces.
    * `duckdb encode` — `COPY (SELECT * FROM t ORDER BY …) TO …` from a table
      already resident in DuckDB. Same rows, same sort, same codec, nothing
      parsed. This is the encoder-for-encoder comparison and the only one of the
      three that isolates it.
    * `duckdb parse+encode` — the same `COPY`, but reading the request's own
      NDJSON off disk with `read_json`. No Elixir term is built for any row at
      all. This is what "throw Polars out" would actually look like at the
      architecture level: spool the body, let DuckDB do the rest.

  The third arm is not a drop-in for the first. It does no per-row validation and
  reports no per-index errors, which `Smolquery.IngestService.Validator` does and
  the API contract promises. Treat its number as a ceiling for a redesign, not as
  a swap.

  ## Row groups are the second question

  Pruning depends on row-group statistics, and the two encoders do not agree
  about them. `Writer` calls `Explorer.DataFrame.to_parquet/3`, which exposes no
  row-group size, so Polars uses its own default; `Smolquery.StorageService.Merge`
  passes `ROW_GROUP_SIZE #{16_384}` explicitly. Each arm's output is therefore
  inspected with `parquet_metadata` afterwards — group count, whether statistics
  are present at all, and how tight the clustering column's bounds are.

  ## Measuring

  CPU is `ps -o time=` on this OS process, as in `bench/parquet_write.exs`:
  Polars encodes on Rust threads and DuckDB on its own, and BEAM scheduler time
  sees neither.
  """

  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @engine __MODULE__.Lake
  @source "/tmp/smolquery-bodies/eachrow.3062.ndjson"
  @schema_file "scripts/k6/schema.json"
  @clustering ["project_id", "timestamp"]
  @row_group 16_384
  @order ~s("project_id" ASC NULLS LAST, "timestamp" ASC NULLS LAST)

  def run do
    {:ok, _apps} = Application.ensure_all_started(:explorer)
    {:ok, _apps} = Application.ensure_all_started(:adbc)
    {:ok, _pid} = Engine.start_link(name: @engine, max_result_rows: :infinity)

    duration = env_int("DURATION", 20) * 1000
    dir = System.get_env("OUT", "/tmp/segment-encoder")

    File.rm_rf!(dir)
    File.mkdir_p!(Path.join(dir, "polars"))
    File.mkdir_p!(Path.join(dir, "duckdb"))

    schema = %{load_schema() | clustering: @clustering}
    columns = load_columns(@source, schema)
    rows = columns |> hd() |> length()

    load_into_duckdb(schema)

    IO.puts("""

      source       #{@source}
      batch        #{rows} rows x #{length(columns)} columns
      sort         #{@order}
      codec        zstd on both sides
      duration     #{div(duration, 1000)}s per arm
    """)

    results = [
      polars(dir, columns, schema, duration, rows),
      duckdb("duckdb encode", dir, "batch", duration, rows),
      duckdb("duckdb parse+encode", dir, json_scan(schema), duration, rows)
    ]

    report(results)
    row_groups(results)
  end

  defp polars(dir, columns, schema, duration, rows) do
    store = Store.Local.new(dir: Path.join(dir, "polars"))
    batch = {:columns, columns}

    measure("polars", rows, duration, fn _index ->
      {:ok, segment} = Writer.write(batch, schema, store: store, compression: :zstd)

      {segment.byte_size, segment.path}
    end)
  end

  defp duckdb(label, dir, source, duration, rows) do
    measure(label, rows, duration, fn index ->
      path = Path.join([dir, "duckdb", "#{slug(label)}_#{index}.parquet"])

      sql = """
      COPY (SELECT * FROM #{source} ORDER BY #{@order})
      TO '#{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE #{@row_group})
      """

      {:ok, _result} = Engine.query(@engine, sql)

      {File.stat!(path).size, path}
    end)
  end

  # Every arm writes real segments for a fixed wall-clock window and keeps the
  # last path, so the row-group inspection afterwards reads a file this run
  # actually produced rather than a file written to be inspected.
  defp measure(label, rows, duration, write) do
    :erlang.garbage_collect()

    started = System.monotonic_time(:millisecond)
    cpu_before = cpu_seconds()

    {count, bytes, last} = loop(write, started, duration, 0, 0, nil)

    %{
      label: label,
      rows: count * rows,
      segments: count,
      wall: (System.monotonic_time(:millisecond) - started) / 1000,
      cpu: cpu_seconds() - cpu_before,
      bytes: bytes,
      sample: last
    }
  end

  defp loop(write, started, duration, count, bytes, last) do
    if System.monotonic_time(:millisecond) - started >= duration do
      {count, bytes, last}
    else
      {size, path} = write.(count)

      loop(write, started, duration, count + 1, bytes + size, path)
    end
  end

  defp load_into_duckdb(schema) do
    {:ok, _result} =
      Engine.query(@engine, "CREATE OR REPLACE TABLE batch AS SELECT * FROM #{json_scan(schema)}")

    :ok
  end

  defp json_scan(schema) do
    columns =
      Enum.map_join(schema.fields, ", ", fn field ->
        "'#{field.name}': '#{sql_type(field.type)}'"
      end)

    "read_json('#{@source}', format='newline_delimited', columns={#{columns}})"
  end

  defp sql_type(:timestamp), do: "TIMESTAMP"
  defp sql_type(:int64), do: "BIGINT"
  defp sql_type(:float64), do: "DOUBLE"
  defp sql_type(:bool), do: "BOOLEAN"
  defp sql_type(_other), do: "VARCHAR"

  # Asked of DuckDB, which is also what reads segments on the query path, so this
  # is the pruning decision itself and not a description of it.
  defp row_groups(results) do
    IO.puts(
      "\n  #{pad("arm", 22)} #{lead("groups", 7)} #{lead("rows/group", 11)} #{lead("stats?", 7)}  clustering bounds"
    )

    Enum.each(results, fn result ->
      sql = """
      SELECT count(*), coalesce(max(row_group_num_rows), 0), count(stats_min),
             coalesce(min(stats_min), '-'), coalesce(max(stats_max), '-')
      FROM parquet_metadata('#{result.sample}')
      WHERE path_in_schema = 'project_id'
      """

      case Engine.query(@engine, sql) do
        {:ok, %{rows: [[groups, per_group, with_stats, low, high]]}} ->
          IO.puts(
            "  #{pad(result.label, 22)} #{lead(groups, 7)} #{lead(per_group, 11)} #{lead(with_stats, 7)}  #{low}..#{high}"
          )

        other ->
          IO.puts("  #{pad(result.label, 22)} inspection failed: #{inspect(other)}")
      end
    end)

    IO.puts("""

      groups is row groups in one segment; a segment with one group can prune
      nothing inside itself, whatever its statistics say.
    """)
  end

  defp report(results) do
    IO.puts(
      "  #{pad("arm", 22)} #{lead("rows/s", 10)} #{lead("segments", 9)} #{lead("avg %CPU", 9)} #{lead("CPU s/Mrow", 11)} #{lead("bytes/row", 10)}"
    )

    Enum.each(results, fn result ->
      IO.puts(
        "  #{pad(result.label, 22)} #{lead(round(result.rows / result.wall), 10)}" <>
          " #{lead(result.segments, 9)}" <>
          " #{lead(fmt(result.cpu / result.wall * 100), 9)}" <>
          " #{lead(fmt(result.cpu / (result.rows / 1_000_000)), 11)}" <>
          " #{lead(fmt(result.bytes / result.rows), 10)}"
      )
    end)
  end

  defp load_schema do
    @schema_file
    |> File.read!()
    |> JSON.decode!()
    |> Map.fetch!("schema")
    |> Enum.map(&{&1["name"], type(&1["type"])})
    |> Schema.new!()
  end

  defp type("TIMESTAMP"), do: :timestamp
  defp type("INT64"), do: :int64
  defp type("FLOAT64"), do: :float64
  defp type("BOOL"), do: :bool
  defp type(_other), do: :string

  defp load_columns(path, schema) do
    rows =
      path
      |> File.stream!()
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Enum.map(&JSON.decode!/1)

    Enum.map(schema.fields, fn field ->
      Enum.map(rows, &coerce(Map.get(&1, field.name), field.type))
    end)
  end

  defp coerce(nil, _type), do: nil
  defp coerce(value, :int64) when is_float(value), do: trunc(value)
  defp coerce(value, :float64) when is_integer(value), do: value * 1.0

  defp coerce(value, :timestamp) when is_binary(value) do
    {:ok, at} = NaiveDateTime.from_iso8601(String.replace(value, " ", "T"))

    at
  end

  defp coerce(value, _type), do: value

  defp cpu_seconds do
    {output, 0} = System.cmd("ps", ["-o", "time=", "-p", System.pid()])

    output
    |> String.trim()
    |> String.split("-")
    |> case do
      [clock] -> parse_clock(clock)
      [days, clock] -> String.to_integer(days) * 86_400 + parse_clock(clock)
    end
  end

  defp parse_clock(clock) do
    clock
    |> String.split(":")
    |> Enum.reduce(0.0, fn part, total -> total * 60 + parse_number(part) end)
  end

  defp parse_number(text) do
    case Float.parse(text) do
      {value, _rest} -> value
      :error -> 0.0
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp slug(label), do: label |> String.replace(~r/[^a-z]+/, "_") |> String.trim("_")
  defp fmt(number), do: :erlang.float_to_binary(number * 1.0, decimals: 1)
  defp pad(text, width), do: String.pad_trailing(to_string(text), width)
  defp lead(text, width), do: String.pad_leading(to_string(text), width)
end

Bench.SegmentEncoder.run()
