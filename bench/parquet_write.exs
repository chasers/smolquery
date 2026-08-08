defmodule Bench.ParquetWrite do
  @moduledoc """
  How fast Elixir writes Parquet, through the path the write tier actually uses.

  `Smolquery.Segments.Writer.write/3` is the terminal step of every write in the
  system: a batch becomes an `Explorer.DataFrame` and Polars encodes the file in
  Rust. This measures that step alone — no socket, no validator, no buffer, no
  DuckDB. Whatever the HTTP arms achieve, they cannot beat what is here.

  It is the Elixir counterpart of `scripts/duckdb`, which measured the same rows
  going into a DuckDB file from Go. Same fixture, same columns, so the two
  numbers sit next to each other:

      MIX_ENV=prod mix run bench/parquet_write.exs

  ## What it varies

    * **batch size** — 1x, 4x, 16x and 64x the 3062-row body. The DuckDB arm
      found that per-commit overhead, not per-row work, set its ceiling, and
      four times the rows per call cost far less than four calls. Parquet has no
      commit, but it does have a footer, row-group statistics and a file per
      call, so the same question has to be asked rather than assumed.
    * **codec** — the default `:zstd` against `:snappy` and no compression. The
      write path pays this on every segment and the reader pays it back on every
      scan.
    * **the clustering sort** — `Writer` sorts by the clustering key before
      encoding, so row-group stats are tight enough to prune on. That sort is
      real work on every batch and is measured separately, because the DuckDB
      arm did no such thing and the comparison would otherwise be unfair in
      smolquery's favour.

  ## Measuring

  CPU comes from `ps -o time=` on this OS process, the same source
  `scripts/k6/watch.go` uses, because BEAM's own `:erlang.statistics(:runtime)`
  does not see the Rust threads Polars encodes on — which is most of the work
  here. Reporting scheduler time would report roughly zero and be worse than not
  measuring at all.
  """

  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @source "/tmp/smolquery-bodies/eachrow.3062.ndjson"
  @schema_file "scripts/k6/schema.json"
  @clustering ["project_id", "timestamp"]

  def run do
    # Run with `--no-start`: booting smolquery would put the buffer service, the
    # sealer and their timers on the same cores as the thing being measured, and
    # the API refuses to start without a key it has no use for here. Only
    # Explorer is needed, so only Explorer is started.
    {:ok, _started} = Application.ensure_all_started(:explorer)

    duration = env_int("DURATION", 20) * 1000
    dir = System.get_env("OUT", "/tmp/parquet-bench")

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    schema = load_schema()
    clustered = %{schema | clustering: @clustering}
    columns = load_columns(@source, schema)
    rows = columns |> hd() |> length()
    store = Store.Local.new(dir: dir)

    IO.puts("""

      source       #{@source}
      batch        #{rows} rows x #{length(columns)} columns
      out          #{dir}
      duration     #{div(duration, 1000)}s per arm
      schedulers   #{System.schedulers_online()}
    """)

    results =
      [
        {"zstd x1, sorted", 1, :zstd, clustered},
        {"zstd x4, sorted", 4, :zstd, clustered},
        {"zstd x16, sorted", 16, :zstd, clustered},
        {"zstd x64, sorted", 64, :zstd, clustered},
        {"zstd x16, unsorted", 16, :zstd, schema},
        {"snappy x16, sorted", 16, :snappy, clustered},
        # Explorer spells "no compression" as nil; :uncompressed raises.
        {"none x16, sorted", 16, nil, clustered}
      ]
      |> Enum.map(fn {label, multiple, codec, used} ->
        arm(label, repeat(columns, multiple), used, codec, store, duration, rows * multiple)
      end)

    report(results)
  end

  # One arm: write segments for `duration` and report what it cost. A failed
  # codec is reported rather than raised — an unsupported name should not take
  # the arms that would have worked with it.
  defp arm(label, columns, schema, codec, store, duration, rows) do
    IO.puts("  #{label} ...")

    :erlang.garbage_collect()

    started = System.monotonic_time(:millisecond)
    cpu_before = cpu_seconds()

    outcome = loop({:columns, columns}, schema, store, codec, started, duration, 0, 0)

    wall = System.monotonic_time(:millisecond) - started

    case outcome do
      {:error, reason} ->
        %{label: label, error: inspect(reason)}

      {batches, bytes} ->
        %{
          label: label,
          rows: batches * rows,
          batches: batches,
          wall: wall / 1000,
          cpu: cpu_seconds() - cpu_before,
          rss: rss_mib(),
          bytes: bytes
        }
    end
  end

  defp loop(batch, schema, store, codec, started, duration, batches, bytes) do
    if System.monotonic_time(:millisecond) - started >= duration do
      {batches, bytes}
    else
      case Writer.write(batch, schema, store: store, compression: codec) do
        {:ok, segment} ->
          loop(
            batch,
            schema,
            store,
            codec,
            started,
            duration,
            batches + 1,
            bytes + segment.byte_size
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # A larger batch of the same rows. Repeating the fixture keeps the value
  # distribution identical across batch sizes, so a bigger batch does not also
  # become a differently compressible one.
  defp repeat(columns, 1), do: columns

  defp repeat(columns, multiple) do
    Enum.map(columns, fn values ->
      Enum.flat_map(1..multiple, fn _ -> values end)
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
  defp type(_), do: :string

  # The bodies are read and transposed once, up front. Doing it inside the
  # measured loop would put JSON decoding on the critical path and report it as
  # Polars' cost, which is the mistake this whole harness exists to avoid.
  defp load_columns(path, schema) do
    fields = schema.fields

    rows =
      path
      |> File.stream!()
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Enum.map(&JSON.decode!/1)

    Enum.map(fields, fn field ->
      Enum.map(rows, &coerce(Map.get(&1, field.name), field.type))
    end)
  end

  defp coerce(nil, _type), do: nil
  defp coerce(value, :int64) when is_float(value), do: trunc(value)
  defp coerce(value, :float64) when is_integer(value), do: value * 1.0

  defp coerce(value, :timestamp) when is_binary(value) do
    # generate.js writes ClickHouse's spelling into this file: a space instead
    # of the T, and no zone. Same instants as the smolquery bodies.
    {:ok, at} = NaiveDateTime.from_iso8601(String.replace(value, " ", "T"))

    at
  end

  defp coerce(value, _type), do: value

  defp report(results) do
    IO.puts("""

      #{pad("arm", 22)} #{lead("rows/s", 10)} #{lead("segments", 9)} #{lead("avg %CPU", 9)} #{lead("CPU s/Mrow", 11)} #{lead("bytes/row", 10)}\
    """)

    Enum.each(results, fn
      %{error: reason, label: label} ->
        IO.puts("  #{pad(label, 22)} #{reason}")

      result ->
        IO.puts(
          "  #{pad(result.label, 22)} #{lead(round(result.rows / result.wall), 10)}" <>
            " #{lead(result.batches, 9)}" <>
            " #{lead(fmt(result.cpu / result.wall * 100), 9)}" <>
            " #{lead(fmt(result.cpu / (result.rows / 1_000_000)), 11)}" <>
            " #{lead(fmt(result.bytes / result.rows), 10)}"
        )
    end)

    rss = results |> Enum.map(&Map.get(&1, :rss, 0)) |> Enum.max()

    IO.puts("""

      peak RSS     #{round(rss)} MiB
      %CPU is per core; #{System.schedulers_online()}00 is every scheduler.

      CPU is this OS process, Polars' Rust threads included. BEAM scheduler time
      would miss them and report a fraction of the real cost.

      bytes/row is only honest on the x1 arm. Larger batches repeat the same
      3062 rows, and zstd finds those copies — the falling size with batch size
      is the duplication compressing, not a property of the format. Codecs at
      the same multiple still compare with each other.
    """)
  end

  # Accumulated CPU time for this process, as [[dd-]hh:]mm:ss[.cc].
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

  defp rss_mib do
    {output, 0} = System.cmd("ps", ["-o", "rss=", "-p", System.pid()])

    output |> String.trim() |> String.to_integer() |> Kernel./(1024)
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp fmt(number), do: :erlang.float_to_binary(number * 1.0, decimals: 1)

  defp pad(text, width), do: String.pad_trailing(to_string(text), width)

  defp lead(text, width), do: String.pad_leading(to_string(text), width)
end

Bench.ParquetWrite.run()
