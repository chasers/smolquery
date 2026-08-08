defmodule Bench.BufferDuckDB do
  @moduledoc """
  What if the accumulator were a DuckDB table instead of GenServer state?

      MIX_ENV=prod mix run --no-start bench/buffer_duckdb.exs

  `Smolquery.BufferService.TableBuffer` keeps a flush's rows as column-major
  Elixir terms in `state.chunks`, zips them in `handoff/1`, and hands them to
  Polars. The alternative keeps the same GenServer and the same thresholds but
  accumulates into an in-memory DuckDB table, then flushes with one
  `COPY … ORDER BY … TO` — the statement `Smolquery.StorageService.Merge`
  already uses at seal.

  Both arms are measured over the full accumulate-then-flush cycle, because that
  is the unit the buffer actually pays. Splitting them would hide where the cost
  moved: DuckDB does more work per write and much less per flush.

  ## Arms

    * `terms + polars` — accumulate `k` column-major batches, `Enum.zip_with` them
      exactly as `merge_chunks/1` does, then `Writer.write/3`. Today.
    * `terms decode + polars` — the same, but each request is decoded and coerced
      from the NDJSON first, which is what `Validator` does. This is the arm that
      compares fairly with `duckdb`: both start from bytes on disk.
    * `duckdb` — `INSERT INTO buf SELECT * FROM read_json(body)` per request, then
      `COPY (SELECT * FROM buf ORDER BY …) TO segment` and `DELETE FROM buf`.
      No row becomes an Elixir term at any point.

  `k` is requests per flush, so it is the same axis `flush_max_bytes` controls.

  ## What this does not model

  The DuckDB arm reads each request from a file, which assumes the API spools the
  body instead of decoding it. That is the point — it is where the term cost
  disappears — but it is a different ingest contract, and it does no per-row
  validation and returns no per-index errors.

  Two facts from the code bound the design regardless of the numbers:

    * `Smolquery.Engine.Connection` is a GenServer around one ADBC connection, so
      it is a per-query mutex. A buffer sharing the query path's connection would
      serialize against user queries.
    * a fatal or internal DuckDB error invalidates the whole database, and
      `Connection` responds by killing it so the subtree rebuilds. A buffer
      sharing a database with the query path would lose unacked rows to somebody
      else's bad query. It needs its own instance.

  ## Memory

  Peak RSS is reported per arm, and the accumulation-only figure is measured
  separately: `k` batches inserted with no flush, RSS sampled before and after.
  That is the number that decides whether a bigger batch is affordable — the
  reason `flush_max_bytes` is where it is.
  """

  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @engine __MODULE__.Buffer
  @source "/tmp/smolquery-bodies/eachrow.3062.ndjson"
  @schema_file "scripts/k6/schema.json"
  @clustering ["project_id", "timestamp"]
  @row_group 16_384
  @order ~s("project_id" ASC NULLS LAST, "timestamp" ASC NULLS LAST)

  def run do
    {:ok, _apps} = Application.ensure_all_started(:explorer)
    {:ok, _apps} = Application.ensure_all_started(:adbc)
    {:ok, _pid} = Engine.start_link(name: @engine, max_result_rows: :infinity)

    duration = env_int("DURATION", 15) * 1000
    dir = System.get_env("OUT", "/tmp/buffer-duckdb")

    File.rm_rf!(dir)
    File.mkdir_p!(Path.join(dir, "polars"))
    File.mkdir_p!(Path.join(dir, "duckdb"))

    schema = %{load_schema() | clustering: @clustering}
    columns = load_columns(@source, schema)
    rows = columns |> hd() |> length()

    # Built once. Called per INSERT it would re-read and re-parse schema.json on
    # every write and charge that to DuckDB.
    scan = scan(schema)

    {:ok, _result} =
      Engine.query(@engine, "CREATE OR REPLACE TABLE buf AS SELECT * FROM #{scan} LIMIT 0")

    IO.puts("""

      request      #{rows} rows x #{length(columns)} columns
      sort         #{@order}
      duration     #{div(duration, 1000)}s per arm
    """)

    IO.puts("  #{pad("arm", 18)} #{lead("k", 3)} #{lead("rows/s", 10)} #{lead("flushes", 8)} #{lead("avg %CPU", 9)} #{lead("CPU s/Mrow", 11)} #{lead("bytes/row", 10)} #{lead("RSS", 9)}")

    for k <- [1, 4, 16] do
      terms(dir, columns, schema, duration, rows, k)
      decoded(dir, schema, duration, rows, k)
      duckdb(dir, scan, duration, rows, k)
    end

    accumulation(columns, rows, scan)

    sharded(dir, schema, scan, duration, rows, env_int("SHARDS", 4))
  end

  # Four shards of one table: each gets its own accumulator and writes its own
  # segments, which is what routing on {dataset, table, partition} would produce.
  #
  # Each DuckDB shard needs its own engine. `Engine.Connection` wraps one ADBC
  # connection in a GenServer, so shards sharing a connection would queue behind
  # each other and the run would measure the mutex.
  defp sharded(dir, schema, scan, duration, rows, workers) when workers > 1 do
    IO.puts("\n  #{workers} shards of one table, each with its own accumulator:\n")

    engines =
      Enum.map(1..workers, fn index ->
        name = Module.concat(@engine, "Shard#{index}")
        {:ok, _pid} = Engine.start_link(name: name, max_result_rows: :infinity)

        {:ok, _result} =
          Engine.query(name, "CREATE OR REPLACE TABLE buf AS SELECT * FROM #{scan} LIMIT 0")

        name
      end)

    IO.puts("  #{pad("arm", 22)} #{lead("k", 3)} #{lead("rows/s", 10)} #{lead("flushes", 8)} #{lead("avg %CPU", 9)} #{lead("CPU s/Mrow", 11)}")

    for k <- [1, 4] do
      shard_out = arm_dir(dir, "duckdb_shards", k)

      race("duckdb x#{workers}", duration, rows * k, fn shard ->
        engine = Enum.at(engines, shard - 1)
        path = Path.join(shard_out, "sh#{shard}_#{:erlang.unique_integer([:positive])}.parquet")

        Enum.each(1..k, fn _ ->
          {:ok, _r} = Engine.query(engine, "INSERT INTO buf SELECT * FROM #{scan}")
        end)

        {:ok, _r} =
          Engine.query(engine, """
          COPY (SELECT * FROM buf ORDER BY #{@order})
          TO '#{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE #{@row_group})
          """)

        {:ok, _r} = Engine.query(engine, "DELETE FROM buf")
      end, workers, k, shard_out)

      elixir_out = arm_dir(dir, "elixir_shards", k)

      race("elixir x#{workers}", duration, rows * k, fn shard ->
        store = Store.Local.new(dir: elixir_out)
        chunks = Enum.map(1..k, fn _ -> load_columns(@source, schema) end)
        merged = chunks |> Enum.reverse() |> Enum.zip_with(&Enum.concat/1)
        {:ok, _segment} = Writer.write({:columns, merged}, schema, store: store, compression: :zstd)
        _ = shard
      end, workers, k, elixir_out)
    end
  end

  defp sharded(_dir, _schema, _scan, _duration, _rows, _workers), do: :ok

  # Workers run the same cycle concurrently for a fixed window; throughput is the
  # sum, because a table's shards all feed the same table.
  defp race(label, duration, per_flush, work, workers, k, verify_dir) do
    :erlang.garbage_collect()

    started = System.monotonic_time(:millisecond)
    cpu_before = cpu_seconds()

    flushes =
      1..workers
      |> Task.async_stream(
        fn shard -> spin(work, shard, started, duration, 0) end,
        max_concurrency: workers,
        timeout: :infinity
      )
      |> Enum.reduce(0, fn {:ok, count}, total -> total + count end)

    wall = (System.monotonic_time(:millisecond) - started) / 1000
    cpu = cpu_seconds() - cpu_before
    total = flushes * per_flush

    IO.puts(
      "  #{pad(label, 22)} #{lead(k, 3)} #{lead(round(total / wall), 10)} #{lead(flushes, 8)}" <>
        " #{lead(fmt(cpu / wall * 100), 9)} #{lead(fmt(cpu / (total / 1_000_000)), 11)}" <>
        "  #{verified(verify_dir, total)}"
    )
  end

  # Reads the segments back and counts them. A throughput number nobody checked
  # against the bytes on disk is a count of function calls: an arm that wrote
  # nothing, or wrote empty files, would post the best result in the table.
  defp verified(dir, claimed) do
    case Engine.query(@engine, "SELECT count(*) FROM read_parquet('#{dir}/*.parquet')") do
      {:ok, %{rows: [[landed]]}} when landed == claimed ->
        "verified #{landed}"

      {:ok, %{rows: [[landed]]}} ->
        "MISMATCH: #{landed} on disk vs #{claimed} claimed"

      {:error, error} ->
        "unverifiable: #{String.slice(Exception.message(error), 0, 60)}"
    end
  end

  defp spin(work, shard, started, duration, count) do
    if System.monotonic_time(:millisecond) - started >= duration do
      count
    else
      work.(shard)
      spin(work, shard, started, duration, count + 1)
    end
  end

  defp terms(dir, columns, schema, duration, rows, k) do
    out = arm_dir(dir, "terms", k)
    store = Store.Local.new(dir: out)
    chunks = List.duplicate(columns, k)

    cycle(duration, fn _index ->
      # merge_chunks/1's own shape: newest chunk first, joined column-wise.
      merged = chunks |> Enum.reverse() |> Enum.zip_with(&Enum.concat/1)
      {:ok, segment} = Writer.write({:columns, merged}, schema, store: store, compression: :zstd)

      segment.byte_size
    end)
    |> emit("terms + polars", k, rows * k, out)
  end

  # Bytes in, segment out, with the decode and coercion the validator performs.
  # The only thing missing against a real request is the socket.
  defp decoded(dir, schema, duration, rows, k) do
    out = arm_dir(dir, "decoded", k)
    store = Store.Local.new(dir: out)

    cycle(duration, fn _index ->
      chunks = Enum.map(1..k, fn _ -> load_columns(@source, schema) end)
      merged = chunks |> Enum.reverse() |> Enum.zip_with(&Enum.concat/1)
      {:ok, segment} = Writer.write({:columns, merged}, schema, store: store, compression: :zstd)

      segment.byte_size
    end)
    |> emit("terms decode+polars", k, rows * k, out)
  end

  defp duckdb(dir, scan, duration, rows, k) do
    out = arm_dir(dir, "duckdb", k)

    cycle(duration, fn index ->
      path = Path.join(out, "seg_#{index}.parquet")

      Enum.each(1..k, fn _ ->
        {:ok, _result} = Engine.query(@engine, "INSERT INTO buf SELECT * FROM #{scan}")
      end)

      {:ok, _result} =
        Engine.query(@engine, """
        COPY (SELECT * FROM buf ORDER BY #{@order})
        TO '#{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE #{@row_group})
        """)

      {:ok, _result} = Engine.query(@engine, "DELETE FROM buf")

      File.stat!(path).size
    end)
    |> emit("duckdb", k, rows * k, out)
  end

  defp cycle(duration, flush) do
    :erlang.garbage_collect()

    started = System.monotonic_time(:millisecond)
    cpu_before = cpu_seconds()

    {count, bytes} = loop(flush, started, duration, 0, 0)

    %{
      flushes: count,
      bytes: bytes,
      wall: (System.monotonic_time(:millisecond) - started) / 1000,
      cpu: cpu_seconds() - cpu_before,
      rss: rss_mib()
    }
  end

  defp loop(flush, started, duration, count, bytes) do
    if System.monotonic_time(:millisecond) - started >= duration do
      {count, bytes}
    else
      loop(flush, started, duration, count + 1, bytes + flush.(count))
    end
  end

  defp arm_dir(dir, slug, k) do
    out = Path.join(dir, "#{slug}_k#{k}")
    File.mkdir_p!(out)

    out
  end

  defp emit(result, label, k, per_flush, out) do
    total = result.flushes * per_flush

    IO.puts(
      "  #{pad(label, 18)} #{lead(k, 3)} #{lead(round(total / result.wall), 10)}" <>
        " #{lead(result.flushes, 8)}" <>
        " #{lead(fmt(result.cpu / result.wall * 100), 9)}" <>
        " #{lead(fmt(result.cpu / (total / 1_000_000)), 11)}" <>
        " #{lead(fmt(result.bytes / total), 10)}" <>
        " #{lead("#{round(result.rss)} MiB", 9)}" <>
        "  #{verified(out, total)}"
    )
  end

  # What the accumulator costs to hold, which is what bounds batch size. Elixir
  # terms against a DuckDB table, same rows, no flush in either.
  defp accumulation(columns, rows, scan) do
    IO.puts("\n  holding #{16 * rows} rows without flushing:")

    :erlang.garbage_collect()
    before = rss_mib()

    # Genuinely distinct copies. `Enum.map(1..16, fn _ -> columns end)` would hold
    # sixteen references to one term and measure nothing, which is not what a
    # buffer holding sixteen requests has.
    held =
      Enum.map(1..16, fn _ ->
        columns |> :erlang.term_to_binary() |> :erlang.binary_to_term()
      end)

    :erlang.garbage_collect()
    terms_rss = rss_mib() - before
    _keep = length(held)

    :erlang.garbage_collect()
    before_duck = rss_mib()

    Enum.each(1..16, fn _ ->
      {:ok, _result} = Engine.query(@engine, "INSERT INTO buf SELECT * FROM #{scan}")
    end)

    duck_rss = rss_mib() - before_duck
    {:ok, _result} = Engine.query(@engine, "DELETE FROM buf")

    IO.puts("    terms   #{fmt(terms_rss)} MiB RSS delta  (#{fmt(terms_rss * 1_048_576 / (16 * rows))} bytes/row)")
    IO.puts("    duckdb  #{fmt(duck_rss)} MiB RSS delta  (#{fmt(duck_rss * 1_048_576 / (16 * rows))} bytes/row)")

    IO.puts("""

      RSS deltas are noisy — the BEAM does not return freed memory promptly and
      DuckDB keeps its own arenas. Read the ratio, not the absolute.
    """)
  end

  defp scan(schema) do
    spec =
      Enum.map_join(schema.fields, ", ", fn field ->
        "'#{field.name}': '#{sql_type(field.type)}'"
      end)

    "read_json('#{@source}', format='newline_delimited', columns={#{spec}})"
  end

  defp sql_type(:timestamp), do: "TIMESTAMP"
  defp sql_type(:int64), do: "BIGINT"
  defp sql_type(:float64), do: "DOUBLE"
  defp sql_type(:bool), do: "BOOLEAN"
  defp sql_type(_other), do: "VARCHAR"

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

    output |> String.trim() |> String.split(":") |> Enum.reduce(0.0, &(&2 * 60 + parse(&1)))
  end

  defp parse(text) do
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

Bench.BufferDuckDB.run()
