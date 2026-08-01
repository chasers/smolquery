Code.require_file("support.exs", __DIR__)

defmodule Bench.Buffer do
  @moduledoc """
  The hot tier's exit criterion: what group commit costs, and where it bends.

  Four questions, each settling a provisional decision from the milestone plan:

    * **Ack latency and throughput** — across batch size, concurrent writers, and
      table count. The number every other question is read against.
    * **Flush cadence** — `flush_interval_ms` is the ack-latency dial; this shows
      what turning it costs in throughput.
    * **The two fsyncs (D3)** — `Store.Local`'s segment fsync and the manifest
      log's fsync are what makes an ack durable. This prices them, isolated from
      everything else group commit does.
    * **The inline-flush ceiling (D6)** — one table's throughput is capped at one
      Polars encode per cycle. This is the number that decides whether
      double-buffering is worth building.

  Rows are two columns (`id`, `ts`) rather than `Bench.Support.schema/0`'s four,
  so the numbers measure group commit and the store, not `Decimal` construction.
  MB/s is measured on the in-memory batch (`:erlang.external_size/1`), the same
  measure the buffer's byte bound uses — not the encoded Parquet. Every cell
  warms up with one untimed write per table, so lazy buffer start, manifest
  recovery, and NIF warmup stay out of the tail percentiles.

      mix run bench/buffer.exs
      CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs

  ## What this measured, and what it settled

  On the machine this was last run on:

  - **Ack latency is `flush_interval_ms` plus a couple of milliseconds — and
    nothing else moves it.** With the interval held at 25 ms, p50 sat at
    30-35 ms across every batch size (1-500 rows), writer count (1-32), and
    table count (1-4) tried. Throughput scaled with all three instead
    (32 writers × 500-row batches reached ~450K rows/s); latency did not, which
    is the whole point of group commit — a client pays the flush cadence, not
    the load on it.
  - **Flush cadence is exactly the dial it's documented as.** 10 ms →
    ~51K rows/s at ~16 ms p50 ack; 1000 ms → ~800 rows/s at ~1010 ms p50 —
    linear in between. Pick the interval by the ack-latency budget, not by
    guessing.
  - **D3: the two fsyncs cost about 2 ms together, not the bottleneck.**
    An open+write+fsync+close of a 4 KiB file runs ~0.3-0.7 ms; a full group
    commit with the segment store's fsync on ran ~2.0 ms p50 versus ~1.7 ms
    with it off (down from ~2.8 ms before the manifest log held its fd open
    across appends) — so the segment fsync's own marginal cost is
    sub-millisecond, and the manifest append pays only its write and fsync.
    Either way, both fsyncs together are an order of magnitude under any
    `flush_interval_ms` worth running. D3's accepted durability window costs
    single-digit milliseconds, not the ack.
  - **D6: no ceiling found through 256 concurrent writers on one table.**
    Throughput scaled linearly — 628 → 2,305 → 10,133 → 38,834 → 148,205 rows/s
    at 1, 4, 16, 64, 256 writers — with p50 ack staying flat near
    `flush_interval_ms`. Inline flush stays; double-buffering has no evidence
    behind it yet. Re-run this at a higher writer count, or after a schema with
    a heavier encode, before revisiting.

  """

  import Bench.Support

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store

  @dataset "bench"

  def run do
    with_tmp_dir("buffer", fn dir ->
      throughput_and_latency(dir)
      flush_cadence(dir)
      fsync_cost(dir)
      inline_flush_ceiling(dir)
    end)
  end

  defp throughput_and_latency(dir) do
    heading("ack latency and throughput: batch size x concurrent writers x table count")

    batch_sizes = Enum.filter([1, 50, 500], &(&1 <= env("MAX_BATCH", 500)))
    writer_counts = Enum.filter([1, 8, 32], &(&1 <= env("MAX_WRITERS", 32)))
    table_counts = Enum.filter([1, 4], &(&1 <= env("MAX_TABLES", 4)))
    calls = env("CALLS", 20)

    IO.puts(
      "\n  batch  writers  tables    batches/s      rows/s      MB/s      p50     p95     p99  (ms)"
    )

    for tables <- table_counts, writers <- writer_counts, size <- batch_sizes do
      {name, pid} = start_buffer(dir, flush_interval_ms: 25, flush_max_rows: 1_000_000)
      refs = for t <- 1..tables, do: {@dataset, "events#{t}"}

      {wall_us, latencies} = hammer(name, refs, writers, calls, size)

      stop_buffer(name, pid)

      report(
        [pad(size, 5), pad(writers, 7), pad(tables, 6)],
        latencies,
        wall_us,
        writers * calls,
        size
      )
    end
  end

  defp flush_cadence(dir) do
    heading("flush cadence: the ack-latency dial (Runtime.flush_interval_ms)")

    writers = env("WRITERS", 16)
    calls = env("CALLS", 20)
    size = env("BATCH", 50)

    IO.puts("\n  flush_ms    batches/s      rows/s      MB/s      p50     p95     p99  (ms)")

    for interval <- [10, 25, 100, 250, 1_000] do
      {name, pid} = start_buffer(dir, flush_interval_ms: interval, flush_max_rows: 1_000_000)

      {wall_us, latencies} = hammer(name, [{@dataset, "events"}], writers, calls, size)

      stop_buffer(name, pid)

      report([pad(interval, 8)], latencies, wall_us, writers * calls, size)
    end
  end

  defp fsync_cost(dir) do
    heading("the cost of the two fsyncs (D3): the segment put, the manifest append")

    raw = raw_fsync()

    IO.puts(
      "\n  an open+write+fsync+close of a 4 KiB file: #{ms(raw.min)} ms min, #{ms(raw.median)} ms median"
    )

    IO.puts("  the segment put pays one of these per flush; the manifest append")
    IO.puts("  holds its fd open and pays only the write and the fsync\n")

    IO.puts("  store fsync    p50     p95     p99  (ms of one flush = one batch)")

    for fsync? <- [true, false] do
      store = Store.Local.new(dir: Path.join(dir, "fsync-#{fsync?}/segments"), fsync: fsync?)

      {name, pid} =
        start_buffer(dir,
          store: store,
          log_dir: Path.join(dir, "fsync-#{fsync?}/manifests"),
          flush_interval_ms: 60_000,
          flush_max_rows: 1
        )

      {_wall_us, latencies} = hammer(name, [{@dataset, "events"}], 1, env("CALLS", 30), 1)

      stop_buffer(name, pid)

      sorted = Enum.sort(latencies)

      IO.puts(
        "  #{pad(fsync?, 11)}  #{pad(ms(percentile(sorted, 0.50)), 6)}  " <>
          "#{pad(ms(percentile(sorted, 0.95)), 6)}  #{pad(ms(percentile(sorted, 0.99)), 6)}"
      )
    end
  end

  defp inline_flush_ceiling(dir) do
    heading("the inline-flush ceiling (D6): one table, one Polars encode per cycle")

    writer_counts = Enum.filter([1, 4, 16, 64, 256], &(&1 <= env("MAX_WRITERS", 256)))
    calls = env("CALLS", 20)
    size = env("BATCH", 20)

    IO.puts("\n  writers      rows/s      MB/s     p50 ack     p99 ack  (ms)")

    for writers <- writer_counts do
      {name, pid} = start_buffer(dir, flush_interval_ms: 25, flush_max_rows: 1_000_000)

      {wall_us, latencies} = hammer(name, [{@dataset, "events"}], writers, calls, size)

      stop_buffer(name, pid)

      sorted = Enum.sort(latencies)
      seconds = wall_us / 1_000_000
      rows_per_sec = Float.round(writers * calls * size / seconds, 1)
      mb_per_sec = megabytes_per_second(writers * calls, size, seconds)

      IO.puts(
        "  #{pad(writers, 7)}  #{pad(rows_per_sec, 10)}  #{pad(mb_per_sec, 8)}  " <>
          "#{pad(ms(percentile(sorted, 0.50)), 10)}  #{pad(ms(percentile(sorted, 0.99)), 10)}"
      )
    end

    IO.puts("\n  a plateau here is the ceiling one table's group commit can serve;")
    IO.puts("  more tables scale past it (each gets its own TableBuffer), one table does not.")
  end

  defp start_buffer(dir, opts) do
    name = Module.concat(__MODULE__, "Buffer#{System.unique_integer([:positive])}")

    defaults = [
      name: name,
      dir: Path.join(dir, "instance-#{System.unique_integer([:positive])}"),
      hot_server_port: 0
    ]

    {:ok, pid} = BufferService.Supervisor.start_link(Keyword.merge(defaults, opts))

    {name, pid}
  end

  defp stop_buffer(name, pid) do
    Supervisor.stop(pid)
    Runtime.delete(name)
  end

  defp hammer(name, table_refs, writers, calls, size) do
    warmup(name, table_refs, size)
    tables = length(table_refs)

    :timer.tc(fn ->
      1..writers
      |> Task.async_stream(
        fn writer ->
          for call <- 1..calls do
            table_ref = Enum.at(table_refs, rem(writer + call, tables))
            offset = writer * 10_000_000 + call * size

            {us, {:ok, _ack}} =
              :timer.tc(fn -> Client.write_batch(name, table_ref, batch(size, offset)) end)

            us
          end
        end,
        max_concurrency: writers,
        timeout: 60_000
      )
      |> Enum.flat_map(fn {:ok, times} -> times end)
    end)
  end

  defp warmup(name, table_refs, size) do
    Enum.each(table_refs, fn table_ref ->
      {:ok, _ack} = Client.write_batch(name, table_ref, batch(size, 0))
    end)
  end

  defp batch_bytes(size), do: :erlang.external_size(batch(size, 0).rows)

  defp megabytes_per_second(batches, size, seconds),
    do: Float.round(batches * batch_bytes(size) / 1_000_000 / seconds, 2)

  defp row_schema, do: Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])

  defp batch(size, offset) do
    rows =
      for i <- 1..size do
        %{"id" => offset + i, "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], offset + i)}
      end

    %{schema: row_schema(), rows: rows}
  end

  defp raw_fsync do
    path =
      Path.join(System.tmp_dir!(), "smolquery-bench-fsync-#{System.unique_integer([:positive])}")

    bytes = :crypto.strong_rand_bytes(4_096)

    try do
      timed(
        fn ->
          {:ok, fd} = :file.open(path, [:write, :raw, :binary])
          :ok = :file.write(fd, bytes)
          :ok = :file.sync(fd)
          :ok = :file.close(fd)
        end,
        50
      )
    after
      File.rm(path)
    end
  end

  defp percentile(sorted, p) do
    index = min(length(sorted) - 1, trunc(p * length(sorted)))
    Enum.at(sorted, index)
  end

  defp report(columns, latencies, wall_us, batches, size) do
    sorted = Enum.sort(latencies)
    seconds = wall_us / 1_000_000

    IO.puts(
      "  #{Enum.join(columns, "  ")}  #{pad(Float.round(batches / seconds, 1), 9)}  " <>
        "#{pad(Float.round(batches * size / seconds, 1), 10)}  " <>
        "#{pad(megabytes_per_second(batches, size, seconds), 8)}  " <>
        "#{pad(ms(percentile(sorted, 0.50)), 6)}  " <>
        "#{pad(ms(percentile(sorted, 0.95)), 6)}  #{pad(ms(percentile(sorted, 0.99)), 6)}"
    )
  end
end

Bench.Buffer.run()
