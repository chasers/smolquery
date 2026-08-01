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
      double-buffering is worth building, and the number partitioned writes
      (PL-6) have to beat.

  D6 comes in three parts, because "one table plateaus at N rows/s" is only
  actionable once you know *what* plateaued:

    * **the sweep** — writers against rows/s, for a light schema and a heavy one,
      carrying the two numbers that explain a plateau: how many flushes actually
      happened (each flush is one segment in the manifest, so the manifest counts
      them) and how deep the buffer's mailbox got.
    * **the encode in isolation** — `Writer.write` timed off to the side, across
      the rows-per-flush the sweep produced. One table's structural ceiling is
      `rows_per_flush / max(encode, flush_interval_ms)`: while an encode finishes
      inside the interval, cadence sets the ack; the moment it does not, the
      encode does.
    * **the fsyncs at the ceiling** — D3's store-fsync toggle re-run at the top of
      the writer sweep, to show whether the plateau is the fsync or the encode.

  Rows are two columns (`id`, `ts`) rather than `Bench.Support.schema/0`'s four,
  so the numbers measure group commit and the store, not `Decimal` construction.
  The D6 sweep runs both: `light` is those two columns, `heavy` is the four-column
  schema with a `Decimal`, which is what makes the encode do real work.
  MB/s is measured on the in-memory batch (`:erlang.external_size/1`), the same
  measure the buffer's byte bound uses — not the encoded Parquet. Every cell
  warms up with one untimed write per table, so lazy buffer start, manifest
  recovery, and NIF warmup stay out of the tail percentiles.

      mix run bench/buffer.exs
      CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs

  ## What this measured, and what it settled

  The last full run — its numbers, machine, and tables — is in
  `bench/results/buffer.md`. The short version:

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
  - **D6: still no plateau at 1024 writers — and writer count was never going to
    find one.** 582 → 2,179 → 8,968 → 32,983 → 145,944 → 492,904 rows/s at 1, 4,
    16, 64, 256, 1024 writers, and the heavy schema tracks it within 13%. The
    reason is in the `flushes` column: it is 20 in every cell. Group commit turns
    added writers into a *wider* flush, not more flushes, so rows/s climbs with
    rows-per-flush (20 → 20,480) while the cadence holds.
  - **What bends is the encode, structurally rather than at a writer count.**
    Throughput is `rows_per_flush / max(encode, flush_interval_ms)`; the ceiling
    is where one encode outgrows the interval. Timed off to the side, that is
    ~100K rows/flush light (29.4 ms) and ~50K heavy (30.4 ms) — a ceiling of
    **~2.0-3.4M rows/s light, ~1.7M heavy** on one table, needing thousands of
    concurrent 20-row writers or hundreds of 500-row ones to reach.
  - **Not the fsync, and not the mailbox.** Toggling the segment fsync at 1024
    writers moves throughput ±6%, and the wrong way for the heavy schema — noise.
    Mailbox depth peaks below the writer count (809 of 1024) because each writer
    has one outstanding call, so the buffer never falls behind its own inbox.
  - **So partitioning (PL-6) is the right multiplier, and less urgent than it was
    written to be.** P TableBuffers give P parallel encodes, which is the quantity
    that binds. But PL-6 was drafted against "148K rows/s, no plateau found"; the
    real wall is an order of magnitude past that. Double-buffering is worth ~7 ms
    of a 41.5 ms cycle today, and past the crossover partitioning is the better
    lever — so inline flush stays.

  """

  import Bench.Support

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @dataset "bench"

  def run do
    with_tmp_dir("buffer", fn dir ->
      throughput_and_latency(dir)
      flush_cadence(dir)
      fsync_cost(dir)
      inline_flush_ceiling(dir)
      encode_in_isolation(dir)
      fsyncs_at_the_ceiling(dir)
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

    writer_counts = Enum.filter([1, 4, 16, 64, 256, 1024], &(&1 <= env("MAX_WRITERS", 1024)))
    calls = env("CALLS", 20)
    size = env("BATCH", 20)

    IO.puts(
      "\n  schema  writers      rows/s      MB/s   flushes   rows/flush   mailbox" <>
        "     p50 ack     p99 ack  (ms)"
    )

    for weight <- [:light, :heavy], writers <- writer_counts do
      {name, pid} = start_buffer(dir, flush_interval_ms: 25, flush_max_rows: 1_000_000)
      table_ref = {@dataset, "events"}

      {{wall_us, latencies}, mailbox} =
        with_mailbox_sampler(name, table_ref, fn ->
          hammer(name, [table_ref], writers, calls, size, weight)
        end)

      flushes = flush_count(name, table_ref)

      stop_buffer(name, pid)

      sorted = Enum.sort(latencies)
      seconds = wall_us / 1_000_000
      batches = writers * calls
      rows = batches * size

      IO.puts(
        "  #{label(weight, 6)}  #{pad(writers, 7)}  " <>
          "#{pad(round(rows / seconds), 10)}  " <>
          "#{pad(megabytes_per_second(batches, size, seconds, weight), 8)}  " <>
          "#{pad(flushes, 7)}  #{pad(per_flush(rows, flushes), 11)}  #{pad(mailbox, 7)}  " <>
          "#{pad(ms(percentile(sorted, 0.50)), 10)}  #{pad(ms(percentile(sorted, 0.99)), 10)}"
      )
    end

    IO.puts(
      "\n  flushes counts segments in the manifest — one per group commit, warmup excluded."
    )

    IO.puts("  rows/flush is what one encode had to swallow; mailbox is the buffer's deepest")
    IO.puts("  queue during the run. rows/s plateauing while rows/flush keeps climbing means")
    IO.puts("  the encode, not the cadence, is now setting the flush rate.")
  end

  defp encode_in_isolation(dir) do
    heading("what one encode costs (D6): Writer.write against rows per flush, off to the side")

    interval = 25
    store = Store.Local.new(dir: Path.join(dir, "encode/segments"))
    sizes = [100, 1_000, 10_000, 50_000, 100_000]

    IO.puts("\n  the flush cadence holds while one encode fits inside flush_interval_ms")
    IO.puts("  (#{interval} ms here). Past that, rows/s = rows_per_flush / encode.\n")

    IO.puts(
      "  schema     rows   encode min   encode med   fits #{interval}ms      ceiling rows/s"
    )

    for weight <- [:light, :heavy], rows <- sizes do
      %{rows: batch_rows, schema: schema} = batch(rows, 0, weight)

      encode =
        timed(fn -> {:ok, _segment} = Writer.write(batch_rows, schema, store: store) end, 5)

      cycle = max(encode.median, interval * 1_000)

      IO.puts(
        "  #{label(weight, 6)}  #{pad(rows, 7)}  #{pad(ms(encode.min), 11)}  " <>
          "#{pad(ms(encode.median), 11)}  #{pad(encode.median <= interval * 1_000, 9)}  " <>
          "#{pad(round(rows / (cycle / 1_000_000)), 18)}"
      )
    end
  end

  defp fsyncs_at_the_ceiling(dir) do
    heading("the fsyncs at the ceiling (D6): is the plateau the encode or the store fsync?")

    writers = env("MAX_WRITERS", 1024)
    calls = env("CALLS", 20)
    size = env("BATCH", 20)

    IO.puts(
      "\n  schema  store fsync      rows/s   flushes   mailbox     p50 ack     p99 ack  (ms)"
    )

    for weight <- [:light, :heavy], fsync? <- [true, false] do
      root = Path.join(dir, "ceiling-#{weight}-#{fsync?}")

      {name, pid} =
        start_buffer(dir,
          store: Store.Local.new(dir: Path.join(root, "segments"), fsync: fsync?),
          log_dir: Path.join(root, "manifests"),
          flush_interval_ms: 25,
          flush_max_rows: 1_000_000
        )

      table_ref = {@dataset, "events"}

      {{wall_us, latencies}, mailbox} =
        with_mailbox_sampler(name, table_ref, fn ->
          hammer(name, [table_ref], writers, calls, size, weight)
        end)

      flushes = flush_count(name, table_ref)

      stop_buffer(name, pid)

      sorted = Enum.sort(latencies)

      IO.puts(
        "  #{label(weight, 6)}  #{label(fsync?, 11)}  " <>
          "#{pad(round(writers * calls * size / (wall_us / 1_000_000)), 10)}  " <>
          "#{pad(flushes, 7)}  #{pad(mailbox, 7)}  " <>
          "#{pad(ms(percentile(sorted, 0.50)), 10)}  #{pad(ms(percentile(sorted, 0.99)), 10)}"
      )
    end

    IO.puts("\n  the manifest log's fsync is unconditional, so this isolates the segment")
    IO.puts("  fsync only. Little movement here means the ceiling is upstream of both.")
  end

  defp flush_count(name, table_ref) do
    {:ok, entries} = Client.hot_manifest(name, table_ref)

    max(length(entries) - 1, 0)
  end

  defp per_flush(_rows, 0), do: 0
  defp per_flush(rows, flushes), do: Float.round(rows / flushes, 1)

  defp with_mailbox_sampler(name, table_ref, fun) do
    parent = self()
    registry = Runtime.registry(name)
    sampler = spawn_link(fn -> sample_mailbox(registry, table_ref, 0) end)

    result = fun.()
    send(sampler, {:peak, parent})

    peak =
      receive do
        {:peak, depth} -> depth
      after
        1_000 -> :unknown
      end

    {result, peak}
  end

  defp sample_mailbox(registry, table_ref, peak) do
    receive do
      {:peak, from} -> send(from, {:peak, peak})
    after
      1 -> sample_mailbox(registry, table_ref, max(peak, mailbox_depth(registry, table_ref)))
    end
  end

  defp mailbox_depth(registry, table_ref) do
    with [{pid, _value}] <- Registry.lookup(registry, table_ref),
         {:message_queue_len, depth} <- Process.info(pid, :message_queue_len) do
      depth
    else
      _not_running -> 0
    end
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

  defp hammer(name, table_refs, writers, calls, size, weight \\ :light) do
    warmup(name, table_refs, size, weight)
    tables = length(table_refs)

    :timer.tc(fn ->
      1..writers
      |> Task.async_stream(
        fn writer ->
          for call <- 1..calls do
            table_ref = Enum.at(table_refs, rem(writer + call, tables))
            offset = writer * 10_000_000 + call * size

            {us, {:ok, _ack}} =
              :timer.tc(fn ->
                Client.write_batch(name, table_ref, batch(size, offset, weight))
              end)

            us
          end
        end,
        max_concurrency: writers,
        timeout: 120_000
      )
      |> Enum.flat_map(fn {:ok, times} -> times end)
    end)
  end

  defp warmup(name, table_refs, size, weight) do
    Enum.each(table_refs, fn table_ref ->
      {:ok, _ack} = Client.write_batch(name, table_ref, batch(size, 0, weight))
    end)
  end

  defp batch_bytes(size, weight), do: :erlang.external_size(batch(size, 0, weight).rows)

  defp megabytes_per_second(batches, size, seconds, weight \\ :light),
    do: Float.round(batches * batch_bytes(size, weight) / 1_000_000 / seconds, 2)

  defp row_schema(:light),
    do: Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])

  defp row_schema(:heavy), do: schema()

  defp batch(size, offset, :light) do
    rows =
      for i <- 1..size do
        %{"id" => offset + i, "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], offset + i)}
      end

    %{schema: row_schema(:light), rows: rows}
  end

  defp batch(size, offset, :heavy) do
    rows =
      for i <- 1..size do
        %{
          "id" => offset + i,
          "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], offset + i),
          "name" => "row-#{offset + i}",
          "amount" => Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")
        }
      end

    %{schema: row_schema(:heavy), rows: rows}
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
