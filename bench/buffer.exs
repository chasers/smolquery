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

  D6 comes in five parts, because "one table plateaus at N rows/s" is only
  actionable once you know *what* plateaued:

    * **the sweep** — writers against rows/s, for a light schema and a heavy one,
      carrying the two numbers that explain a plateau: how many flushes actually
      happened (each flush is one segment in the manifest, so the manifest counts
      them) and how deep the buffer's mailbox got.
    * **the encode in isolation** — `Writer.write` timed off to the side, across
      the rows-per-flush the sweep produced. Throughput is
      `rows_per_flush / max(encode, flush_interval_ms)`: while an encode finishes
      inside the interval, cadence sets the ack; the moment it does not, the
      encode does.
    * **the fsyncs at the ceiling** — D3's store-fsync toggle re-run at the top of
      the writer sweep, to show whether the plateau is the fsync or the encode.
    * **the byte bound** — the correction to the two parts above. They price the
      encode against `flush_interval_ms` as if `rows_per_flush` could grow without
      limit, but `flush_max_bytes` caps it first (8 MB by default, so ~48K light
      rows or ~31K heavy). This sweeps that bound to find which of the three
      actually binds, because the answer decides whether partitioning is even
      addressing the right quantity.
    * **the partition proxy** — P independent TableBuffers over one workload,
      addressed as P tables. Partitions differ from tables in routing and identity,
      not in throughput mechanics, so this reads the multiplier PL-6 would buy
      without building PL-6. Two modes, and they answer different questions:
      `split` divides a fixed writer pool P ways (partitioning a real workload),
      `scale` holds writers-per-buffer constant (headroom). Below the crossover
      `split` should be *neutral* by construction — P encodes of 1/P the rows
      finish in the same cycle — so gains there mean P=1 was already past it.

  The first two sections use two columns (`id`, `ts`) so their numbers measure group
  commit and the store rather than `Decimal` construction. Every D6 section runs
  three schemas instead, because the encode is the thing under investigation and
  its cost per row is the variable that moves everything else:

    * `light` — 2 columns (`int64`, `timestamp`). ~165 B/row.
    * `heavy` — `Bench.Support.schema/0`'s 4 columns, adding a `string` and a
      `Decimal`. ~254 B/row.
    * `huge` — 20 columns spanning all seven logical types, the shape of a real
      event table: ids, a session string, low-cardinality enums (`event`, `region`,
      `country`), floats, a `bool`, a `date`, two `Decimal`s, and a `tags` blob.
      ~890 B/row.

  `huge` is 5.4× `light`'s bytes but 12× its encode time — 20 Arrow arrays to
  build, so per-column overhead dominates payload size. That makes it the only one
  of the three whose encode is already past `flush_interval_ms` at the default
  `flush_max_bytes`, and therefore the case partitioning helps most.

  Unlike `heavy`, `huge`'s column values derive from the *global* row index, so
  cardinality is realistic per column rather than repeating identically in every
  batch. (`heavy` keys its `Decimal` off the within-batch index; that quirk is left
  alone so its numbers stay comparable with earlier runs.)

  MB/s is measured on the in-memory batch (`:erlang.external_size/1`), the same
  measure the buffer's byte bound uses — not the encoded Parquet. Every cell
  warms up with one untimed write per table, so lazy buffer start, manifest
  recovery, and NIF warmup stay out of the tail percentiles.

  **Sealing is disabled here** — every cell raises the seal thresholds out of
  reach. This script measures group commit; `bench/sealer.exs` measures sealing.
  Without that, the high-volume sections cross `seal_max_bytes` mid-cell and
  signal seals into a DuckLake catalog this script never creates, and the failed
  seals retry with backoff *inside the timed window*. Also why `flush_count/2` can
  trust the manifest: nothing retires entries out from under it.

      mix run bench/buffer.exs
      CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs

  ## What this measured, and what it settled

  The last full run — its numbers, machine, and tables — is in
  `bench/results/buffer.md`. The short version:

  - **Ack latency has two regimes, and which one you are in decides everything.**
    *Below saturation:* `p50 = flush_interval_ms + ~5 ms`, invariant across a
    14,000× throughput spread — a client pays the flush cadence, not the load on
    it, which is the whole point of group commit. *At saturation:*
    `p50 = outstanding rows ÷ throughput` (Little's Law, since synchronous writers
    pin outstanding rows at `writers × batch`) and **`flush_interval_ms` drops out
    of the equation entirely**. Verified within 13%, usually 3%, across all 21
    saturated cells.
  - **Flush cadence is exactly the dial it's documented as — below saturation.**
    10 ms → ~51K rows/s at ~15 ms p50 ack; 1000 ms → ~790 rows/s at ~1010 ms p50 —
    linear in between. Above saturation this knob does nothing; see T-56.
  - **D3: the two fsyncs cost about 2 ms together, not the bottleneck.**
    An open+write+fsync+close of a 4 KiB file runs ~0.3-0.7 ms; a full group
    commit with the segment store's fsync on ran ~2.0 ms p50 versus ~1.7 ms
    with it off (down from ~2.8 ms before the manifest log held its fd open
    across appends) — so the segment fsync's own marginal cost is
    sub-millisecond, and the manifest append pays only its write and fsync.
    Either way, both fsyncs together are an order of magnitude under any
    `flush_interval_ms` worth running. D3's accepted durability window costs
    single-digit milliseconds, not the ack.
  - **D6: the writer sweep finds no plateau for `light` or `heavy`, because 20-row
    batches cannot saturate them.** 601 → 2,293 → 9,378 → 37,048 → 150,911 →
    547,345 rows/s at 1, 4, 16, 64, 256, 1024 writers. `flushes` is 20 in every one
    of those cells: group commit turns added writers into a *wider* flush, not more
    flushes, so rows/s climbs with rows-per-flush (20 → 20,480) while cadence holds.
    Their linear scaling measures headroom, not a limit — and the "~148K rows/s, no
    plateau found" PL-6 was drafted against was this artifact.
  - **`huge` is the exception, and it is why the sweep now works.** At 1024 writers
    it flushes 46 times, not 20, because 20,480 rows × 867 B crosses the 8 MB byte
    bound — and it is the one row that plateaus (34,808 → 109,395 → 227,556, i.e.
    3.1× then 2.1×).
  - **Saturate with 500-row batches and one table tops out at 2.19M rows/s light,
    1.08M heavy, 280K huge.** None of the three configured bounds sets it: sweeping
    `flush_max_bytes` 8 MB → 512 MB moves rows-per-flush 8-27× and throughput
    5-12%, not even monotonically. The byte bound decides how rows are *packed*
    into flushes, not how many get through — though at its 8 MB default it is what
    caps rows-per-flush (47,189 / 29,681 / 9,499 rows).
  - **What sets it is the encode plus the write path around it.** Encode alone
    sustains ~4.17M rows/s light, ~2.28M heavy, ~356K huge. Measured saturation is
    53% / 47% / **79%** of that: the write path costs proportionally *less* the more
    expensive the encode, because per-flush fixed costs amortize over longer work.
    Read `encode_in_isolation/1`'s last column as an upper bound.
  - **Not the fsync, and not the mailbox.** Toggling the segment fsync at 1024
    writers moves throughput ±2-6% on all three schemas — noise. Mailbox depth
    stays at or below the writer count because each writer has one outstanding call.
  - **The argument for PL-6 is the ack contract, not headroom.** At the default
    config a 20-column event table returns a **2.01 second p50 ack** — a
    user-visible defect at ordinary load, not a ceiling someone might reach later.
  - **P independent buffers multiply it, and most for the schemas that need it
    most.** The proxy reaches **6.68M rows/s light, 3.58M heavy, 1.11M huge at
    P=8** on 10 cores. The multiplier tracks how encode-bound the schema is —
    3.11× light (53% in encode), 3.22× heavy (47%), **4.09× huge (79%)** — which is
    the mechanism claim, since the encode is what partitioning parallelizes.
  - **But partitioning alone cannot fix overload.** Latency at saturation is
    `outstanding ÷ throughput`, so P divides the overload factor by P and no more:
    at the 73× overload this bench drives, P=8 still yields 438 ms. Bounding ack
    latency requires bounding outstanding rows, which nothing does today — see
    T-56, a companion to PL-6 rather than a follow-up.
  - **Inline flush stays; double-buffering still has no case.** It hides one encode
    behind the next accumulation — worth part of one cycle — where partitioning
    multiplies the encode itself and helps the ack degradation too.

  """

  import Bench.Support

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @dataset "bench"
  @weights [:light, :heavy, :huge]

  def run do
    sections = [
      throughput_and_latency: &throughput_and_latency/1,
      flush_cadence: &flush_cadence/1,
      fsync_cost: &fsync_cost/1,
      inline_flush_ceiling: &inline_flush_ceiling/1,
      encode_in_isolation: &encode_in_isolation/1,
      fsyncs_at_the_ceiling: &fsyncs_at_the_ceiling/1,
      byte_bound: &byte_bound/1,
      partition_proxy: &partition_proxy/1,
      replication_delta: &replication_delta/1
    ]

    only = System.get_env("BENCH_SECTION")
    known = Enum.map(sections, fn {name, _section} -> Atom.to_string(name) end)

    if only != nil and only not in known do
      raise ArgumentError,
            "BENCH_SECTION=#{only} matches no section; known: #{Enum.join(known, ", ")}"
    end

    with_tmp_dir("buffer", fn dir ->
      for {name, section} <- sections, only in [nil, Atom.to_string(name)] do
        section.(dir)
      end
    end)
  end

  defp replication_delta(dir) do
    heading(
      "replication: segment shipping's ack price (T-103) — one local follower over " <>
        "Transport.Local, so the delta is the protocol (segment read-back, second put, " <>
        "second manifest fsync, one Endpoint hop) without a network RTT; kind adds the RTT"
    )

    writers = env("WRITERS", 16)
    calls = env("CALLS", 20)
    size = env("BATCH", 50)

    IO.puts("\n  mode      batches/s      rows/s      MB/s      p50     p95     p99  (ms)")

    {follower, follower_pid} =
      start_buffer(dir, flush_interval_ms: 25, flush_max_rows: 1_000_000)

    modes = [
      {"none", []},
      {"rf2",
       [
         replicator:
           {Smolquery.BufferService.Replicator.SegmentShipping,
            replication_factor: 2,
            targets: fn _name, _ref ->
              {:ok, [{Smolquery.BufferService.Transport.Local, node(), follower}]}
            end}
       ]}
    ]

    for {label, extra} <- modes do
      {name, pid} =
        start_buffer(dir, [flush_interval_ms: 25, flush_max_rows: 1_000_000] ++ extra)

      {wall_us, latencies} = hammer(name, [{@dataset, "events"}], writers, calls, size)

      stop_buffer(name, pid)

      report([pad(label, 6)], latencies, wall_us, writers * calls, size)
    end

    stop_buffer(follower, follower_pid)
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

    for weight <- @weights, writers <- writer_counts do
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
    IO.puts("  (#{interval} ms here). Past that, rows/s = rows_per_flush / encode.")
    IO.puts("  Read the last column as an upper bound, not a prediction: it prices the")
    IO.puts("  encode alone, and the byte-bound section measures the write path around")
    IO.puts("  it costing roughly another half on top.\n")

    IO.puts(
      "  schema     rows   encode min   encode med   fits #{interval}ms      ceiling rows/s"
    )

    for weight <- @weights, rows <- sizes do
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

    for weight <- @weights, fsync? <- [true, false] do
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

  defp byte_bound(dir) do
    heading("what actually caps rows per flush: flush_max_bytes, not the interval")

    writers = env("MAX_WRITERS", 1024)
    calls = env("CALLS", 20)
    size = 500

    IO.puts(
      "\n  #{writers} writers x #{size}-row batches offers #{writers * size} rows per cycle."
    )

    IO.puts("  Whichever bound binds first decides what one encode actually swallows.\n")

    IO.puts(
      "  schema  flush_max_bytes      rows/s   flushes   rows/flush   MiB/flush     p50 ack"
    )

    for weight <- @weights, limit <- [8_000_000, 32_000_000, 512_000_000] do
      root = Path.join(dir, "bytebound-#{weight}-#{limit}")

      {name, pid} =
        start_buffer(dir,
          store: Store.Local.new(dir: Path.join(root, "segments")),
          log_dir: Path.join(root, "manifests"),
          flush_interval_ms: 25,
          flush_max_rows: 100_000_000,
          flush_max_bytes: limit,
          max_buffered_rows: 100_000_000,
          max_buffered_bytes: 2_048_000_000
        )

      table_ref = {@dataset, "events"}
      {wall_us, latencies} = hammer(name, [table_ref], writers, calls, size, weight)
      flushes = flush_count(name, table_ref)

      stop_buffer(name, pid)

      rows = writers * calls * size
      per = per_flush(rows, flushes)

      IO.puts(
        "  #{label(weight, 6)}  #{pad(div(limit, 1_000_000), 13)} MB  " <>
          "#{pad(round(rows / (wall_us / 1_000_000)), 10)}  #{pad(flushes, 7)}  " <>
          "#{pad(per, 11)}  " <>
          "#{pad(mib(per * bytes_per_row(weight)), 9)}  " <>
          "#{pad(ms(percentile(Enum.sort(latencies), 0.50)), 10)}"
      )
    end

    IO.puts("\n  the default is 8 MB. If rows/flush stops growing there while rows/s")
    IO.puts("  stops with it, the byte bound — not flush_interval_ms and not the")
    IO.puts("  encode — is what one table's ceiling is actually made of.")
  end

  defp partition_proxy(dir) do
    heading("the partition proxy (PL-6 step 4): P independent buffers over one workload")

    total = env("MAX_WRITERS", 1024)
    per_buffer = env("WRITERS_PER_BUFFER", 128)
    calls = env("CALLS", 20)
    size = 500

    IO.puts("\n  P independent TableBuffers, addressed as P tables. Partitions differ from")
    IO.puts("  tables in routing and identity, not in throughput mechanics, so this reads")
    IO.puts("  the multiplier PL-6 would buy without building PL-6.\n")
    IO.puts("  split — #{total} writers divided P ways: models partitioning a fixed workload.")
    IO.puts("  scale — #{per_buffer} writers per buffer: models P buffers each fully loaded.\n")

    IO.puts(
      "  mode    schema    P   writers      rows/s      vs P=1   flushes   rows/flush   mailbox     p50 ack"
    )

    for mode <- [:split, :scale], weight <- @weights do
      Enum.reduce([1, 2, 4, 8], nil, fn partitions, baseline ->
        writers = if mode == :split, do: total, else: per_buffer * partitions
        refs = for p <- 1..partitions, do: {@dataset, "events_p#{p}"}
        root = Path.join(dir, "proxy-#{mode}-#{weight}-#{partitions}")

        {name, pid} =
          start_buffer(dir,
            store: Store.Local.new(dir: Path.join(root, "segments")),
            log_dir: Path.join(root, "manifests"),
            flush_interval_ms: 25,
            flush_max_rows: 100_000_000,
            flush_max_bytes: 512_000_000,
            max_buffered_rows: 100_000_000,
            max_buffered_bytes: 2_048_000_000
          )

        {{wall_us, latencies}, mailbox} =
          with_mailbox_sampler(name, refs, fn ->
            hammer(name, refs, writers, calls, size, weight)
          end)

        flushes = refs |> Enum.map(&flush_count(name, &1)) |> Enum.sum()

        stop_buffer(name, pid)

        rows = writers * calls * size
        rate = rows / (wall_us / 1_000_000)
        speedup = if baseline, do: "#{Float.round(rate / baseline, 2)}x", else: "-"

        IO.puts(
          "  #{label(mode, 6)}  #{label(weight, 6)}  #{pad(partitions, 3)}  #{pad(writers, 7)}  " <>
            "#{pad(round(rate), 10)}  #{pad(speedup, 10)}  #{pad(flushes, 7)}  " <>
            "#{pad(per_flush(rows, flushes), 11)}  #{pad(mailbox, 7)}  " <>
            "#{pad(ms(percentile(Enum.sort(latencies), 0.50)), 10)}"
        )

        baseline || rate
      end)
    end

    IO.puts("\n  split is the prediction under test: while one buffer's encode still fits")
    IO.puts("  inside flush_interval_ms, dividing a fixed workload P ways should be")
    IO.puts("  throughput-neutral — P encodes of 1/P the rows finish in the same cycle.")
    IO.puts("  Gains there mean P=1 was already past its crossover. scale is the headroom")
    IO.puts("  question: do P loaded buffers multiply, or contend?")
  end

  defp flush_count(name, table_ref) do
    {:ok, entries} = Client.hot_manifest(name, table_ref)

    max(length(entries) - 1, 0)
  end

  defp per_flush(_rows, 0), do: 0
  defp per_flush(rows, flushes), do: round(rows / flushes)

  defp with_mailbox_sampler(name, table_refs, fun) do
    parent = self()
    registry = Runtime.registry(name)
    sampler = spawn_link(fn -> sample_mailbox(registry, List.wrap(table_refs), 0) end)

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

  defp sample_mailbox(registry, table_refs, peak) do
    receive do
      {:peak, from} -> send(from, {:peak, peak})
    after
      1 -> sample_mailbox(registry, table_refs, max(peak, deepest_mailbox(registry, table_refs)))
    end
  end

  defp deepest_mailbox(registry, table_refs),
    do: table_refs |> Enum.map(&mailbox_depth(registry, &1)) |> Enum.max()

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
      hot_server_port: 0,
      seal_max_bytes: 1_000_000_000_000,
      seal_max_files: 1_000_000_000,
      seal_max_age_ms: 86_400_000
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

  defp bytes_per_row(weight), do: batch_bytes(1_000, weight) / 1_000

  defp megabytes_per_second(batches, size, seconds, weight \\ :light),
    do: Float.round(batches * batch_bytes(size, weight) / 1_000_000 / seconds, 2)

  defp row_schema(:light),
    do: Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])

  defp row_schema(:heavy), do: schema()

  defp row_schema(:huge) do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}},
      {"user_id", :int64},
      {"session_id", :string},
      {"event", :string},
      {"region", :string},
      {"latency_ms", :float64},
      {"ok", :bool},
      {"day", :date},
      {"price", {:numeric, 18, 4}},
      {"quantity", :int64},
      {"discount", :float64},
      {"sku", :string},
      {"country", :string},
      {"retries", :int64},
      {"updated_at", :timestamp},
      {"score", :float64},
      {"tags", :string}
    ])
  end

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

  @events ~w(page_view click purchase signup logout search add_to_cart error)
  @regions ~w(us-east-1 us-west-2 eu-west-1 eu-central-1 ap-south-1 sa-east-1)
  @countries ~w(US CA GB DE FR ES IT NL SE NO PL BR MX JP KR IN AU NZ ZA SG)

  defp batch(size, offset, :huge) do
    rows =
      for i <- 1..size do
        n = offset + i

        %{
          "id" => n,
          "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], n),
          "name" => "row-#{n}",
          "amount" => Decimal.new(1, rem(n, 100_000_000), -2),
          "user_id" => rem(n, 250_000),
          "session_id" => "sess-" <> String.pad_leading(Integer.to_string(n, 16), 12, "0"),
          "event" => Enum.at(@events, rem(n, length(@events))),
          "region" => Enum.at(@regions, rem(n, length(@regions))),
          "latency_ms" => rem(n, 4_000) / 7,
          "ok" => rem(n, 97) != 0,
          "day" => Date.add(~D[2026-01-01], rem(n, 365)),
          "price" => Decimal.new(1, rem(n, 1_000_000), -4),
          "quantity" => rem(n, 40) + 1,
          "discount" => rem(n, 30) / 100,
          "sku" => "sku-#{rem(n, 50_000)}",
          "country" => Enum.at(@countries, rem(n, length(@countries))),
          "retries" => rem(n, 4),
          "updated_at" => NaiveDateTime.add(~N[2026-01-01 00:00:00], n + rem(n, 900)),
          "score" => rem(n, 10_000) / 100,
          "tags" => Enum.map_join(1..3, ",", &"tag-#{rem(n + &1, 50)}")
        }
      end

    %{schema: row_schema(:huge), rows: rows}
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
