Code.require_file("support.exs", __DIR__)

defmodule Bench.HotManifest do
  @moduledoc """
  Can `HotServer` answer manifest reads as fast as the buffer commits?

  `HotServer` is the only way anything outside the buffer service reads the hot
  tier: the sealer pulls a claim's inputs through it, the query planner pulls the
  manifest through it. It runs on the pod that is also committing. So a manifest
  read that costs the node more than a commit does is a read that steals the
  throughput it exists to serve (PL-45).

  Four questions:

    * **What a manifest read costs against backlog depth.** The whole-manifest
      `GET` scans the table's ETS entries, sorts them, builds a record each, and
      encodes the lot as one JSON document. Every term is linear in the unsealed
      backlog. The claim-scoped `POST` resolves named ids through the index and
      skips the stats. This sweeps backlog depth and prices both.
    * **How many reads a node serves per second, against how many it needs.** A
      seal attempt makes two scoped reads (T-316). A table sealing every
      `flush_interval_ms` needs the route to sustain that rate while the node
      commits. This measures served rate at each backlog depth and reports the
      seal rate it supports.
    * **What the reads cost the commits.** Commit throughput and ack latency at a
      fixed *offered* read rate, with no readers, under `GET` load, and under
      `POST` load. The rate is offered rather than saturated on purpose: a real
      sealer makes two reads per attempt, so saturating readers measures a load
      nothing generates and prices the two routes as though they were asked for
      the same work. A route that cannot meet the offered rate shows it in the
      achieved column. This is the contention itself: the plan's collapse was
      commit concurrency falling while commit service time held flat.
    * **Where the bytes go.** Response size per entry, with and without the
      flush-time stats, against column count. The stats block is what makes an
      entry expensive, and it scales with the table's width rather than its rows.

  Column count is the variable that matters and the one a 4-column fixture
  hides — the soak that motivated this ran a 63-column table. `COLUMNS` sweeps it.

      mix run bench/hot_manifest.exs
      COLUMNS=63 BACKLOGS=64,1024,4096 mix run bench/hot_manifest.exs
      READERS=8 SECONDS=10 mix run bench/hot_manifest.exs
      BENCH_SECTION=contention READ_RATE=40 mix run bench/hot_manifest.exs

  ## What this measured, and what it settled

  On the machine this was last run on (`bench/results/hot_manifest.md`):

  - **The whole-manifest read cannot keep up with the buffer; the scoped read has
    an order of magnitude of headroom.** At a 1,024-entry backlog the `GET` route
    sustains 15.3 reads/s — 7.6 seal attempts a second — while the same node
    commits about 150 batches/s. At 4,096 it sustains 2.4 reads/s, so 1.2 seal
    attempts a second. Sealing cannot drain a backlog it takes a second per
    attempt to read. The `POST` route sustains 2,866-3,455 reads/s at every depth
    measured. That is PL-45's feedback loop closed.
  - **The scoped read is flat and the whole read is flat on nothing.** 1.1 ms and
    16.9 KiB from 64 entries to 4,096, against 8.3 ms/183 KiB rising to
    970 ms/11.9 MiB — 880x the latency and 700x the bytes at the top, for an
    answer the sealer discards all but 64 entries of.
  - **The stats block is the entry, once a table is real.** 53.9% of an entry at
    4 columns, 94.8% at 63 — the width the soak behind PL-45 ran. A stats-free
    entry is 270 bytes and does not move with width, because nothing else in the
    record is per column.
  - **Contention is a throughput loss, not a latency loss.** At 20 offered
    reads/s against a 1,024 backlog the `GET` route costs 25% of commit
    throughput *and still misses the offered rate*, achieving 11.9 of 20. The
    `POST` route meets the full rate at no measurable commit cost.
  - **Filling the fixture found a second O(backlog) term, on the write path.**
    Commit rate fell with backlog depth with no reader running at all — the
    buffer's own maintenance tick (T-317). See `bench/buffer.exs`'s
    `backlog_drag` section.
  """

  import Bench.Support

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotClient
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Schema

  @dataset "analytics"
  @table {@dataset, "events"}

  @claim_size 64

  def run do
    schedulers()

    sections = [
      read_cost: &read_cost/1,
      entry_width: &entry_width/1,
      serve_rate: &serve_rate/1,
      contention: &contention/1
    ]

    only = System.get_env("BENCH_SECTION")
    known = Enum.map(sections, fn {name, _section} -> Atom.to_string(name) end)

    if only != nil and only not in known do
      raise ArgumentError,
            "BENCH_SECTION=#{only} matches no section; known: #{Enum.join(known, ", ")}"
    end

    with_tmp_dir("hot-manifest", fn dir ->
      for {name, section} <- sections, only in [nil, Atom.to_string(name)] do
        section.(dir)
      end
    end)
  end

  defp read_cost(dir) do
    heading("manifest read cost against backlog depth (T-316)")

    columns = env("COLUMNS", 32)
    backlogs = sweep_env("BACKLOGS", [64, 256, 1_024, 4_096])

    IO.puts("\n  #{columns} columns; the scoped read names #{@claim_size} ids and skips stats\n")

    IO.puts("  backlog     route         served     ms p50     ms p99      KiB    B/entry")

    for backlog <- backlogs do
      stack = start_stack(dir, "cost-#{backlog}", columns)

      try do
        ids = fill(stack, backlog)
        claim = Enum.take(ids, @claim_size)

        report_read(backlog, "GET whole", fn -> whole(stack) end, backlog)
        report_read(backlog, "POST claim", fn -> scoped(stack, claim) end, length(claim))
      after
        stop_stack(stack)
      end
    end
  end

  defp report_read(backlog, route, fun, entries) do
    samples = for _ <- 1..20, do: :timer.tc(fun)
    {_us, body} = hd(samples)
    times = samples |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    bytes = byte_size(body)

    IO.puts(
      "  #{pad(backlog, 7)}     #{label(route, 12)}  #{pad(entries, 6)}  " <>
        "#{pad(ms(percentile(times, 0.50)), 9)}  #{pad(ms(percentile(times, 0.99)), 9)}  " <>
        "#{pad(kib(bytes), 7)}  #{pad(div(bytes, max(entries, 1)), 9)}"
    )
  end

  defp entry_width(dir) do
    heading("what an entry costs, against table width")

    widths = sweep_env("WIDTHS", [4, 16, 32, 63])

    IO.puts("\n  one entry, as the two routes serve it\n")
    IO.puts("  columns    with stats    without    stats share")

    for columns <- widths do
      stack = start_stack(dir, "width-#{columns}", columns)

      try do
        [id] = fill(stack, 1)
        with_stats = byte_size(scoped_with_stats(stack, [id]))
        without = byte_size(scoped(stack, [id]))
        share = Float.round((with_stats - without) / with_stats * 100, 1)

        IO.puts(
          "  #{pad(columns, 7)}    #{pad(with_stats, 10)}    #{pad(without, 7)}    " <>
            "#{pad(share, 10)}%"
        )
      after
        stop_stack(stack)
      end
    end
  end

  defp serve_rate(dir) do
    heading("served reads per second, and the seal rate that buys")

    columns = env("COLUMNS", 32)
    readers = env("READERS", 4)
    seconds = env("SECONDS", 5)
    backlogs = sweep_env("BACKLOGS", [64, 256, 1_024, 4_096])

    IO.puts(
      "\n  #{readers} concurrent readers for #{seconds}s; a seal attempt is two scoped reads\n"
    )

    IO.puts("  backlog     route         reads/s     MiB/s     ms p50     ms p99     seals/s")

    for backlog <- backlogs do
      stack = start_stack(dir, "rate-#{backlog}", columns)

      try do
        ids = fill(stack, backlog)
        claim = Enum.take(ids, @claim_size)

        report_rate(backlog, "GET whole", readers, seconds, fn -> whole(stack) end)
        report_rate(backlog, "POST claim", readers, seconds, fn -> scoped(stack, claim) end)
      after
        stop_stack(stack)
      end
    end
  end

  defp report_rate(backlog, route, readers, seconds, fun) do
    %{count: reads, bytes: bytes, times: times, wall_us: wall_us} =
      saturate(readers, seconds, fun)

    per_second = reads / (wall_us / 1_000_000)

    IO.puts(
      "  #{pad(backlog, 7)}     #{label(route, 12)}  #{pad(Float.round(per_second, 1), 7)}  " <>
        "#{pad(Float.round(bytes / 1_048_576 / (wall_us / 1_000_000), 1), 8)}  " <>
        "#{pad(ms(percentile(times, 0.50)), 9)}  #{pad(ms(percentile(times, 0.99)), 9)}  " <>
        "#{pad(Float.round(per_second / 2, 1), 10)}"
    )
  end

  defp contention(dir) do
    heading("what the reads cost the commits (PL-45)")

    columns = env("COLUMNS", 32)
    backlog = env("CONTENDED_BACKLOG", 1_024)
    writers = env("WRITERS", 8)
    readers = env("READERS", 4)
    read_rate = env("READ_RATE", 20)
    seconds = env("SECONDS", 5)
    rows = env("ROWS", 200)

    IO.puts(
      "\n  #{writers} writers of #{rows} rows against a #{backlog}-entry backlog, " <>
        "#{columns} columns, #{seconds}s; #{readers} readers offering #{read_rate} reads/s\n"
    )

    IO.puts("  read load        commits/s      rows/s     ack p50     ack p99     reads/s")

    for {name, reader} <- [
          {"none", nil},
          {"GET whole", :whole},
          {"POST claim", :scoped}
        ] do
      stack = start_stack(dir, "contend-#{name}", columns)

      try do
        ids = fill(stack, backlog)
        claim = Enum.take(ids, @claim_size)
        read = reader_fun(stack, reader, claim)

        report_contention(name, writers, readers, read_rate, seconds, rows, columns, stack, read)
      after
        stop_stack(stack)
      end
    end
  end

  defp reader_fun(_stack, nil, _claim), do: nil
  defp reader_fun(stack, :whole, _claim), do: fn -> whole(stack) end
  defp reader_fun(stack, :scoped, claim), do: fn -> scoped(stack, claim) end

  defp report_contention(name, writers, readers, read_rate, seconds, rows, columns, stack, read) do
    stop = make_ref()
    parent = self()
    interval_us = round(1_000_000 / max(read_rate, 1) * readers)

    reading =
      if read do
        for _ <- 1..readers,
            do: spawn_link(fn -> read_at_rate(parent, stop, read, interval_us, 0) end)
      else
        []
      end

    %{count: acks, times: times, wall_us: wall_us} =
      saturate(writers, seconds, fn -> commit(stack, rows, columns) end)

    for pid <- reading, do: send(pid, {stop, :halt})
    served = Enum.sum(for _ <- reading, do: receive(do: ({^stop, :halted, count} -> count)))

    seconds_run = wall_us / 1_000_000
    per_second = acks / seconds_run

    IO.puts(
      "  #{label(name, 14)}   #{pad(Float.round(per_second, 1), 9)}  " <>
        "#{pad(Float.round(per_second * rows, 1), 10)}  " <>
        "#{pad(ms(percentile(times, 0.50)), 10)}  #{pad(ms(percentile(times, 0.99)), 10)}  " <>
        "#{pad(Float.round(served / seconds_run, 1), 10)}"
    )
  end

  # Offered, not saturated: a sealer makes two reads per attempt, so a reader
  # spinning flat out prices a load nothing generates. A route that cannot meet
  # the offered rate answers with a lower achieved rate instead of stealing more.
  defp read_at_rate(parent, stop, fun, interval_us, count) do
    receive do
      {^stop, :halt} ->
        send(parent, {stop, :halted, count})
    after
      0 ->
        {us, _result} = :timer.tc(fun)
        sleep_for(interval_us - us)

        read_at_rate(parent, stop, fun, interval_us, count + 1)
    end
  end

  defp sleep_for(us) when us > 1_000, do: Process.sleep(div(us, 1000))
  defp sleep_for(_us), do: :ok

  defp saturate(workers, seconds, fun) do
    deadline = System.monotonic_time(:microsecond) + seconds * 1_000_000
    parent = self()
    started_at = System.monotonic_time(:microsecond)

    pids =
      for _ <- 1..workers do
        spawn_link(fn -> send(parent, {self(), work_until(deadline, fun, 0, 0, [])}) end)
      end

    tallies = for pid <- pids, do: receive(do: ({^pid, tally} -> tally))
    wall_us = System.monotonic_time(:microsecond) - started_at

    %{
      count: Enum.sum_by(tallies, & &1.count),
      bytes: Enum.sum_by(tallies, & &1.bytes),
      times: tallies |> Enum.flat_map(& &1.times) |> Enum.sort(),
      wall_us: wall_us
    }
  end

  defp work_until(deadline, fun, count, bytes, times) do
    {us, result} = :timer.tc(fun)
    count = count + 1
    bytes = bytes + result_bytes(result)
    times = [us | times]

    if System.monotonic_time(:microsecond) >= deadline do
      %{count: count, bytes: bytes, times: times}
    else
      work_until(deadline, fun, count, bytes, times)
    end
  end

  defp result_bytes(result) when is_binary(result), do: byte_size(result)
  defp result_bytes(_result), do: 0

  defp whole(stack) do
    {:ok, entries} = HotClient.manifest(stack.base_url, @table)

    JSON.encode!(entries)
  end

  defp scoped(stack, ids) do
    {:ok, entries} = HotClient.manifest(stack.base_url, @table, ids: ids, stats: false)

    JSON.encode!(entries)
  end

  defp scoped_with_stats(stack, ids) do
    {:ok, entries} = HotClient.manifest(stack.base_url, @table, ids: ids, stats: true)

    JSON.encode!(entries)
  end

  defp commit(stack, rows, columns) do
    offset = System.unique_integer([:positive])

    {:ok, ack} =
      Client.write_batch(stack.buffer, @table, batch(stack.schema, columns, rows, offset))

    ack
  end

  defp fill(stack, count) do
    for i <- 1..count do
      {:ok, ack} =
        Client.write_batch(stack.buffer, @table, batch(stack.schema, stack.columns, 1, i * 1_000))

      ack.segment_id
    end
  end

  defp start_stack(dir, label, columns) do
    unique = System.unique_integer([:positive])
    buffer = Module.concat(__MODULE__, "Buffer#{unique}")
    root = Path.join(dir, label)
    File.mkdir_p!(root)

    {:ok, pid} =
      BufferService.Supervisor.start_link(
        name: buffer,
        dir: Path.join(root, "buffer"),
        hot_server_port: 0,
        flush_interval_ms: 5,
        flush_max_rows: 100_000,
        seal_max_files: 1_000_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000,
        retire_grace_ms: 600_000
      )

    %{
      buffer: buffer,
      buffer_pid: pid,
      base_url: HotServer.base_url(buffer),
      columns: columns,
      schema: wide_schema(columns)
    }
  end

  defp stop_stack(stack) do
    Supervisor.stop(stack.buffer_pid)
    BufferService.Runtime.delete(stack.buffer)
  end

  @doc """
  A schema `columns` wide, cycling the logical types a manifest carries bounds for.

  Stats are per column, so table width is what an entry's cost scales with. The
  cycle keeps string and timestamp bounds in the mix rather than measuring
  integers alone — a string bound is the expensive one to encode.
  """
  def wide_schema(columns) do
    fields =
      for index <- 1..columns do
        case rem(index, 4) do
          0 -> {"col_#{index}", :int64}
          1 -> {"col_#{index}", :string}
          2 -> {"col_#{index}", :timestamp}
          3 -> {"col_#{index}", :float64}
        end
      end

    Schema.new!(fields)
  end

  defp batch(schema, columns, rows, offset) do
    values =
      for i <- 1..rows do
        Map.new(1..columns, fn index -> {"col_#{index}", value(index, offset + i)} end)
      end

    %{schema: schema, rows: values}
  end

  defp value(index, i) do
    case rem(index, 4) do
      0 -> i
      1 -> "value-#{i}-#{index}"
      2 -> NaiveDateTime.add(~N[2026-01-01 00:00:00], i)
      3 -> i / 7
    end
  end

  defp percentile(sorted, fraction) do
    Enum.at(sorted, min(length(sorted) - 1, trunc(fraction * length(sorted))))
  end

  defp kib(bytes), do: Float.round(bytes / 1024, 1)
end

Bench.HotManifest.run()
