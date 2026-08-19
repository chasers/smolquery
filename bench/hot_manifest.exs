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
    * **What the reads cost the commits.** Commit throughput and ack latency with
      no readers, then under `GET` load, then under `POST` load. This is the
      contention itself: the plan's collapse was commit concurrency falling while
      commit service time held flat.
    * **Where the bytes go.** Response size per entry, with and without the
      flush-time stats, against column count. The stats block is what makes an
      entry expensive, and it scales with the table's width rather than its rows.

  Column count is the variable that matters and the one a 4-column fixture
  hides — the soak that motivated this ran a 63-column table. `COLUMNS` sweeps it.

      mix run bench/hot_manifest.exs
      COLUMNS=63 BACKLOGS=64,1024,4096 mix run bench/hot_manifest.exs
      READERS=8 SECONDS=10 mix run bench/hot_manifest.exs

  ## What this measures, and what it settles

  Nothing yet — this script is new with the T-315/T-316 fix and has not been run
  on a recorded machine. `bench/results/hot_manifest.md` gets the numbers and the
  conclusions on the first recorded run.
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

    with_tmp_dir("hot-manifest", fn dir ->
      read_cost(dir)
      entry_width(dir)
      serve_rate(dir)
      contention(dir)
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
    seconds = env("SECONDS", 5)
    rows = env("ROWS", 200)

    IO.puts(
      "\n  #{writers} writers of #{rows} rows against a #{backlog}-entry backlog, " <>
        "#{columns} columns, #{seconds}s\n"
    )

    IO.puts("  read load        commits/s      rows/s     ack p50     ack p99")

    for {label, reader} <- [
          {"none", nil},
          {"GET whole", :whole},
          {"POST claim", :scoped}
        ] do
      stack = start_stack(dir, "contend-#{label}", columns)

      try do
        ids = fill(stack, backlog)
        claim = Enum.take(ids, @claim_size)
        read = reader_fun(stack, reader, claim)

        report_contention(label, writers, readers, seconds, rows, columns, stack, read)
      after
        stop_stack(stack)
      end
    end
  end

  defp reader_fun(_stack, nil, _claim), do: nil
  defp reader_fun(stack, :whole, _claim), do: fn -> whole(stack) end
  defp reader_fun(stack, :scoped, claim), do: fn -> scoped(stack, claim) end

  defp report_contention(label, writers, readers, seconds, rows, columns, stack, read) do
    stop = make_ref()
    parent = self()

    reading =
      if read do
        for _ <- 1..readers, do: spawn_link(fn -> read_until(parent, stop, read) end)
      else
        []
      end

    %{count: acks, times: times, wall_us: wall_us} =
      saturate(writers, seconds, fn -> commit(stack, rows, columns) end)

    for pid <- reading, do: send(pid, {stop, :halt})
    for _ <- reading, do: receive(do: ({^stop, :halted} -> :ok))

    per_second = acks / (wall_us / 1_000_000)

    IO.puts(
      "  #{label(label, 14)}   #{pad(Float.round(per_second, 1), 9)}  " <>
        "#{pad(Float.round(per_second * rows, 1), 10)}  " <>
        "#{pad(ms(percentile(times, 0.50)), 10)}  #{pad(ms(percentile(times, 0.99)), 10)}"
    )
  end

  defp read_until(parent, stop, fun) do
    receive do
      {^stop, :halt} -> send(parent, {stop, :halted})
    after
      0 ->
        fun.()
        read_until(parent, stop, fun)
    end
  end

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
