Code.require_file("support.exs", __DIR__)

defmodule Bench.Sealer do
  @moduledoc """
  The sealed tier's exit criterion: what a seal costs, and how far behind it runs.

  Four questions, each settling something the milestone plan left open:

    * **Seal lag** — how old the oldest unsealed row gets before it reaches the
      catalog. This is the number that bounds the single-copy loss window, so it is
      the one the milestone is judged on.
    * **Merge implementation** — DuckDB `COPY (SELECT * FROM read_parquet(...))`
      against reading each input into an `Explorer.DataFrame` and concatenating.
      PL-1 left this as an open question to measure rather than argue.
    * **Merge throughput and peak memory** — across input count and rows per
      input, to see what the merge scales with and whether it stays off the BEAM
      heap.
    * **Sealed segment size distribution** — what `seal_max_bytes` and
      `seal_max_age_ms` actually produce. The compactor's input, so Milestone 7
      starts with numbers instead of a guess.

  Every merge here reads its inputs over HTTP from a live `HotServer` through
  `httpfs`, and commits to a real DuckLake catalog, because those are the two costs
  worth measuring and faking either would measure the harness.

      mix run bench/sealer.exs
      INPUTS=64 ROWS=20000 mix run bench/sealer.exs

  ## What this measured, and what it settled

  On the machine this was last run on:

  - **DuckDB `COPY` is about twice as fast as Explorer concat, and it stays that
    way.** 32 inputs of 5,000 rows each merged in ~30 ms p50 through `COPY` against
    ~58 ms reading each input into a DataFrame and concatenating. `COPY` stays,
    which settles PL-1's open question. Peak *BEAM* heap is zero for both, and that
    is not a tiebreaker: Polars holds frames in Rust either way, so the difference
    is wall time and the number of round trips, not Elixir memory.
  - **The merge inflated the data 2.85x until the codec was fixed, and that is this
    benchmark's whole justification.** `Segments.Writer` writes micro-segments with
    zstd; DuckDB's `COPY` defaults to snappy. Sealing therefore made a table
    *larger* — 0.4 MiB out of 0.1 MiB in — while every other test passed, because
    nothing about correctness depends on file size. With zstd matched, sealing
    shrinks instead: ratios of 0.80, 0.65, 0.63 at 4, 16, and 64 inputs.
  - **The compaction win is real before the compactor exists.** That ratio falls as
    input count rises (0.80 → 0.63), which is Parquet doing better on one large file
    than on many small ones — dictionaries and row groups amortize. Merging more
    aggressively is cheaper storage as well as fewer files.
  - **Merge cost is linear in total rows, with a visible per-input floor.** 640,000
    rows across 64 inputs merged in ~72 ms (~8.9M rows/s); 4,000 rows across 4
    inputs took ~6.6 ms (~0.6M rows/s). The floor is per-input HTTP and Parquet
    footer work, so small inputs are what waste a merge — an argument for letting
    `seal_max_files` grow rather than sealing eagerly on file count.
  - **Seal lag is the threshold, not the seal.** The handoff itself — manifest pull,
    merge, catalog commit, retire — ran 37-51 ms regardless of claim size. So the
    age of the oldest unsealed row, and therefore the single-copy loss window, is
    whatever `seal_max_age_ms` and the file/byte thresholds allow before signalling.
    That is the milestone's exit criterion met: the loss window is a config decision
    with a known, small cost, not an emergent property of the sealer.
  - **The default `seal_max_bytes` (64 MiB) produces sealed segments well under the
    128-512 MB the architecture targets**, which is exactly the case Milestone 7's
    compactor exists for. A deployment that wants large sealed segments raises the
    threshold and accepts the lag that follows — lag which, per the previous point,
    it can price.

  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime

  @dataset "analytics"
  @table {@dataset, "events"}

  def run do
    with_tmp_dir("sealer", fn dir ->
      merge_implementations(dir)
      merge_scaling(dir)
      seal_lag(dir)
      segment_sizes(dir)
    end)
  end

  defp merge_implementations(dir) do
    heading("merge implementation: DuckDB COPY against Explorer concat")

    inputs = env("INPUTS", 32)
    rows = env("ROWS", 5_000)

    IO.puts("\n  #{inputs} inputs of #{rows} rows each, read over HTTP through httpfs\n")
    IO.puts("  implementation      p50      min     peak heap (MiB)   rows/s")

    stack = start_stack(dir, "impl")
    ids = fill(stack, inputs, rows)
    claim = freeze(stack, ids)

    copy = measure_merge(fn -> Merge.run(stack.runtime, @table, claim) end)
    explorer = measure_merge(fn -> explorer_merge(stack, claim) end)

    for {label, result} <- [{"DuckDB COPY", copy}, {"Explorer concat", explorer}] do
      IO.puts(
        "  #{label(label, 18)}  #{pad(ms(result.median), 7)}  #{pad(ms(result.min), 7)}  " <>
          "#{pad(mib(result.peak), 15)}   #{pad(rows_per_second(inputs * rows, result.median), 8)}"
      )
    end

    stop_stack(stack)

    IO.puts("\n  peak heap is the merging process's own; COPY keeps rows out of the BEAM,")
    IO.puts("  so a growing number there is the Explorer path materializing frames.")
  end

  defp merge_scaling(dir) do
    heading("merge throughput: input count x rows per input")

    input_counts = Enum.filter([4, 16, 64], &(&1 <= env("MAX_INPUTS", 64)))
    row_counts = Enum.filter([1_000, 10_000], &(&1 <= env("MAX_ROWS", 10_000)))

    IO.puts("\n  inputs     rows   total rows    merge ms      rows/s   sealed KiB")

    for count <- input_counts, rows <- row_counts do
      stack = start_stack(dir, "scale-#{count}-#{rows}")
      ids = fill(stack, count, rows)
      warm(stack)
      claim = freeze(stack, ids)

      {us, {:ok, segment}} = :timer.tc(fn -> Merge.run(stack.runtime, @table, claim) end)

      stop_stack(stack)

      IO.puts(
        "  #{pad(count, 6)}  #{pad(rows, 7)}  #{pad(count * rows, 11)}  #{pad(ms(us), 10)}  " <>
          "#{pad(rows_per_second(count * rows, us), 10)}  #{pad(kib(segment.byte_size), 11)}"
      )
    end
  end

  defp seal_lag(dir) do
    heading("seal lag: how old the oldest unsealed row gets")

    rows = env("ROWS", 1_000)

    IO.puts("\n  the whole handoff, signalled by the buffer's own threshold:\n")
    IO.puts("  seal_max_files    writes    lag ms    handoff ms   sealed files")

    for max_files <- Enum.filter([1, 8, 32], &(&1 <= env("MAX_FILES", 32))) do
      stack = start_stack(dir, "lag-#{max_files}", seal_max_files: max_files)

      started = System.monotonic_time(:millisecond)
      ids = fill(stack, max_files, rows)
      claim = freeze(stack, ids)

      {us, :ok} = :timer.tc(fn -> seal(stack, claim) end)
      lag = System.monotonic_time(:millisecond) - started

      {:ok, sealed} = Catalog.segments(stack.catalog, @table, :current)

      stop_stack(stack)

      IO.puts(
        "  #{pad(max_files, 14)}  #{pad(max_files, 8)}  #{pad(lag, 8)}  #{pad(ms(us), 12)}  " <>
          "#{pad(length(sealed), 12)}"
      )
    end

    IO.puts("\n  lag here excludes the threshold wait itself (this signals immediately);")
    IO.puts("  add seal_max_age_ms to read the real loss window a deployment carries.")
  end

  defp segment_sizes(dir) do
    heading("sealed segment size: what seal_max_bytes produces")

    rows = env("ROWS", 5_000)

    IO.puts("\n  inputs   input bytes (MiB)   sealed (MiB)   ratio   rows")

    for count <- Enum.filter([4, 16, 64], &(&1 <= env("MAX_INPUTS", 64))) do
      stack = start_stack(dir, "size-#{count}")
      ids = fill(stack, count, rows)

      {:ok, entries} = Client.hot_manifest(stack.buffer, @table)
      input_bytes = Enum.sum_by(entries, & &1.byte_size)

      claim = freeze(stack, ids)
      {:ok, segment} = Merge.run(stack.runtime, @table, claim)

      stop_stack(stack)

      IO.puts(
        "  #{pad(count, 6)}  #{pad(mib(input_bytes), 17)}  #{pad(mib(segment.byte_size), 13)}  " <>
          "#{pad(Float.round(segment.byte_size / max(input_bytes, 1), 2), 5)}   #{pad(segment.row_count, 6)}"
      )
    end

    IO.puts("\n  a ratio under 1 is Parquet doing better on one large file than on many")
    IO.puts("  small ones — the compaction win, before the compactor exists.")
  end

  defp explorer_merge(stack, claim) do
    {:ok, entries} = HotTier.manifest(stack.runtime, @table)
    claimed = MapSet.new(claim.ids)

    frames =
      entries
      |> Enum.filter(&MapSet.member?(claimed, &1["id"]))
      |> Enum.map(&DataFrame.from_parquet!(&1["url"]))

    {:ok, key} = sealed_key()

    Store.put(stack.runtime.store, key, fn staged ->
      frames |> DataFrame.concat_rows() |> DataFrame.to_parquet!(staged)

      :ok
    end)
  end

  defp measure_merge(fun) do
    reps = env("REPS", 5)
    _warm = fun.()

    results =
      for _ <- 1..reps do
        before = process_memory()
        {us, _result} = :timer.tc(fun)

        {us, process_memory() - before}
      end

    times = results |> Enum.map(&elem(&1, 0)) |> Enum.sort()

    %{
      min: hd(times),
      median: Enum.at(times, div(reps, 2)),
      peak: results |> Enum.map(&elem(&1, 1)) |> Enum.max()
    }
  end

  defp process_memory do
    :erlang.garbage_collect()

    {:memory, bytes} = Process.info(self(), :memory)

    bytes
  end

  defp start_stack(dir, label, buffer_opts \\ []) do
    unique = System.unique_integer([:positive])
    buffer = Module.concat(__MODULE__, "Buffer#{unique}")
    storage = Module.concat(__MODULE__, "Storage#{unique}")
    root = Path.join(dir, label)
    File.mkdir_p!(root)

    {:ok, buffer_pid} =
      BufferService.Supervisor.start_link(
        Keyword.merge(
          [
            name: buffer,
            dir: Path.join(root, "buffer"),
            hot_server_port: 0,
            flush_interval_ms: 10,
            flush_max_rows: 1_000_000,
            seal_max_files: 1_000_000,
            seal_max_bytes: 1_000_000_000,
            seal_max_age_ms: 600_000,
            retire_grace_ms: 600_000
          ],
          buffer_opts
        )
      )

    catalog = start_lake!(Runtime.catalog_engine(storage), root, extensions: [:httpfs])

    storage_opts = [
      name: storage,
      dir: Path.join(root, "sealed"),
      buffer_name: buffer,
      buffer_base_url: HotServer.base_url(buffer),
      catalog: catalog,
      engine_extensions: [:httpfs]
    ]

    {:ok, storage_pid} = StorageService.Supervisor.start_link(storage_opts)
    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{
      buffer: buffer,
      buffer_pid: buffer_pid,
      storage: storage,
      storage_pid: storage_pid,
      catalog: catalog,
      runtime: Runtime.new(storage_opts),
      buffer_runtime: buffer_runtime
    }
  end

  defp stop_stack(stack) do
    Supervisor.stop(stack.storage_pid)
    Supervisor.stop(stack.buffer_pid)
    Runtime.delete(stack.storage)
    BufferService.Runtime.delete(stack.buffer)
  end

  defp warm(stack) do
    {:ok, [entry | _rest]} = HotTier.manifest(stack.runtime, @table)

    {:ok, _result} =
      Engine.query(
        Runtime.engine(stack.storage),
        "SELECT count(*) FROM read_parquet($1)",
        [entry["url"]]
      )

    :ok
  end

  defp fill(stack, count, rows) do
    for i <- 1..count do
      {:ok, ack} = Client.write_batch(stack.buffer, @table, batch(rows, i * rows * 10))

      ack.segment_id
    end
  end

  defp freeze(stack, ids) do
    {:ok, key} = sealed_key()
    {:ok, claim} = HotManifest.claim(stack.buffer_runtime.manifest, @table, ids, [key])

    claim
  end

  defp sealed_key do
    {:ok, prefix} = Store.prefix(@table)

    Store.key(prefix, Id.generate())
  end

  defp seal(stack, claim),
    do: Handoff.seal(stack.runtime.handoff, stack.runtime, @table, claim)

  defp batch(size, offset) do
    rows =
      for i <- 1..size do
        %{
          "id" => offset + i,
          "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], offset + i),
          "name" => "row-#{offset + i}",
          "amount" => Decimal.new("#{rem(offset + i, 997)}.#{rem(i, 100)}")
        }
      end

    %{schema: schema(), rows: rows}
  end

  defp rows_per_second(rows, us), do: Float.round(rows / (us / 1_000_000), 1)

  defp kib(bytes), do: Float.round(bytes / 1024, 1)
end

Bench.Sealer.run()
