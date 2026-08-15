defmodule Smolquery.StorageService.Compactor do
  @moduledoc """
  Re-merges undersized sealed segments so a quiet table stops accreting files.

  Eager seals and age-cap seals are the right call on the write path — they
  bound the hot tier — and their cost lands here: a table trickling writes
  seals small, and small sealed files cost every query a footer read forever.
  This process finds runs of undersized segments and replaces each run with
  one merged segment.

  Compaction is smolquery's own, never DuckLake's:
  `ducklake_merge_adjacent_files` crashes DuckDB fatally over
  externally-registered files (PL-2 findings 9/14), so the swap is built from
  this side of the catalog seam — merge through
  `Smolquery.StorageService.Merge.compact/4`, then
  `Smolquery.Catalog.replace_segments/4`, one transaction, one snapshot.

  ## Level-triggered by looking, not by signals

  The sealer is told when a table wants sealing because only the buffer knows.
  Undersized sealed segments are entirely the catalog's knowledge, so nothing
  signals a compactor — it sweeps on an interval and finds work by reading
  `segments/3`. A failed or crashed compaction needs no bookkeeping for the
  same reason: the undersized run is still there next sweep, and the sweep is
  the retry.

  This is exactly the "polling for a compaction-due signal" case Milestone 8
  L6 (PL-11 D6) calls out: `Catalog.tables/1` returns the same catalog-wide
  list to every storage node's compactor, so without a gate every node would
  plan and race to compact the same undersized run. Ownership is checked per
  table, immediately before planning it, rather than once per sweep — a sweep
  merges serially and can run for minutes, and an ownership snapshot from its
  start would widen the two-owner overlap a ring change already opens. The
  gate narrows that overlap; what makes the overlap survivable is the
  catalog's registration diff being re-derived inside every commit retry
  (`Smolquery.Catalog.DuckLake`), so the losing node's retry re-reads what
  the winner committed instead of replaying a stale swap.

  ## The policy is deliberately boring

  Per table, per sweep: segments under `compact_below_bytes` (sizes summed
  from the Parquet footers, never a data read), oldest first — segment ids are
  ULIDs, so name order is time order — greedily grouped until adding the next
  would pass `compact_max_bytes`, compacted only if at least
  `compact_min_inputs` made the cut. One group per table per sweep; a table
  with more work keeps its place in line rather than monopolizing the sweep.

  Bytes bound the group (T-248), with one safety valve. A small input-count
  cap made a small-segment backlog converge across sweeps, with each group
  re-ingesting the previous sweep's still-undersized output. That is
  quadratic write amplification for the exact case compaction exists to
  clean up. A large group is safe because the merge bounds its own engine
  calls: `Merge.compact/5` reads inputs in chunks of `merge_inputs_per_call`.
  The valve protects the sweep's time, not the engine: a group also stops at
  64 staging chunks of files (768 at the default cap), so one table of very
  small files cannot hold the sweep for hours. Re-ingestion at that width is
  negligible — a backlog past the valve converges hundreds of files per
  sweep, not twelve.

  Rows bound the group too (T-260). Compressed bytes alone do not predict
  merge cost: on ~100x-compressible data a byte-bounded group held ~25M rows,
  and the staging inserts plus the clustered `ORDER BY` on the final `COPY`
  scale with rows, so the group blew the merge's five-minute budget and
  re-planned identically every sweep. `compact_max_rows` caps the group by
  the footers' summed `num_rows`, which sizing already reads.

  The sizing query chunks the same way, oldest first, and reads each file's
  `num_rows` in the same call. Sizing stops once the undersized bytes found
  reach `compact_max_bytes`, the rows found reach `compact_max_rows`, or the
  file count reaches the valve. That halt is
  only a work bound — `group/2`'s fold owns the caps, and it alone decides
  the group. The sweep passes the group's summed row count to the merge,
  which then skips its own footer pass, and it narrows the merge's staging
  chunk when the group's average input is large, so one chunk never moves an
  unbounded number of bytes. An output still under `compact_below_bytes`
  stays a candidate and merges with future arrivals. It never re-merges
  alone; `compact_min_inputs` gates that.

  ## Compaction runs on its own engine, and recycles it after a call exit

  Sizing and merging go through `Runtime.compact_engine/1`, never the seal
  merge engine (T-259). An `Adbc.Connection` serializes its queries and a
  timed-out statement keeps running — adbc exposes no cancel — so one
  abandoned compaction merge on the shared connection starved every seal and
  every later sizing call, and each sweep stacked another abandoned query on
  top. T-251 rightly refused to kill that connection: healthy in-flight seals
  run there. A dedicated engine removes the conflict, so when a failure
  carries a `Smolquery.Engine.CallExited`, this module kills the engine's
  database process — `rest_for_one` rebuilds database and connection — waits
  briefly for the connection to re-register, and moves to the next table. The
  abandoned statement's DuckDB instance burns until it completes, but nothing
  queues behind it anymore, and a killed merge is free to retry: the output
  key is derived, so next sweep's attempt overwrites its orphan.

  ## The output key is derived, so a retry overwrites instead of duplicating

  The merged segment's id comes from the sorted input ids
  (`Smolquery.Segments.Id.derive/2`), the same identity rule the sealer's
  claims use. A compaction that crashed after writing but before the swap
  re-plans the same group next sweep, derives the same key, and overwrites its
  own orphan — which GC would otherwise sweep, since nothing ever registered
  it. Old files are never deleted here: earlier snapshots still read them, and
  physical reclaim is GC's job once no snapshot does.

  ## The swap is verified, because the failure it guards against is silent

  `drop_segments` retires a file only because the lake is attached with
  `DATA_INLINING_ROW_LIMIT 0` (PL-2 finding 13). Attached without it,
  compaction still deletes the right rows while the planner keeps listing the
  dead segments — queries get slower and nothing errors. So after every swap
  the compactor re-reads `segments/3` and fails the table loudly if a dropped
  path survived, turning a broken invariant into a logged error instead of a
  slow mystery.
  """

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime]

  @group_max_staging_chunks 64
  @stage_chunk_target_bytes 67_108_864
  @engine_recycle_wait_ms 5_000

  use Smolquery.StorageService.Sweeper, interval: :compact_interval_ms

  require Logger

  @doc """
  Starts the compactor for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.compactor(runtime.name))
  end

  @doc """
  Sweeps now, without waiting for the interval.

  Reports what was compacted and what failed, per table — the observable form
  of the policy above, and what tests assert on.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.compactor(name), :sweep, timeout)

  defp run(state) do
    with {:ok, tables} <- Catalog.tables(state.runtime.catalog) do
      outcomes = Enum.map(tables, &compact_table(state.runtime, &1))

      {:ok,
       %{
         compacted: for({:ok, swap} <- outcomes, do: swap),
         failed: for({:failed, failure} <- outcomes, do: failure)
       }}
    end
  end

  defp compact_table(runtime, table_ref) do
    started_at = System.monotonic_time(:microsecond)

    with true <- runtime.name |> Routing.resolve() |> Routing.own?(table_ref),
         {:ok, paths} <- Catalog.segments(runtime.catalog, table_ref, :current),
         {:ok, group} <- plan(runtime, paths) do
      swap(runtime, table_ref, group, started_at)
    else
      false -> :skip
      :skip -> :skip
      {:error, reason} -> failed(runtime, table_ref, reason, started_at)
    end
  end

  defp plan(runtime, paths) do
    if length(paths) < runtime.compact_min_inputs do
      :skip
    else
      with {:ok, undersized} <- undersized(runtime, Enum.sort_by(paths, &Path.basename/1)) do
        group(runtime, undersized)
      end
    end
  end

  defp undersized(runtime, paths) do
    paths
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while({[], 0, 0, 0}, &size_chunk(runtime, &1, &2))
    |> case do
      {:error, reason} -> {:error, reason}
      {chunks, _bytes, _rows, _count} -> {:ok, chunks |> Enum.reverse() |> List.flatten()}
    end
  end

  defp size_chunk(runtime, chunk, {acc, bytes, rows, count}) do
    case sizes_chunk(runtime, chunk) do
      {:ok, sizes} ->
        found =
          Enum.filter(sizes, fn {_path, size, _rows} -> size < runtime.compact_below_bytes end)

        found_bytes = Enum.sum_by(found, fn {_path, size, _rows} -> size end)
        found_rows = Enum.sum_by(found, fn {_path, _size, rows} -> rows end)

        group_filled(
          {[found | acc], bytes + found_bytes, rows + found_rows, count + length(found)},
          runtime
        )

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp group_filled({_chunks, bytes, rows, count} = acc, runtime)
       when bytes >= runtime.compact_max_bytes or rows >= runtime.compact_max_rows or
              count >= @group_max_staging_chunks * runtime.merge_inputs_per_call,
       do: {:halt, acc}

  defp group_filled(acc, _runtime), do: {:cont, acc}

  defp sizes_chunk(runtime, paths) do
    count = length(paths)

    sql =
      "SELECT sizes.file_name, sizes.bytes, files.num_rows::BIGINT " <>
        "FROM (SELECT file_name, sum(total_compressed_size)::BIGINT AS bytes " <>
        "FROM parquet_metadata([#{placeholders(paths)}]) GROUP BY file_name) sizes " <>
        "JOIN parquet_file_metadata([#{placeholders(paths, count)}]) files USING (file_name)"

    case Engine.try_query(Runtime.compact_engine(runtime.name), sql, paths ++ paths) do
      {:ok, result} ->
        {:ok, Enum.map(result.rows, fn [path, bytes, rows] -> {path, bytes, rows} end)}

      {:error, error} ->
        {:error, {:sizing_failed, error}}
    end
  end

  defp group(runtime, undersized) do
    ceiling = @group_max_staging_chunks * runtime.merge_inputs_per_call

    {group, total, row_count} =
      undersized
      |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
      |> Enum.take(ceiling)
      |> Enum.reduce_while({[], 0, 0}, fn {path, bytes, rows}, {group, total, group_rows} ->
        if (total + bytes > runtime.compact_max_bytes or
              group_rows + rows > runtime.compact_max_rows) and group != [] do
          {:halt, {group, total, group_rows}}
        else
          {:cont, {[path | group], total + bytes, group_rows + rows}}
        end
      end)

    if length(group) >= runtime.compact_min_inputs do
      {:ok, %{paths: Enum.reverse(group), row_count: row_count, bytes: total}}
    else
      :skip
    end
  end

  defp swap(runtime, table_ref, %{paths: paths, row_count: row_count} = group, started_at) do
    with {:ok, key} <- output_key(table_ref, paths),
         {:ok, segment} <-
           Merge.compact(
             %{runtime | merge_engine: Runtime.compact_engine(runtime.name)},
             table_ref,
             key,
             paths,
             row_count: row_count,
             inputs_per_call: staging_per_call(runtime, group)
           ),
         {:ok, snapshot} <-
           Catalog.replace_segments(runtime.catalog, table_ref, [segment], paths),
         :ok <- verify_retired(runtime, table_ref, paths) do
      Logger.info(fn ->
        "compacted #{length(paths)} segment(s) of #{inspect(table_ref)} " <>
          "into #{key} at snapshot #{snapshot}"
      end)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: length(paths), duration_us: elapsed_us(started_at)},
        %{result: :ok}
      )

      {:ok, %{table: table_ref, key: key, replaced: length(paths), snapshot: snapshot}}
    else
      {:error, reason} -> failed(runtime, table_ref, reason, started_at)
    end
  end

  defp output_key({dataset, table} = table_ref, paths) do
    with {:ok, ids} <- input_ids(paths),
         {:ok, prefix} <- Store.prefix(table_ref) do
      sorted = Enum.sort(ids)
      {:ok, timestamp} = sorted |> List.last() |> Id.timestamp()

      Store.key(prefix, Id.derive(timestamp, [dataset, 0, table, 0, Enum.intersperse(sorted, 0)]))
    end
  end

  defp input_ids(paths) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, ids} ->
      id = Path.basename(path, ".parquet")

      if Id.valid?(id) do
        {:cont, {:ok, [id | ids]}}
      else
        {:halt, {:error, {:not_a_segment_path, path}}}
      end
    end)
  end

  defp verify_retired(runtime, table_ref, dropped) do
    with {:ok, current} <- Catalog.segments(runtime.catalog, table_ref, :current) do
      listed = MapSet.new(current)

      case Enum.filter(dropped, &MapSet.member?(listed, &1)) do
        [] -> :ok
        survivors -> {:error, {:inputs_survived_swap, survivors}}
      end
    end
  end

  defp failed(runtime, table_ref, reason, started_at) do
    Logger.warning("compaction of #{inspect(table_ref)} failed: #{inspect(reason)}")

    recycle_on_exit(runtime, reason)

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 0, duration_us: elapsed_us(started_at)},
      %{result: :error}
    )

    {:failed, %{table: table_ref, reason: reason}}
  end

  defp recycle_on_exit(runtime, {step, %CallExited{}})
       when step in [:sizing_failed, :merge_failed] do
    engine = Runtime.compact_engine(runtime.name)

    case Process.whereis(Engine.database_name(engine)) do
      nil ->
        :ok

      database ->
        Logger.warning("recycling the compaction engine after an engine call exit")
        Process.exit(database, :kill)
        await_engine(engine, @engine_recycle_wait_ms)
    end
  end

  defp recycle_on_exit(_runtime, _reason), do: :ok

  defp await_engine(_engine, remaining_ms) when remaining_ms <= 0, do: :ok

  defp await_engine(engine, remaining_ms) do
    if Process.whereis(Engine.connection_name(engine)) do
      :ok
    else
      Process.sleep(100)
      await_engine(engine, remaining_ms - 100)
    end
  end

  defp staging_per_call(runtime, %{paths: paths, bytes: bytes}) do
    average = max(div(bytes, length(paths)), 1)

    runtime.merge_inputs_per_call
    |> min(div(@stage_chunk_target_bytes, average))
    |> max(1)
  end

  defp elapsed_us(started_at), do: System.monotonic_time(:microsecond) - started_at

  defp placeholders(paths, offset \\ 0),
    do: Enum.map_join(1..length(paths), ", ", &"$#{&1 + offset}")
end
