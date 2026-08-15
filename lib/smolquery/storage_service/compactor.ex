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
  would pass `compact_max_bytes` or `compact_max_inputs`, compacted only if at
  least `compact_min_inputs` made the cut. One group per table per sweep; a
  table with more work keeps its place in line rather than monopolizing the
  sweep.

  The input-count cap exists because the merge is one DuckDB call (T-244): the
  byte ceiling bounds the output, but 128 MiB of small segments is over a
  hundred `read_parquet` inputs, and per-input cost is what outran the
  engine's call timeout on the sandbox. The footer-sizing query is chunked by
  the same cap, oldest first, and stops at the first full group of undersized
  candidates — a large backlog costs one sizing call, not one per chunk, and
  no call holds the shared engine with an unbounded list. A merge output
  still under `compact_below_bytes` stays a candidate: a backlog converges
  across sweeps, each group re-ingesting the previous output until one
  crosses the threshold.

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
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime]

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
      {:error, reason} -> failed(table_ref, reason, started_at)
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
    |> Enum.chunk_every(runtime.compact_max_inputs)
    |> Enum.reduce_while({[], 0}, &size_chunk(runtime, &1, &2))
    |> case do
      {:error, reason} -> {:error, reason}
      {chunks, _count} -> {:ok, chunks |> Enum.reverse() |> List.flatten()}
    end
  end

  defp size_chunk(runtime, chunk, {acc, count}) do
    case sizes_chunk(runtime, chunk) do
      {:ok, sizes} ->
        found =
          Enum.filter(sizes, fn {_path, bytes} -> bytes < runtime.compact_below_bytes end)

        group_filled({[found | acc], count + length(found)}, runtime)

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp group_filled({_chunks, count} = acc, runtime)
       when count >= runtime.compact_max_inputs,
       do: {:halt, acc}

  defp group_filled(acc, _runtime), do: {:cont, acc}

  defp sizes_chunk(runtime, paths) do
    sql =
      "SELECT file_name, sum(total_compressed_size)::BIGINT " <>
        "FROM parquet_metadata([#{placeholders(paths)}]) GROUP BY file_name"

    case Engine.query(Runtime.engine(runtime.name), sql, paths) do
      {:ok, result} -> {:ok, Enum.map(result.rows, fn [path, bytes] -> {path, bytes} end)}
      {:error, error} -> {:error, {:sizing_failed, Exception.message(error)}}
    end
  end

  defp group(runtime, undersized) do
    {group, _total} =
      undersized
      |> Enum.sort_by(fn {path, _bytes} -> Path.basename(path) end)
      |> Enum.take(runtime.compact_max_inputs)
      |> Enum.reduce_while({[], 0}, fn {path, bytes}, {group, total} ->
        if total + bytes > runtime.compact_max_bytes and group != [] do
          {:halt, {group, total}}
        else
          {:cont, {[path | group], total + bytes}}
        end
      end)

    if length(group) >= runtime.compact_min_inputs do
      {:ok, Enum.reverse(group)}
    else
      :skip
    end
  end

  defp swap(runtime, table_ref, paths, started_at) do
    with {:ok, key} <- output_key(table_ref, paths),
         {:ok, segment} <- Merge.compact(runtime, table_ref, key, paths),
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
      {:error, reason} -> failed(table_ref, reason, started_at)
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

  defp failed(table_ref, reason, started_at) do
    Logger.warning("compaction of #{inspect(table_ref)} failed: #{inspect(reason)}")

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 0, duration_us: elapsed_us(started_at)},
      %{result: :error}
    )

    {:failed, %{table: table_ref, reason: reason}}
  end

  defp elapsed_us(started_at), do: System.monotonic_time(:microsecond) - started_at

  defp placeholders(paths), do: Enum.map_join(1..length(paths), ", ", &"$#{&1}")
end
