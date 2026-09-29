defmodule Smolquery.StorageService.Scheduler.Job do
  @moduledoc """
  One compaction: merge a planned group through
  `Smolquery.StorageService.Merge.compact/5`, then swap it in with
  `Smolquery.Catalog.replace_segments/4`, one snapshot.

  ## Compaction runs on its own engine, and recycles it after a call exit

  Sizing and merging go through `Runtime.compact_engine/1`, never the seal
  merge engine (T-259). An `Adbc.Connection` serializes its queries and a
  timed-out statement keeps running — adbc exposes no cancel — so one
  abandoned compaction merge on the shared connection starved every seal and
  every later sizing call, and each sweep stacked another abandoned query on
  top. T-251 rightly refused to kill that connection: healthy in-flight seals
  run there. A dedicated engine removes the conflict, so when a failure
  carries a `Smolquery.Engine.CallExited`, `failed/5` kills the engine's
  database process — `rest_for_one` rebuilds database and connection — waits
  briefly for a replacement connection to register (the old registration is
  not the signal: the supervisor tears it down asynchronously, so only a new
  pid proves the rebuild), and moves to the next table. The
  abandoned statement's DuckDB instance burns until it completes, but nothing
  queues behind it anymore, and a killed merge is free to retry: the output
  key is derived, so next sweep's attempt converges on the same key.

  The rebuilt instance must not share the burning one's spill directory:
  `Smolquery.Engine.Connection` names each instance's leaf after the
  database process, because under one shared leaf the rebuilt engine's
  first spilling merge overwrote and deleted the abandoned sort's temp
  files, and reading them back segfaulted the VM on every storage node
  within hours (T-460).

  ## The output key is derived, so a retry converges instead of duplicating

  The merged segment's id comes from the sorted input ids
  (`Smolquery.Segments.Id.derive/2`), the same identity rule the sealer's
  claims use. A compaction that crashed after writing but before the swap
  re-plans the same group next sweep, derives the same key, and finds its own
  orphan already committed — the store's write-once put reports that as
  success (T-308), and the swap registers the orphan, a complete segment the
  crashed run validated before committing. Old files are never deleted here:
  earlier snapshots still read them, and physical reclaim is GC's job once no
  snapshot does.

  ## The swap is verified, because the failure it guards against is silent

  `drop_segments` retires a file only because the lake is attached with
  `DATA_INLINING_ROW_LIMIT 0` (PL-2 finding 13). Attached without it,
  compaction still deletes the right rows while the planner keeps listing the
  dead segments — queries get slower and nothing errors. So after every swap
  the job re-reads `segments/3` and fails the table loudly if a dropped
  path survived, turning a broken invariant into a logged error instead of a
  slow mystery.
  """

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Failure

  @stage_chunk_target_bytes 67_108_864
  @engine_recycle_wait_ms 5_000

  @doc """
  Merges `group` into one segment and swaps it in for its inputs, verified.
  `{:ok, report}` on success; `{:failed, failure}` otherwise, the failure
  carrying what the scheduler's caps, backoff and quarantine read.
  """
  @spec run(Runtime.t(), Catalog.table_ref(), map(), integer()) :: {:ok, map()} | {:failed, map()}
  def run(runtime, table_ref, %{paths: paths, row_count: row_count} = group, started_at) do
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
         {:ok, snapshot} <- swapped(runtime, table_ref, segment, paths) do
      Logger.info(fn ->
        "compacted #{length(paths)} segment(s) of #{inspect(table_ref)} " <>
          "into #{key} at snapshot #{snapshot}"
      end)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: length(paths), duration_us: elapsed_us(started_at)},
        %{result: :ok, table_ref: table_ref, level: group.level}
      )

      {:ok,
       %{
         table: table_ref,
         key: key,
         replaced: length(paths),
         rows: row_count,
         snapshot: snapshot,
         level: group.level
       }}
    else
      {:error, reason} ->
        failed(runtime, table_ref, reason, started_at,
          rows: row_count,
          paths: paths,
          level: group.level,
          span_cap: group.span_cap,
          width: Map.get(group, :width)
        )
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

  defp swapped(runtime, table_ref, segment, paths) do
    with {:ok, snapshot} <- Catalog.replace_segments(runtime.catalog, table_ref, [segment], paths),
         :ok <- verify_retired(runtime, table_ref, paths) do
      {:ok, snapshot}
    end
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

  @doc """
  A compaction failure, logged and reported: the table, the reason, and what
  of `:rows`, `:paths`, `:level`, `:span_cap` and `:width` the caller knows. Recycles
  the compaction engine first when the reason is an engine call exit.
  """
  @spec failed(Runtime.t(), Catalog.table_ref(), term(), integer(), keyword()) ::
          {:failed, map()}
  def failed(runtime, table_ref, reason, started_at, opts \\ []) do
    Logger.warning("compaction of #{inspect(table_ref)} failed: #{inspect(reason)}")

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 0, duration_us: elapsed_us(started_at)},
      %{result: :error, table_ref: table_ref, level: Keyword.get(opts, :level, :hour)}
    )

    recycle_on_exit(runtime, reason)

    failure =
      opts
      |> Keyword.take([:rows, :level, :span_cap, :width])
      |> Map.new()
      |> Map.merge(%{table: table_ref, reason: reason, paths: Keyword.get(opts, :paths, [])})

    {:failed, failure}
  end

  defp recycle_on_exit(runtime, reason) do
    if Failure.engine_call_exited?(reason) do
      engine = Runtime.compact_engine(runtime.name)
      stale_connection = Process.whereis(Engine.connection_name(engine))

      case Process.whereis(Engine.database_name(engine)) do
        nil ->
          await_engine(engine, stale_connection, @engine_recycle_wait_ms)

        database ->
          Logger.warning("recycling the compaction engine after an engine call exit")
          Process.exit(database, :kill)
          await_engine(engine, stale_connection, @engine_recycle_wait_ms)
      end
    else
      :ok
    end
  end

  defp await_engine(_engine, _stale_connection, remaining_ms) when remaining_ms <= 0, do: :ok

  defp await_engine(engine, stale_connection, remaining_ms) do
    connection = Process.whereis(Engine.connection_name(engine))

    if is_pid(connection) and connection != stale_connection do
      :ok
    else
      Process.sleep(100)
      await_engine(engine, stale_connection, remaining_ms - 100)
    end
  end

  defp staging_per_call(runtime, %{paths: paths, bytes: bytes}) do
    average = max(div(bytes, length(paths)), 1)

    runtime.merge_inputs_per_call
    |> min(div(@stage_chunk_target_bytes, average))
    |> max(1)
  end

  defp elapsed_us(started_at), do: System.monotonic_time(:microsecond) - started_at
end
