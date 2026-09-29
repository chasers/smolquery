defmodule Smolquery.StorageService.Scheduler do
  @moduledoc """
  Re-merges undersized sealed segments so a quiet table stops accreting files:
  the storage service's compaction scheduler.

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
  signals the scheduler — it sweeps on an interval and finds work by reading
  `segment_files/3`. A failed or crashed compaction needs no bookkeeping for the
  same reason: the undersized run is still there next sweep, and the sweep is
  the retry.

  This is exactly the "polling for a compaction-due signal" case Milestone 8
  L6 (PL-11 D6) calls out: `Catalog.tables/1` returns the same catalog-wide
  list to every storage node's scheduler, so without a gate every node would
  plan and race to compact the same undersized run.

  A sweep also runs without its span level, hour level only, when spilling
  is unsafe: while a recycled compaction engine's abandoned merge still
  writes to a spill directory (`Smolquery.Engine.abandoned_spill/1`; adbc cannot cancel
  it, and deleting its files under it crashed the VM in T-460), or while the
  spill filesystem has less than `compact_spill_floor_bytes` free. Each
  compaction engine instance also sizes its own temp cap from free space as
  it starts (`Runtime.compact_spill_cap/2`), so a rebuilt engine takes a share
  of what the abandoned one left rather than another fixed 32 GiB.

  ## Compaction commits through its own catalog connection

  The catalog engine carries a connection reserved for compaction
  (`Runtime.compaction_catalog/1`, T-458). The swap's transaction holds its
  connection for as long as `ducklake_add_data_files` takes to read the
  merged file's footer, and `Merge.compact/5` reads the schema and the
  snapshot's file list through the same handle; on the one connection every
  seal commit shared, `SELECT 1` waited 34.77 s behind a compacting node
  against the seal's 30 s call timeout. Sealing's catalog calls stay on the
  first connection, where nothing of compaction's queues ahead of them.

  ## A catalog call that exits is a failure, not a crash

  Every engine call in sizing and merging goes through
  `Smolquery.Engine.try_query/4`, but the catalog's calls — the listing, the
  merge's schema and snapshot reads, and the swap's transaction — are plain
  `GenServer.call`s, and each can exit: the swap's `ducklake_add_data_files`
  outlasted its call timeout on every attempt of one table, and the exit took
  the whole scheduler down with it. A crash forgets the backoff, the row caps
  and the quarantine, so the same group was re-planned and re-merged next
  sweep and timed out again, forty-five times in ten hours, while each
  abandoned transaction kept running on the catalog connection (T-460). The
  catalog now answers such an exit as `{:error, %CallExited{}}` itself
  (`Smolquery.Catalog.DuckLake`, T-464), so a listing that exits fails the
  sweep with that error and a read or the swap that exits fails its table
  with it. One catch remains here, for an exit the catalog never sees: a
  store put whose HTTP pool died mid-upload becomes
  `{:call_exited, %CallExited{}}`. Both back the table off like any other
  failure and neither recycles the compaction engine, whose statement did
  not exit.

  The sweep also stops at the first such exit. The call that exited is still
  running on the catalog's compaction connection, which serializes its
  callers, so every later table's first catalog read would queue behind it,
  exit at its own timeout, and be backed off for a failure that is not its
  own. The tables behind the exit are reported as `deferred` — untouched, no
  cooldown counted — and the next sweep finds them where they are.

  ## The parts

    * `Smolquery.StorageService.Scheduler.Planner`: which files of a table to
      merge next, at which level, and how many.
    * `Smolquery.StorageService.Scheduler.Job`: one merge and its swap.
    * `Smolquery.StorageService.Scheduler.Caps`, `Backoff` and `Quarantine`:
      what a node learns from failures, sweep to sweep.
    * `Smolquery.StorageService.Scheduler.Failure`: which kind of failure a
      failure is.
  """

  alias Smolquery.BufferService.Client, as: BufferClient
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Backoff
  alias Smolquery.StorageService.Scheduler.Caps
  alias Smolquery.StorageService.Scheduler.Failure
  alias Smolquery.StorageService.Scheduler.Job
  alias Smolquery.StorageService.Scheduler.Planner
  alias Smolquery.StorageService.Scheduler.Quarantine

  @enforce_keys [:runtime]
  defstruct [
    :runtime,
    row_caps: %{},
    span_caps: %{},
    quarantine: %{},
    quarantined_groups: MapSet.new(),
    cooldowns: %{}
  ]

  use Smolquery.StorageService.Sweeper, interval: :compact_interval_ms

  require Logger

  @doc """
  Starts the scheduler for a runtime.

  The scheduler reads and commits through the catalog engine's compaction
  connection (`Runtime.compaction_catalog/1`), taken here once, so a swap
  holding it for minutes queues no seal commit behind it (T-458).
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    runtime = %{
      Runtime.with_compact_max_rows(runtime)
      | catalog: compaction_catalog(runtime)
    }

    GenServer.start_link(__MODULE__, runtime, name: Runtime.scheduler(runtime.name))
  end

  # A catalog handed to the storage service outright was started by its
  # caller, on an engine that may carry one connection; a lake the service
  # runs itself always carries the compaction connection. Without it the
  # compactor shares the seal side's, as it did before T-458, and says so —
  # a compaction that crashed on every sweep would be worse than one that
  # queues seals.
  defp compaction_catalog(runtime) do
    catalog = Runtime.compaction_catalog(runtime)

    case Map.get(catalog.config, :engine) do
      {engine, slot} when is_atom(engine) and is_integer(slot) ->
        if Process.whereis(Engine.connection_name(engine, slot)) do
          catalog
        else
          Logger.warning(
            "the catalog engine #{inspect(engine)} carries no connection #{slot} for " <>
              "compaction; compaction shares the seal side's connection (T-458)"
          )

          runtime.catalog
        end

      _not_a_pooled_engine ->
        catalog
    end
  end

  @doc """
  Sweeps now, without waiting for the interval.

  Reports what was compacted and what failed, per table, plus the groups this
  node currently quarantines and the tables it left out of this sweep
  because their last compaction failed (`cooling`, T-458), and the tables it
  left untouched behind a call that exited (`deferred`, T-460) — the
  observable form of the policy above, and what tests assert on. A wedged table shows
  up as a non-empty `quarantined` even on a sweep where nothing else
  happens.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.scheduler(name), :sweep, timeout)

  defp run(state) do
    runtime = spill_gated(state.runtime)

    with {:ok, tables} <- Catalog.tables(runtime.catalog) do
      {cooling, due} = Enum.split_with(tables, &Backoff.cooling_down?(state.cooldowns, &1))
      {outcomes, deferred} = sweep_due(runtime, state, due)
      swept = due -- deferred

      row_caps = Caps.adjusted_row_caps(state.row_caps, outcomes, runtime.compact_max_rows)
      span_caps = Caps.adjusted_span_caps(state.span_caps, outcomes, runtime)

      {quarantine, quarantined_groups} =
        Quarantine.adjusted_quarantine(
          state.quarantine,
          state.quarantined_groups,
          outcomes,
          Quarantine.threshold()
        )

      cooldowns =
        Backoff.adjusted_cooldowns(state.cooldowns, swept, outcomes, runtime, state.row_caps)

      report = %{
        compacted: for({:ok, swap} <- outcomes, do: swap),
        failed: for({:failed, failure} <- outcomes, do: failure),
        quarantined: quarantined_groups |> MapSet.to_list() |> Enum.sort(),
        cooling: Enum.sort(cooling),
        deferred: Enum.sort(deferred)
      }

      {:ok, report,
       %{
         state
         | row_caps: row_caps,
           span_caps: span_caps,
           quarantine: quarantine,
           quarantined_groups: quarantined_groups,
           cooldowns: cooldowns
       }}
    end
  end

  defp compact_table(runtime, state, table_ref) do
    runtime = Caps.table_capped(runtime, state.row_caps, table_ref)
    span_cap = Map.get(state.span_caps, table_ref, runtime.compact_target_bytes)
    compact_capped(runtime, state.quarantined_groups, span_cap, table_ref)
  end

  defp sweep_due(_runtime, _state, []), do: {[], []}

  defp sweep_due(runtime, state, [table_ref | rest]) do
    outcome = compact_table(runtime, state, table_ref)

    if call_exited?(outcome) do
      Logger.warning(fn ->
        "compaction sweep stopped after a call exited on #{inspect(table_ref)}: " <>
          "#{length(rest)} table(s) deferred to the next sweep"
      end)

      {[outcome], rest}
    else
      {outcomes, deferred} = sweep_due(runtime, state, rest)
      {[outcome | outcomes], deferred}
    end
  end

  defp call_exited?({:failed, %{reason: reason}}), do: Failure.stops_sweep?(reason)
  defp call_exited?(_outcome), do: false

  defp compact_capped(runtime, quarantined_groups, span_cap, table_ref) do
    started_at = System.monotonic_time(:microsecond)

    case exit_safe(:call_exited, fn ->
           compact_listed(runtime, quarantined_groups, span_cap, table_ref, started_at)
         end) do
      {:error, reason} -> Job.failed(runtime, table_ref, reason, started_at)
      outcome -> outcome
    end
  end

  defp compact_listed(runtime, quarantined_groups, span_cap, table_ref, started_at) do
    routing = Routing.resolve(runtime.name)
    planning = %{routing: routing, quarantined_groups: quarantined_groups, files: nil}

    with {:ok, files} <- Catalog.segment_files(runtime.catalog, table_ref, :current),
         {:ok, group} <-
           Planner.plan(runtime, %{planning | files: files}, table_ref, span_cap, wall_ms()),
         :ok <- refuse_tombstoned(runtime, table_ref, group) do
      Job.run(runtime, table_ref, group, started_at)
    else
      :not_owned ->
        :skip

      :skip ->
        {:skip, table_ref}

      {:error, reason} ->
        Job.failed(runtime, table_ref, reason, started_at)

      {:error, reason, failed_paths} ->
        Job.failed(runtime, table_ref, reason, started_at, paths: failed_paths)
    end
  end

  defp exit_safe(step, call) do
    call.()
  catch
    :exit, reason -> {:error, {step, CallExited.new(reason)}}
  end

  # A registered segment under a tombstoned key is a released claim's orphan
  # awaiting reconciliation (T-386): merging it would bake its rows into the
  # compacted output past the reconciler's reach, permanently double-counted.
  # An unreachable buffer answers no tombstones, so compaction proceeds — a
  # deployment compacting tables with no live buffer keeps working, at the
  # cost of re-opening this window only while the whole buffer tier is down.
  defp refuse_tombstoned(runtime, table_ref, %{paths: paths}) do
    tombstoned = tombstoned_paths(runtime, table_ref)

    if tombstoned != [] and Enum.any?(paths, &(&1 in tombstoned)) do
      Logger.info(fn ->
        "compaction of #{inspect(table_ref)} deferred: the group holds a released " <>
          "claim's segment awaiting reconciliation (T-386)"
      end)

      :skip
    else
      :ok
    end
  end

  defp tombstoned_paths(runtime, table_ref) do
    case BufferClient.tombstones(runtime.buffer_name, table_ref) do
      {:ok, keys} -> Enum.map(keys, &Store.location(runtime.store, &1))
      {:error, _unreachable} -> []
    end
  catch
    _kind, _reason -> []
  end

  defp spill_gated(%Runtime{compact_target_bytes: nil} = runtime), do: runtime

  defp spill_gated(runtime) do
    case span_pause(runtime) do
      :ok ->
        runtime

      {reason, detail} ->
        Logger.warning("compaction span level paused for this sweep: #{detail} (T-601)")
        :telemetry.execute([:smolquery, :compact, :span_paused], %{count: 1}, %{reason: reason})
        %{runtime | compact_target_bytes: nil}
    end
  end

  defp span_pause(runtime) do
    root = Runtime.spill_root()

    case {Engine.abandoned_spill(Runtime.compact_engine(runtime.name)),
          Smolquery.DiskSpace.free_bytes(root)} do
      {[_ | _] = leaves, _free} ->
        {:abandoned_spill,
         "a recycled compaction engine still spills to #{Enum.join(leaves, ", ")}"}

      {[], {:ok, free}} when free < runtime.compact_spill_floor_bytes ->
        {:spill_floor,
         "#{free} bytes free under #{root}, below compact_spill_floor_bytes " <>
           "#{runtime.compact_spill_floor_bytes}"}

      _room ->
        :ok
    end
  end

  defp wall_ms, do: System.os_time(:millisecond)
end
