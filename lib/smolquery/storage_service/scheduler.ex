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

  ## Two lanes: every table's hour level first, then span merges on a budget

  A sweep used to take each table in listing order and plan its span level
  first, turning to the hour level only when the span level had nothing. On
  the sandbox that let ten bench tables, each failing a span merge after
  three minutes, hold every storage node for most of each sweep: no sweep
  reached `metrics.samples` in half an hour, and its backlog grew (T-603).

  So a sweep now runs two lanes. The **hour lane** runs first, over every
  due table: small, fast merges that keep file counts bounded. The **span
  lane** runs after it, over the same tables and the listing the hour lane
  already read, most span candidates first
  (`Smolquery.StorageService.Scheduler.Planner.by_need/4`), and starts no
  span merge once `compact_span_budget_ms` has passed; the tables it did not
  reach are reported as `span_waiting`. A merge already running
  finishes. Reusing the listing is safe: a span merge takes settled files,
  which an hour swap never touches, and a swap whose inputs another node
  retired meanwhile refuses in the catalog.

  Each lane backs off on its own. A table whose hour-level compaction fails
  waits in `cooling` and is not listed; a table whose span merges fail waits
  in `span_cooling` (`Smolquery.StorageService.Scheduler.Backoff.adjusted_span_cooldowns/5`)
  while its hour lane runs every sweep. A call that exits stops the lane it
  exited in, and a stopped hour lane skips the span lane: the catalog
  connection is still busy with it. The tables a stop leaves are
  `span_deferred`, kept apart from `span_waiting` (the budget's), because the
  fix for each is different. With the span level off or paused for a sweep
  (T-601), the span lane does not run at all, so its cooldowns keep their
  count instead of being cleared by plans that could not happen.

  ## A fresh tick between sweeps for quiet tables (T-627)

  A table that seals on age, not size, leaves one file per seal: about
  110 rows a minute over three write partitions was two or three files a
  minute of 1-30 rows each, and a sweep every `compact_interval_ms` let a
  dozen pile up in the current hour. Every query over recent data opened
  each one over S3: a 60-minute histogram ending now cost about 5x one
  ending 15 minutes ago, on the same row count.

  So every `compact_fresh_interval_ms` (60 s) this process runs the hour
  lane again for the tables the last sweep found quiet
  (`Smolquery.StorageService.Scheduler.Planner.fresh_tables/3`): a recent
  file under `compact_fresh_below_bytes`. It lists and plans only those, so
  a busy table costs nothing extra, and it runs in this process, so it can
  never race a sweep over the same files. Its outcomes feed the row caps,
  the quarantine and the backoff as a sweep's do; a table whose listing is
  no longer quiet leaves the set until the next sweep adds it back.

  The tick merges only files under `compact_fresh_below_bytes`, not
  `compact_below_bytes`: a quiet table's current-hour file grows with every
  merge, and without the lower floor the tick would rewrite it every minute
  up to 32 MiB. Past 4 MiB it waits for the sweep, as before. The tick
  plans with the un-gated runtime, so it takes recent files only and never
  reports the span level paused; with no quiet table it does nothing.

  What a node learns advances per tick as well as per sweep: a row cap's
  patience and the quarantine's threshold count ticks too, and a lost
  commit parks the table for `compact_interval_ms`, skipping its ticks
  until then.

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
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Backoff
  alias Smolquery.StorageService.Scheduler.Caps
  alias Smolquery.StorageService.Scheduler.Failure
  alias Smolquery.StorageService.Scheduler.Job
  alias Smolquery.StorageService.Scheduler.Planner
  alias Smolquery.StorageService.Scheduler.Quarantine
  alias Smolquery.Telemetry

  @enforce_keys [:runtime]
  defstruct [
    :runtime,
    row_caps: %{},
    span_caps: %{},
    quarantine: %{},
    quarantined_groups: MapSet.new(),
    cooldowns: %{},
    span_cooldowns: %{},
    span_widths: %{},
    fresh: MapSet.new()
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

    :ok = Merge.clear_scratch(runtime)
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
  left untouched behind a call that exited (`deferred`, T-460), and for the
  span lane the tables waiting out a span failure (`span_cooling`) and the
  ones its budget did not reach (`span_waiting`, T-603) or a call exit left
  (`span_deferred`) — the
  observable form of the policy above, and what tests assert on. A wedged table shows
  up as a non-empty `quarantined` even on a sweep where nothing else
  happens.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.scheduler(name), :sweep, timeout)

  defp run(state) do
    runtime = spill_gated(state.runtime, spill_free())

    with {:ok, tables} <- Catalog.tables(runtime.catalog) do
      {cooling, due} = Enum.split_with(tables, &Backoff.cooling_down?(state.cooldowns, &1))
      {hour, listings, deferred, stopped} = timed(:hour, fn -> hour_lane(runtime, state, due) end)
      swept = due -- deferred

      {span_cooling, span_due} =
        swept
        |> Enum.filter(&Map.has_key?(listings, &1))
        |> Enum.split_with(&Backoff.cooling_down?(state.span_cooldowns, &1))

      {span, span_swept, span_waiting, span_deferred} =
        timed(:span, fn ->
          span_lane(
            runtime,
            state,
            Planner.by_need(span_due, listings, runtime),
            listings,
            stopped
          )
        end)

      outcomes = hour ++ span
      row_caps = Caps.adjusted_row_caps(state.row_caps, outcomes, runtime.compact_max_rows)
      span_caps = Caps.adjusted_span_caps(state.span_caps, span, runtime)
      span_widths = Caps.adjusted_span_widths(state.span_widths, span)

      {quarantine, quarantined_groups} =
        Quarantine.adjusted_quarantine(
          state.quarantine,
          state.quarantined_groups,
          outcomes,
          Quarantine.threshold()
        )

      cooldowns =
        Backoff.adjusted_cooldowns(state.cooldowns, swept, hour, runtime, state.row_caps)

      span_cooldowns =
        Backoff.adjusted_span_cooldowns(state.span_cooldowns, span_swept, span, runtime)

      gauged(listings, span_waiting, span_cooling)

      report = %{
        compacted: for({:ok, swap} <- outcomes, do: swap),
        failed: for({:failed, failure} <- outcomes, do: failure),
        quarantined: quarantined_groups |> MapSet.to_list() |> Enum.sort(),
        cooling: Enum.sort(cooling),
        deferred: Enum.sort(deferred),
        span_cooling: Enum.sort(span_cooling),
        span_waiting: Enum.sort(span_waiting),
        span_deferred: Enum.sort(span_deferred)
      }

      {:ok, report,
       %{
         state
         | row_caps: row_caps,
           span_caps: span_caps,
           quarantine: quarantine,
           quarantined_groups: quarantined_groups,
           cooldowns: cooldowns,
           span_cooldowns: span_cooldowns,
           span_widths: span_widths,
           fresh: Planner.fresh_tables(listings, runtime)
       }}
    end
  end

  @doc false
  @spec on_start(%__MODULE__{}) :: %__MODULE__{}
  def on_start(state), do: schedule_fresh(state)

  @doc false
  @spec handle_tick(term(), %__MODULE__{}) :: {:noreply, %__MODULE__{}}
  def handle_tick(:fresh, state), do: {:noreply, state |> fresh_run() |> schedule_fresh()}
  def handle_tick(_message, state), do: {:noreply, state}

  defp schedule_fresh(%__MODULE__{runtime: %Runtime{compact_fresh_interval_ms: 0}} = state),
    do: state

  defp schedule_fresh(%__MODULE__{runtime: runtime} = state) do
    Process.send_after(self(), :fresh, runtime.compact_fresh_interval_ms)

    state
  end

  defp fresh_run(%__MODULE__{fresh: fresh} = state) do
    case fresh |> Enum.sort() |> Enum.reject(&Backoff.cooling_down?(state.cooldowns, &1)) do
      [] -> state
      tables -> fresh_run(state, tables)
    end
  end

  defp fresh_run(%__MODULE__{fresh: fresh} = state, tables) do
    runtime = %{
      state.runtime
      | compact_below_bytes:
          min(state.runtime.compact_below_bytes, state.runtime.compact_fresh_below_bytes)
    }

    {hour, listings, deferred, _stopped} =
      timed(:fresh, fn -> hour_lane(runtime, state, tables) end)

    swept = tables -- deferred

    {quarantine, quarantined_groups} =
      Quarantine.adjusted_quarantine(
        state.quarantine,
        state.quarantined_groups,
        hour,
        Quarantine.threshold()
      )

    still_fresh = Planner.fresh_tables(listings, runtime)

    gone =
      for table_ref <- Map.keys(listings),
          not MapSet.member?(still_fresh, table_ref),
          do: table_ref

    %{
      state
      | row_caps: Caps.adjusted_row_caps(state.row_caps, hour, runtime.compact_max_rows),
        quarantine: quarantine,
        quarantined_groups: quarantined_groups,
        cooldowns:
          Backoff.adjusted_cooldowns(state.cooldowns, swept, hour, runtime, state.row_caps),
        fresh: MapSet.difference(fresh, MapSet.new(gone))
    }
  end

  defp timed(lane, run) do
    started = System.monotonic_time(:microsecond)
    result = run.()

    :telemetry.execute(
      [:smolquery, :compact, :lane],
      %{duration_us: System.monotonic_time(:microsecond) - started},
      %{lane: lane}
    )

    result
  end

  defp gauged(listings, span_waiting, span_cooling) do
    {total, largest} =
      Enum.reduce(listings, {0, 0}, fn {_table_ref, files}, {total, largest} ->
        count = length(files)
        {total + count, max(largest, count)}
      end)

    Telemetry.put_gauge("smolquery_compaction_listed_files", [], total)
    Telemetry.put_gauge("smolquery_compaction_listed_files_max", [], largest)

    Telemetry.put_gauge(
      "smolquery_compaction_lane_tables",
      [lane: :span, state: :waiting],
      length(span_waiting)
    )

    Telemetry.put_gauge(
      "smolquery_compaction_lane_tables",
      [lane: :span, state: :cooling],
      length(span_cooling)
    )
  end

  defp hour_lane(_runtime, _state, []), do: {[], %{}, [], false}

  defp hour_lane(runtime, state, [table_ref | rest]) do
    {outcome, listing} = listed_hour(runtime, state, table_ref)

    if stops_sweep?(outcome) do
      Logger.warning(fn ->
        "compaction sweep stopped after a call exited on #{inspect(table_ref)}: " <>
          "#{length(rest)} table(s) deferred to the next sweep or fresh tick"
      end)

      {[outcome], %{}, rest, true}
    else
      {outcomes, listings, deferred, stopped} = hour_lane(runtime, state, rest)
      listings = if listing, do: Map.put(listings, table_ref, listing), else: listings

      {[outcome | outcomes], listings, deferred, stopped}
    end
  end

  defp listed_hour(runtime, state, table_ref) do
    started_at = System.monotonic_time(:microsecond)

    case exit_safe(:call_exited, fn ->
           Catalog.segment_files(runtime.catalog, table_ref, :current)
         end) do
      {:ok, files} -> {compact_lane(runtime, state, table_ref, files, :hour), files}
      {:error, reason} -> {Job.failed(runtime, table_ref, reason, started_at), nil}
    end
  end

  defp span_lane(%Runtime{compact_target_bytes: nil}, _state, _tables, _listings, _stopped),
    do: {[], [], [], []}

  defp span_lane(_runtime, _state, tables, _listings, true), do: {[], [], [], tables}

  defp span_lane(runtime, state, tables, listings, false) do
    deadline = System.monotonic_time(:millisecond) + runtime.compact_span_budget_ms
    {outcomes, swept, left} = spanned(runtime, state, tables, listings, deadline)

    case left do
      {:budget, waiting} ->
        span_waited(waiting)
        {outcomes, swept, waiting, []}

      {:stopped, deferred} ->
        {outcomes, swept, [], deferred}
    end
  end

  defp spanned(_runtime, _state, [], _listings, _deadline), do: {[], [], {:budget, []}}

  defp spanned(runtime, state, [table_ref | rest] = tables, listings, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {[], [], {:budget, tables}}
    else
      runtime
      |> compact_lane(state, table_ref, Map.fetch!(listings, table_ref), :span)
      |> spanned_after(runtime, state, table_ref, rest, listings, deadline)
    end
  end

  defp spanned_after(outcome, runtime, state, table_ref, rest, listings, deadline) do
    if stops_sweep?(outcome) do
      Logger.warning(fn ->
        "compaction span lane stopped after a call exited on #{inspect(table_ref)}: " <>
          "#{length(rest)} table(s) deferred to the next sweep"
      end)

      {[outcome], [table_ref], {:stopped, rest}}
    else
      {outcomes, swept, left} = spanned(runtime, state, rest, listings, deadline)
      {[outcome | outcomes], [table_ref | swept], left}
    end
  end

  defp span_waited([]), do: :ok

  defp span_waited(waiting) do
    Logger.info(fn ->
      "compaction span lane spent its budget; #{length(waiting)} table(s) wait for the " <>
        "next sweep (T-603)"
    end)
  end

  defp stops_sweep?({:failed, %{reason: reason}}), do: Failure.stops_sweep?(reason)
  defp stops_sweep?(_outcome), do: false

  defp compact_lane(runtime, state, table_ref, files, lane) do
    runtime = Caps.table_capped(runtime, state.row_caps, table_ref)
    span_cap = Map.get(state.span_caps, table_ref, runtime.compact_target_bytes)

    lane = %{
      name: lane,
      span_cap: span_cap,
      started_at: System.monotonic_time(:microsecond),
      failure: lane_failure(lane, span_cap)
    }

    case exit_safe(:call_exited, fn -> planned(runtime, state, table_ref, files, lane) end) do
      {:error, reason} -> Job.failed(runtime, table_ref, reason, lane.started_at, lane.failure)
      outcome -> outcome
    end
  end

  defp planned(runtime, state, table_ref, files, lane) do
    planning = %{
      routing: Routing.resolve(runtime.name),
      quarantined_groups: state.quarantined_groups,
      learned_width: Map.get(state.span_widths, table_ref),
      files: files
    }

    with {:ok, group} <-
           Planner.plan(runtime, planning, table_ref, lane.span_cap, wall_ms(), lane.name),
         :ok <- refuse_tombstoned(runtime, table_ref, group) do
      Job.run(runtime, table_ref, group, lane.started_at)
    else
      :not_owned ->
        :skip

      :skip ->
        skipped(lane.name, table_ref)

      {:error, reason} ->
        Job.failed(runtime, table_ref, reason, lane.started_at, lane.failure)

      {:error, reason, failed_paths} ->
        Job.failed(
          runtime,
          table_ref,
          reason,
          lane.started_at,
          [paths: failed_paths] ++ lane.failure
        )
    end
  end

  defp skipped(:hour, table_ref), do: {:skip, table_ref}
  defp skipped(:span, _table_ref), do: :skip

  defp lane_failure(:hour, _span_cap), do: []
  defp lane_failure(:span, span_cap), do: [level: :span, span_cap: span_cap]

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

  defp spill_free do
    free = Smolquery.DiskSpace.free_bytes(Runtime.spill_root())

    with {:ok, bytes} <- free,
         do: Telemetry.put_gauge("smolquery_compaction_spill_free_bytes", [], bytes)

    free
  end

  defp spill_gated(%Runtime{compact_target_bytes: nil} = runtime, _free), do: runtime

  defp spill_gated(runtime, free) do
    case span_pause(runtime, free) do
      :ok ->
        runtime

      {reason, detail} ->
        Logger.warning("compaction span level paused for this sweep: #{detail} (T-601)")
        :telemetry.execute([:smolquery, :compact, :span_paused], %{count: 1}, %{reason: reason})
        %{runtime | compact_target_bytes: nil}
    end
  end

  defp span_pause(runtime, free) do
    root = Runtime.spill_root()

    case {Engine.abandoned_spill(Runtime.compact_engine(runtime.name)), free} do
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
