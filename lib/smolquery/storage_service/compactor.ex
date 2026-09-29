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
  `segment_files/3`. A failed or crashed compaction needs no bookkeeping for the
  same reason: the undersized run is still there next sweep, and the sweep is
  the retry.

  This is exactly the "polling for a compaction-due signal" case Milestone 8
  L6 (PL-11 D6) calls out: `Catalog.tables/1` returns the same catalog-wide
  list to every storage node's compactor, so without a gate every node would
  plan and race to compact the same undersized run.

  ## Ownership is per time bucket, not per table

  The gate used to check `Routing.own?(table_ref)`, which bounded compaction
  parallelism by table count: one node owned a hot table's whole backlog,
  carried every merge's memory pressure alone, and OOMed while its peers
  idled (T-269). The unit of ownership is now `{table_ref, bucket}`, where a
  segment's bucket is its ULID timestamp divided by `compact_bucket_ms`.
  Disjointness holds only while every storage node runs the same
  `compact_bucket_ms`: divergent values during a rolling config change give
  two nodes overlapping owned sets until the rollout completes, so change
  the value fleet-wide, not node by node. The overlap is survivable (below)
  but wasteful while it lasts.

  Every node lists every table's segments each sweep — the listing is what
  makes owned buckets knowable, so fleet-wide listing load is N x T catalog
  queries per interval where the per-table gate cost T. The listing is
  metadata-only, but its cost scales with segment count; sizing and merging
  stay owned-only, so footer I/O and merge work spread across the fleet.

  Ownership is snapshotted once per table, before sizing: sizing a large
  backlog takes chunked engine calls and can run for minutes, so a ring
  change mid-scan re-opens the two-owner overlap for later buckets — the
  same window the per-table gate had across a sweep. What makes the overlap
  survivable is the catalog's registration diff being re-derived inside
  every commit retry (`Smolquery.Catalog.DuckLake`), so the losing node's
  retry re-reads what the winner committed instead of replaying a stale
  swap.

  A group crosses a bucket boundary in exactly one case: a bucket that
  cannot meet `compact_min_inputs` alone rolls its candidates forward into
  this node's next owned bucket, oldest first, so a quiet table that seals
  one small file per bucket still compacts instead of accreting forever.
  Cross-node disjointness holds — a carry only ever holds this node's own
  candidates — and everything else stays bucket-local, so merged output
  stays as time-local as its inputs and min/max pruning does not erode. A
  path whose basename is not a ULID has no bucket, so it silently stops
  being a compaction candidate (it could never derive an output key anyway).
  The merged segment's id derives from its newest input's timestamp, so a
  still-undersized output lands back in its own bucket and the same node
  keeps converging it.

  ## The policy is deliberately boring

  Per table, per sweep: segments under `compact_below_bytes` (sizes summed
  from the Parquet footers, never a data read), oldest first — segment ids are
  ULIDs, so name order is time order — greedily grouped until adding the next
  would pass `compact_max_bytes`, compacted only if at least
  `compact_min_inputs` made the cut, all within one owned bucket, oldest
  owned bucket first. One group per table per sweep per node; a table with
  more work keeps its place in line rather than monopolizing the sweep, and
  a fleet of N owners advances up to N of its buckets per sweep.

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
  the footers' summed `num_rows`, which sizing already reads. A head file no
  neighbor fits beside under the row cap cannot wedge the table: a group
  smaller than `compact_min_inputs` drops its head and regroups from the next
  candidate, so the small files behind a row-heavy head still merge. The byte
  cap never needs this — two files under `compact_below_bytes` always fit —
  but a single small file can carry more rows than the cap admits twice.

  The row cap also adapts per table: a merge OOM halves the table's cap, and
  evidence at the tightened cap earns it back, because no static
  bytes-per-row constant fits every workload — see `adjusted_row_caps/3`
  (T-262).

  The catalog screens the candidates before any footer is opened (T-463).
  `Catalog.segment_files/3` carries the whole-file size DuckLake recorded at
  registration, and a file at or above `compact_below_bytes` by that measure
  is never a candidate — its compressed-data sum is smaller still, so the
  screen only ever tightens the threshold by a footer's width. Without it,
  every owned segment's footer was read on every sweep, and once the
  engines stopped caching file reads (T-461) a table with nothing to
  compact cost two store requests per file per sweep, forever. A size the
  catalog does not know is recorded as `0`, which is under any threshold,
  so the footer decides for that file. Footers are still read for the
  candidates: sizing is where a corrupt file first fails, and the
  quarantine keys on that.

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

  ## Files settle at a target size, two levels, as soon as they can (T-592)

  The policy above tops out at `compact_max_bytes` inside an hour bucket, so
  on a table that seals small files often it tops out much lower: at about
  6 MB, the 1 h bucket held `metrics.samples` to 168 files a week, and a
  backlog of 16k never converged (T-588). Every query pays per file.

  So compaction runs at two levels, split by age. The **hour level** is the
  policy above, over the current bucket and the one before it. Every file
  older than one more bucket is **settled**, and the **span level** merges
  settled files under half of `compact_target_bytes` toward the target:
  grouped within one `compact_span_ms` (a day), never carried across spans,
  capped by the target's bytes and by no row count, because the merge
  spills (T-591) and a row cap would hold a day of small rows to a fraction
  of the target. A file therefore reaches the target about two buckets after
  its data arrived, not once its day closes, at the cost of rewriting a day
  file still under half the target about once a bucket until it gets there:
  for a table writing less than that in a day, up to about 24 rewrites of a
  growing file, around 12 times the day's bytes. A table that seals at most
  one file in a span keeps one file per span, since no merge crosses a span.

  The bucket between the levels belongs to neither, so a merge the hour level
  planned has usually finished before the span level can plan any of its
  files. Usually is not enough when rows would count twice, so correctness
  does not rest on it: a swap whose inputs another swap already retired
  refuses in the catalog and commits nothing (`Smolquery.Catalog.DuckLake`),
  the next sweep replans from the current files, and that refusal never backs
  the table off.

  ## A span merge is sized by what it decodes to, and pauses when the disk is short

  Bytes on disk do not bound a merge. The `bench.clickstack_*` tables keep
  attribute bags as `MAP` or `VARIANT`: about 4 bytes a row on disk and about
  3 KiB in a merge. A 1 GiB span group of them was hundreds of gigabytes of
  sort, so every span merge spilled to DuckDB's 32 GiB temp cap and filled
  the storage nodes' disks within ten minutes of the roll (T-601).

  So the span level also caps a group's rows at `compact_span_decoded_bytes`
  over an estimated decoded row width: the mean text width of up to
  1,024 rows of the group's first file, the one read this
  costs per plan. Text is a proxy for what DuckDB holds, not a measure of it;
  the span cap halving below remains the correction when it guesses low.

  A sweep also runs without its span level, hour level only, when spilling
  is unsafe: while a recycled compaction engine's abandoned merge still holds
  a spill directory (`Smolquery.Engine.abandoned_spill/1`; adbc cannot cancel
  it, and deleting its files under it crashed the VM in T-460), or while the
  spill filesystem has less than `compact_spill_floor_bytes` free. Each
  compaction engine instance also sizes its own temp cap from free space as
  it starts (`Runtime.compact_spill_cap/2`), so a rebuilt engine takes a share
  of what the abandoned one left rather than another fixed 32 GiB.

  A failing span group does not re-run as it was. Its merge is 1 GiB with no
  row cap, so the lever is bytes: an OOM, an engine call exit or a swap timeout
  halves the table's span cap, never below `compact_max_bytes`, and the table
  does not back off while the cap can still shrink; see
  `adjusted_span_caps/3`. A span failure leaves the hour level's row cap
  alone. Span-level work is owned per
  `{table_ref, {:span, span}}`, the way the hour level is per bucket, so a
  backlog of days spreads across the fleet. Only when the span level has
  nothing to do does the sweep turn to the hour level, still one group per
  table per sweep. A `compact_target_bytes` of `nil` turns the span level
  off and leaves every file at the hour level, as before.

  ## A failing table backs off instead of re-running every sweep

  "The sweep is the retry" is the right shape for a crash, and the wrong
  one for a merge that fails the same way every time. A group that OOMs
  under a row cap already at its floor, or whose put keeps failing, re-ran
  every `compact_interval_ms` for as long as it failed, and each attempt
  was minutes of merge that held a catalog connection: on the sandbox the
  seals queued behind it timed out, so a compacting node could not seal and
  the buffer backlog it was meant to drain grew instead (T-458). Now a
  failed table waits `compact_backoff_base_ms`, doubling per consecutive
  failure up to `compact_backoff_max_ms`, before the sweep looks at it
  again — `adjusted_cooldowns/5` — and a success clears the wait. The
  quarantine (below) is the stop for a *corrupt* input; this is the pace
  for everything else.

  ## Compaction commits through its own catalog connection

  The catalog engine carries a connection reserved for compaction
  (`Runtime.compaction_catalog/1`, T-458). The swap's transaction holds its
  connection for as long as `ducklake_add_data_files` takes to read the
  merged file's footer, and `Merge.compact/5` reads the schema and the
  snapshot's file list through the same handle; on the one connection every
  seal commit shared, `SELECT 1` waited 34.77 s behind a compacting node
  against the seal's 30 s call timeout. Sealing's catalog calls stay on the
  first connection, where nothing of compaction's queues ahead of them.

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

  ## A catalog call that exits is a failure, not a crash

  Every engine call in sizing and merging goes through
  `Smolquery.Engine.try_query/4`, but the catalog's calls — the listing, the
  merge's schema and snapshot reads, and the swap's transaction — are plain
  `GenServer.call`s, and each can exit: the swap's `ducklake_add_data_files`
  outlasted its call timeout on every attempt of one table, and the exit took
  the whole compactor down with it. A crash forgets the backoff, the row caps
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
  the compactor re-reads `segments/3` and fails the table loudly if a dropped
  path survived, turning a broken invariant into a logged error instead of a
  slow mystery.
  """

  alias Smolquery.BufferService.Client, as: BufferClient
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [
    :runtime,
    row_caps: %{},
    span_caps: %{},
    quarantine: %{},
    quarantined_groups: MapSet.new(),
    cooldowns: %{}
  ]

  @group_max_staging_chunks 64
  @span_max_rows 9_223_372_036_854_775_807
  @stage_chunk_target_bytes 67_108_864
  @engine_recycle_wait_ms 5_000
  @quarantine_after 5
  @width_sample_rows 1024

  @typedoc """
  A table left out of the sweep until `retry_at`, after `consecutive`
  failures and, when it last lost a commit, `conflicts` of those in a row.
  """
  @type cooldown :: %{
          required(:consecutive) => non_neg_integer(),
          required(:retry_at) => integer(),
          optional(:conflicts) => pos_integer()
        }

  @stuck_after 5
  @conflicts_warn_after 3

  use Smolquery.StorageService.Sweeper, interval: :compact_interval_ms

  require Logger

  @doc """
  Starts the compactor for a runtime.

  The compactor reads and commits through the catalog engine's compaction
  connection (`Runtime.compaction_catalog/1`), taken here once, so a swap
  holding it for minutes queues no seal commit behind it (T-458).
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    runtime = %{
      Runtime.with_compact_max_rows(runtime)
      | catalog: compaction_catalog(runtime)
    }

    GenServer.start_link(__MODULE__, runtime, name: Runtime.compactor(runtime.name))
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
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.compactor(name), :sweep, timeout)

  defp run(state) do
    runtime = spill_gated(state.runtime)

    with {:ok, tables} <- Catalog.tables(runtime.catalog) do
      {cooling, due} = Enum.split_with(tables, &cooling_down?(state.cooldowns, &1))
      {outcomes, deferred} = sweep_due(runtime, state, due)
      swept = due -- deferred

      row_caps = adjusted_row_caps(state.row_caps, outcomes, runtime.compact_max_rows)
      span_caps = adjusted_span_caps(state.span_caps, outcomes, runtime)

      {quarantine, quarantined_groups} =
        adjusted_quarantine(
          state.quarantine,
          state.quarantined_groups,
          outcomes,
          @quarantine_after
        )

      cooldowns = adjusted_cooldowns(state.cooldowns, swept, outcomes, runtime, state.row_caps)

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

  @doc """
  The per-table cooldowns after a sweep — a failing table's compaction backs
  off instead of re-running every sweep (T-458).

  The sweep is the compactor's retry, and a table whose merge fails the same
  way every time — an OOM the row cap cannot fix, a store put that keeps
  failing — used to re-run it every `compact_interval_ms` for as long as it
  failed: minutes of merge and a held catalog connection per attempt, with
  no path out but the operator. Every table in `swept` whose failure has no
  recovery of its own backs off; every other swept table's cooldown is
  cleared, so one success — or nothing left to do — starts the next
  failure's count from one. Tables the sweep left out because they were
  cooling are not in `swept` and keep their entry. The wait is
  `Smolquery.Backoff.exponential/3` over the runtime's
  `compact_backoff_base_ms` and `compact_backoff_max_ms`; a base of `0`
  puts `retry_at` in the past and never leaves a table out.

  Two failures already have a recovery that depends on the very next sweep
  retrying, and neither backs off: a merge OOM while the table's row cap is
  still above the floor, which `adjusted_row_caps/3` halves the cap on and
  must see again to calibrate (T-262), and a corruption-shaped failure,
  which `adjusted_quarantine/4` counts toward the quarantine that stops it
  (T-310). What backs off is the rest: an OOM at the floor, where halving
  has nothing left to give; an engine call exit, which ran the merge for
  its whole budget; a store put, an invariant check.
  `row_caps` is the state before the sweep, so the cap the OOM ran under
  is the one judged.

  A catalog commit conflict (`:commit_conflict`) is neither: it is
  contention with the table's own seals, not a failure that repeats, and the
  catalog has already retried it five times inside one call. Counted toward
  `consecutive`, it drove `metrics.samples` into the 4 h stall within a day
  on conflicts alone (T-588, T-595). So a conflict waits one
  `compact_interval_ms`, one or two sweeps depending on where the next one
  falls, and leaves `consecutive` as it was: a real failure after a run of
  conflicts still starts at one. The entry counts the conflicts in a row;
  each is logged at info, and from the `#{@conflicts_warn_after}`th in a row
  at warning, so contention stays visible without ever stalling the table.

  At `#{@stuck_after}` consecutive failures the log escalates to an error,
  the way the sealer's does at its stuck threshold: the difference between
  "retrying" and "stalled" is what an operator needs, not a stop.
  `retry_at` is monotonic time, so a clock step cannot shorten or extend a
  cooldown. The cooldowns live in the compactor's state: a restart forgets
  them, and the first failure after it starts the count again.
  """
  @spec adjusted_cooldowns(
          %{Catalog.table_ref() => cooldown()},
          [Catalog.table_ref()],
          [term()],
          Runtime.t(),
          %{Catalog.table_ref() => map()},
          integer()
        ) :: %{Catalog.table_ref() => cooldown()}
  def adjusted_cooldowns(cooldowns, swept, outcomes, runtime, row_caps, now_ms \\ now_ms()) do
    conflicted =
      for {:failed, %{table: table_ref, reason: :commit_conflict}} <- outcomes,
          into: MapSet.new(),
          do: table_ref

    failed =
      for {:failed, %{table: table_ref} = failure} <- outcomes,
          failure_backs_off?(failure, runtime, row_caps),
          into: MapSet.new(),
          do: table_ref

    Enum.reduce(swept, cooldowns, fn table_ref, acc ->
      cond do
        MapSet.member?(conflicted, table_ref) -> conflict_wait(acc, table_ref, runtime, now_ms)
        MapSet.member?(failed, table_ref) -> back_off(acc, table_ref, runtime, now_ms)
        true -> Map.delete(acc, table_ref)
      end
    end)
  end

  defp conflict_wait(cooldowns, table_ref, runtime, now_ms) do
    entry = Map.get(cooldowns, table_ref, %{consecutive: 0})
    conflicts = Map.get(entry, :conflicts, 0) + 1
    wait = runtime.compact_interval_ms

    :telemetry.execute(
      [:smolquery, :compact, :conflict],
      %{conflicts: conflicts, wait_ms: wait},
      %{table_ref: table_ref}
    )

    log_conflict(table_ref, conflicts, wait)

    Map.put(cooldowns, table_ref, %{
      consecutive: entry.consecutive,
      conflicts: conflicts,
      retry_at: now_ms + wait
    })
  end

  defp log_conflict(table_ref, conflicts, wait) when conflicts >= @conflicts_warn_after do
    Logger.warning(
      "compaction of #{inspect(table_ref)} lost its catalog commit to a concurrent write " <>
        "#{conflicts} sweeps in a row; retrying in #{wait} ms (T-595)"
    )
  end

  defp log_conflict(table_ref, conflicts, wait) do
    Logger.info(
      "compaction of #{inspect(table_ref)} lost its catalog commit to a concurrent write " <>
        "(#{conflicts} in a row); retrying in #{wait} ms"
    )
  end

  defp back_off(cooldowns, table_ref, runtime, now_ms) do
    consecutive =
      case cooldowns do
        %{^table_ref => %{consecutive: consecutive}} -> consecutive + 1
        _first -> 1
      end

    wait =
      Smolquery.Backoff.exponential(
        consecutive,
        runtime.compact_backoff_base_ms,
        runtime.compact_backoff_max_ms
      )

    :telemetry.execute(
      [:smolquery, :compact, :backoff],
      %{consecutive: consecutive, wait_ms: wait},
      %{table_ref: table_ref}
    )

    log_backoff(table_ref, consecutive, wait)

    Map.put(cooldowns, table_ref, %{consecutive: consecutive, retry_at: now_ms + wait})
  end

  defp log_backoff(_table_ref, _consecutive, 0), do: :ok

  defp log_backoff(table_ref, consecutive, wait) when consecutive >= @stuck_after do
    Logger.error(
      "compaction of #{inspect(table_ref)} has failed #{consecutive} times in a row; " <>
        "next attempt in #{wait} ms — compaction on this table is stalled (T-458)"
    )
  end

  defp log_backoff(table_ref, consecutive, wait) do
    Logger.warning(
      "compaction of #{inspect(table_ref)} backs off #{wait} ms " <>
        "(#{consecutive} consecutive failure(s))"
    )
  end

  defp cooling_down?(cooldowns, table_ref) do
    case cooldowns do
      %{^table_ref => %{retry_at: retry_at}} -> now_ms() < retry_at
      _not_cooling -> false
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  @row_cap_floor 65_536
  @relax_patience_start 2
  @relax_patience_max 64

  @doc """
  The per-table row caps after a sweep — the workload-adaptive half of the
  row bound (T-262).

  No static bytes-per-row constant predicts a workload's pin rate: wide,
  repetitive text fields pin kilobytes per row while compressing well enough
  to pass every static cap, and narrow rows pin almost nothing. So the
  runtime's cap is only a start, and the caps here answer the workload
  instead of predicting it. A merge that OOMs halves the
  table's cap, never below #{@row_cap_floor} rows. Only tables that OOMed
  carry an entry, so the map stays empty on healthy deployments. The caps
  live in the compactor's state: a restart forgets them, and the first OOM
  after the restart re-learns them.

  Raising a cap needs evidence, because a raise re-probes the level that
  OOMed and a failed probe burns a multi-minute merge. A sweep counts toward
  the raise when the table's group compacts at more than half its cap — a
  two-file success proves nothing about a cap-sized group — or when the cap
  itself makes every plan skip, since a cap wedged at the floor produces no
  successes to learn from. The cap doubles after `patience` such sweeps, and
  sheds the override when it reaches the runtime's. Each OOM doubles the
  table's patience, up to #{@relax_patience_max} sweeps, so a table sitting
  on its true limit probes it rarely instead of every other sweep.

  The log lines classify each OOM, because a probe finding its limit is the
  design working while an OOM under a tightened cap is not (T-283). A raise
  marks the entry as a probe; the next outcome resolves it. An OOM at a
  probed cap logs at info as expected. A table's first OOM logs at info as
  calibration. An OOM at a cap that was not probing — a level that
  previously held — logs at warning, because it means memory pressure
  changed, not that the compactor is learning.

  Cap state is per node and in-memory. Under bucket-sharded ownership
  (T-269) each node calibrates a table independently, so a fleet of N pays
  up to N calibration OOMs per table, and a restart or a bucket moving to a
  fresh node re-enters calibration — a regression arriving at that moment
  logs at info, not warning. The calibration line says so, and the warning
  fires on the recurrence.
  """
  @spec adjusted_row_caps(
          %{Catalog.table_ref() => map()},
          [term()],
          pos_integer()
        ) :: %{Catalog.table_ref() => map()}
  def adjusted_row_caps(row_caps, outcomes, resolved) do
    Enum.reduce(outcomes, row_caps, fn
      {_outcome, %{level: :span}}, caps ->
        caps

      {:ok, %{table: table, rows: rows}}, caps ->
        relax(caps, table, rows, resolved)

      {:skip, table}, caps ->
        case caps do
          %{^table => entry} -> advance(caps, table, entry, resolved)
          _no_override -> caps
        end

      {:failed, %{table: table, reason: reason} = failure}, caps ->
        if merge_oom?(reason),
          do: tighten(caps, table, resolved, Map.get(failure, :rows)),
          else: caps

      _not_owned, caps ->
        caps
    end)
  end

  defp tighten(caps, table, resolved, rows) do
    {attempted, patience, kind} =
      case caps do
        %{^table => %{cap: cap, patience: patience, probe: probe}} ->
          {min(cap, resolved), min(patience * 2, @relax_patience_max),
           if(probe, do: :probe, else: :regression)}

        _no_override ->
          {resolved, @relax_patience_start, :calibration}
      end

    cap = max(div(attempted, 2), @row_cap_floor)
    log_tighten(kind, table, oom_phrase(rows, attempted), cap)

    Map.put(caps, table, %{cap: cap, streak: 0, patience: patience, probe: false})
  end

  defp oom_phrase(nil, attempted), do: "a merge OOM under the #{attempted}-row cap"

  defp oom_phrase(rows, attempted),
    do: "a merge of #{rows} rows OOMed under the #{attempted}-row cap"

  defp log_tighten(:probe, table, oom, cap) do
    Logger.info(
      "compaction row-cap probe of #{inspect(table)} found its limit: #{oom} " <>
        "(expected while probing); the cap returns to #{cap}"
    )
  end

  defp log_tighten(:calibration, table, oom, cap) do
    Logger.info(
      "compaction row cap of #{inspect(table)} is calibrating: first merge OOM " <>
        "on this node (#{oom}); the cap tightens to #{cap}. A restart or a ring " <>
        "change also lands here, so recurrence at this cap logs as a warning"
    )
  end

  defp log_tighten(:regression, table, oom, cap) do
    Logger.warning(
      "unexpected compaction merge OOM under the already-tightened row cap of " <>
        "#{inspect(table)}: #{oom}; the cap tightens to #{cap} — " <>
        "the compaction engine memory limit may be too small"
    )
  end

  defp relax(caps, table, rows, resolved) do
    case caps do
      %{^table => %{cap: cap} = entry} when rows * 2 >= cap ->
        advance(caps, table, entry, resolved)

      _small_group_or_no_override ->
        caps
    end
  end

  defp advance(caps, table, entry, resolved) do
    cond do
      entry.streak + 1 < entry.patience ->
        Map.put(caps, table, %{entry | streak: entry.streak + 1, probe: false})

      entry.cap * 2 >= resolved ->
        Logger.info(
          "compaction row cap of #{inspect(table)} earned back the resolved " <>
            "#{resolved} rows"
        )

        Map.delete(caps, table)

      true ->
        Logger.info(
          "compaction row cap of #{inspect(table)} probes #{entry.cap * 2} rows; " <>
            "a merge OOM at this level is expected and re-tightens the cap"
        )

        Map.put(caps, table, %{entry | cap: entry.cap * 2, streak: 0, probe: true})
    end
  end

  @doc """
  The per-table byte caps of the span level after a sweep (T-592).

  A span-level group is sized by bytes up to `compact_target_bytes` and by no
  row count, so the lever `adjusted_row_caps/3` gives the hour level does not
  reach it. A span merge that fails for want of memory, spill or time, which
  is an OOM, an engine call exit or a swap that timed out, halves that table's
  span cap, never below `compact_max_bytes`, so the next sweep plans a smaller
  group instead of the same one. Other failures leave the cap alone. Like the
  row caps, these live in the compactor's state: a restart forgets them and
  the table starts again at the target.
  """
  @spec adjusted_span_caps(%{Catalog.table_ref() => pos_integer()}, [term()], Runtime.t()) ::
          %{Catalog.table_ref() => pos_integer()}
  def adjusted_span_caps(span_caps, outcomes, runtime) do
    Enum.reduce(outcomes, span_caps, fn
      {:failed, %{level: :span, table: table, span_cap: cap, reason: reason}}, caps ->
        if span_shrinks?(reason), do: shrink_span(caps, table, cap, runtime), else: caps

      _other, caps ->
        caps
    end)
  end

  defp shrink_span(caps, table, cap, runtime) do
    shrunk = max(div(cap, 2), runtime.compact_max_bytes)

    Logger.warning(
      "compaction span level of #{inspect(table)} failed on a group of up to #{cap} bytes; " <>
        "its groups shrink to #{shrunk} bytes"
    )

    Map.put(caps, table, shrunk)
  end

  defp span_shrinks?(%CallExited{}), do: true
  defp span_shrinks?({:call_exited, %CallExited{}}), do: true
  defp span_shrinks?(reason), do: merge_oom?(reason) or engine_call_exited?(reason)

  defp failure_backs_off?(%{reason: {:inputs_not_live, _paths}}, _runtime, _row_caps), do: false

  defp failure_backs_off?(%{level: :span, span_cap: cap, reason: reason}, runtime, _row_caps) do
    if span_shrinks?(reason),
      do: cap <= runtime.compact_max_bytes,
      else: backs_off?(reason, @span_max_rows)
  end

  defp failure_backs_off?(%{table: table_ref, reason: reason}, runtime, row_caps),
    do: backs_off?(reason, table_capped(runtime, row_caps, table_ref).compact_max_rows)

  # See `adjusted_cooldowns/6`: a failure with a recovery of its own that
  # needs the next sweep is left to it.
  defp backs_off?(reason, cap) do
    cond do
      counts_toward_quarantine?(reason) -> false
      merge_oom?(reason) -> cap <= @row_cap_floor
      true -> true
    end
  end

  defp merge_oom?({:put_failed, _key, reason}), do: merge_oom?(reason)

  defp merge_oom?({:merge_failed, %Adbc.Error{message: message}}),
    do: message =~ "Out of Memory"

  defp merge_oom?(_reason), do: false

  @doc """
  The quarantine state after a sweep — the groups this node has stopped
  planning, because their inputs read as corrupt sweep after sweep (T-310).

  A failure counts toward quarantine only when all three hold: it carries
  the input paths that failed (`plan/2`'s sizing and `swap/4`'s merge both
  attach them); its reason is corruption-shaped — a DuckDB error reading
  the inputs during sizing or merging, never a store put failure, a catalog
  conflict, or an invariant check, which say nothing about the input bytes;
  and it is neither an OOM nor an engine call exit — both already have
  their own recovery (`adjusted_row_caps/3`, `recycle_on_exit/2`) and can
  legitimately repeat while calibrating.

  The streak is keyed by the group's sorted paths, not the table, because
  `plan/2` can regroup a table's candidates differently sweep to sweep as
  segments arrive — and it advances only on the *same* reason: a group
  failing five different ways is an unstable environment, not a corrupt
  segment, so a changed reason restarts the streak. A group reaching
  `threshold` quarantines and its streak entry is dropped.

  Quarantine is this node's in-memory state, nothing more: a restart
  forgets it, and the group re-earns it over `threshold` sweeps. A
  quarantined group stays out of this node's plans while every member is
  still listed in the table's current snapshot; the moment any member
  leaves — the operator drops the corrupt path (`Catalog.drop_segments/3`),
  retention retires it — the group no longer matches and its surviving
  members return to planning (`active_quarantined_paths/2`).
  """
  @spec adjusted_quarantine(
          %{[String.t()] => %{reason: term(), streak: pos_integer()}},
          MapSet.t([String.t()]),
          [term()],
          pos_integer()
        ) :: {%{[String.t()] => %{reason: term(), streak: pos_integer()}}, MapSet.t([String.t()])}
  def adjusted_quarantine(quarantine, quarantined_groups, outcomes, threshold) do
    Enum.reduce(outcomes, {quarantine, quarantined_groups}, fn
      {:failed, %{table: table_ref, reason: reason, paths: [_ | _] = paths}}, acc ->
        if counts_toward_quarantine?(reason) do
          quarantine_step(acc, table_ref, reason, paths, threshold)
        else
          acc
        end

      _other, acc ->
        acc
    end)
  end

  defp counts_toward_quarantine?(reason) do
    corruption_shaped?(reason) and not merge_oom?(reason) and not engine_call_exited?(reason)
  end

  defp corruption_shaped?({:put_failed, _key, reason}), do: corruption_shaped?(reason)

  defp corruption_shaped?({step, %Adbc.Error{}}) when step in [:sizing_failed, :merge_failed],
    do: true

  defp corruption_shaped?(_environmental_or_invariant), do: false

  defp quarantine_step({quarantine, quarantined_groups}, table_ref, reason, paths, threshold) do
    key = Enum.sort(paths)

    streak =
      case quarantine do
        %{^key => %{reason: ^reason, streak: streak}} -> streak + 1
        _new_group_or_changed_reason -> 1
      end

    if streak >= threshold do
      Logger.warning(
        "compaction quarantined #{length(paths)} segment(s) of #{inspect(table_ref)} " <>
          "after #{streak} identical failures: #{inspect(paths)}"
      )

      :telemetry.execute(
        [:smolquery, :compact, :quarantine],
        %{count: length(paths)},
        %{table_ref: table_ref, paths: paths}
      )

      {Map.delete(quarantine, key), MapSet.put(quarantined_groups, key)}
    else
      {Map.put(quarantine, key, %{reason: reason, streak: streak}), quarantined_groups}
    end
  end

  @doc """
  The paths a table's plan must skip, given its current segment listing —
  the read side of `adjusted_quarantine/4`.

  A quarantined group binds only while the listing still holds every one of
  its members. Once any member is gone — dropped by an operator, retired by
  retention — the group's verdict no longer describes what the table holds,
  so its surviving members become plannable again instead of staying
  excluded forever.
  """
  @spec active_quarantined_paths(MapSet.t([String.t()]), [String.t()]) :: MapSet.t(String.t())
  def active_quarantined_paths(quarantined_groups, listed) do
    listed = MapSet.new(listed)

    for group <- quarantined_groups,
        Enum.all?(group, &MapSet.member?(listed, &1)),
        path <- group,
        into: MapSet.new(),
        do: path
  end

  defp compact_table(runtime, state, table_ref) do
    runtime = table_capped(runtime, state.row_caps, table_ref)
    span_cap = Map.get(state.span_caps, table_ref, runtime.compact_target_bytes)
    compact_capped(runtime, state.quarantined_groups, span_cap, table_ref)
  end

  defp table_capped(runtime, row_caps, table_ref) do
    cap =
      case row_caps do
        %{^table_ref => %{cap: cap}} -> min(cap, runtime.compact_max_rows)
        _no_override -> runtime.compact_max_rows
      end

    %{runtime | compact_max_rows: cap}
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

  defp call_exited?({:failed, %{reason: %CallExited{}}}), do: true
  defp call_exited?({:failed, %{reason: {:call_exited, %CallExited{}}}}), do: true
  defp call_exited?(_outcome), do: false

  defp compact_capped(runtime, quarantined_groups, span_cap, table_ref) do
    started_at = System.monotonic_time(:microsecond)

    case exit_safe(:call_exited, fn ->
           compact_listed(runtime, quarantined_groups, span_cap, table_ref, started_at)
         end) do
      {:error, reason} -> failed(runtime, table_ref, reason, started_at)
      outcome -> outcome
    end
  end

  defp compact_listed(runtime, quarantined_groups, span_cap, table_ref, started_at) do
    routing = Routing.resolve(runtime.name)
    planning = %{routing: routing, quarantined_groups: quarantined_groups, files: nil}

    with {:ok, files} <- Catalog.segment_files(runtime.catalog, table_ref, :current),
         {:ok, group} <-
           plan_levels(runtime, %{planning | files: files}, table_ref, span_cap, wall_ms()),
         :ok <- refuse_tombstoned(runtime, table_ref, group) do
      swap(runtime, table_ref, group, started_at)
    else
      :not_owned ->
        :skip

      :skip ->
        {:skip, table_ref}

      {:error, reason} ->
        failed(runtime, table_ref, reason, started_at)

      {:error, reason, failed_paths} ->
        failed(runtime, table_ref, reason, started_at, paths: failed_paths)
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

  defp reject_quarantined(quarantined_groups, owned, listed) do
    active = active_quarantined_paths(quarantined_groups, listed)

    Enum.reject(owned, &MapSet.member?(active, &1))
  end

  defp plan_levels(runtime, planning, table_ref, span_cap, now_ms) do
    case by_level(runtime, Enum.map(planning.files, & &1.path), now_ms) do
      {[], recent} ->
        plan_level(runtime, :hour, planning, table_ref, recent)

      {settled, recent} ->
        span = span_level(runtime, span_cap)

        case plan_level(span, :span, planning, {table_ref, :span}, settled) do
          skipped when skipped in [:not_owned, :skip] ->
            runtime
            |> plan_level(:hour, planning, table_ref, recent)
            |> hour_or_skip(skipped)

          planned ->
            planned
        end
    end
  end

  defp hour_or_skip(:not_owned, :skip), do: :skip
  defp hour_or_skip(hour, _span), do: hour

  defp plan_level(runtime, level, planning, owner, paths) do
    listed = Enum.map(planning.files, & &1.path)

    with [_ | _] = owned <- owned_paths(runtime, planning.routing, owner, paths),
         [_ | _] = plannable <- reject_quarantined(planning.quarantined_groups, owned, listed),
         {:ok, group} <- plan(runtime, level, listed_among(planning.files, plannable)) do
      {:ok, Map.merge(group, %{level: level, span_cap: runtime.compact_max_bytes})}
    else
      [] -> :not_owned
      other -> other
    end
  end

  defp by_level(%Runtime{compact_target_bytes: nil}, paths, _now_ms), do: {[], paths}

  defp by_level(runtime, paths, now_ms) do
    grace = runtime.compact_bucket_ms

    Enum.reduce(paths, {[], []}, fn path, {settled, recent} ->
      case bucket(path, grace) do
        :error -> {settled, recent}
        hour when (hour + 1) * grace + 2 * grace <= now_ms -> {[path | settled], recent}
        hour when (hour + 1) * grace + grace > now_ms -> {settled, [path | recent]}
        _between -> {settled, recent}
      end
    end)
  end

  defp span_level(runtime, span_cap) do
    %{
      runtime
      | compact_below_bytes: div(runtime.compact_target_bytes, 2),
        compact_max_bytes: span_cap,
        compact_max_rows: @span_max_rows,
        compact_bucket_ms: runtime.compact_span_ms
    }
  end

  defp owned_paths(runtime, routing, owner, paths) do
    paths
    |> Enum.sort_by(&Path.basename/1)
    |> Enum.filter(fn path ->
      case bucket(path, runtime.compact_bucket_ms) do
        :error -> false
        bucket -> Routing.own?(routing, owner_key(owner, bucket))
      end
    end)
  end

  defp owner_key({table_ref, :span}, bucket), do: {table_ref, {:span, bucket}}
  defp owner_key(table_ref, bucket), do: {table_ref, bucket}

  defp wall_ms, do: System.os_time(:millisecond)

  defp bucket(path, bucket_ms) do
    case path |> Path.basename(".parquet") |> Id.timestamp() do
      {:ok, timestamp} -> div(timestamp, bucket_ms)
      :error -> :error
    end
  end

  defp listed_among(files, paths) do
    plannable = MapSet.new(paths)

    Enum.filter(files, &MapSet.member?(plannable, &1.path))
  end

  defp plan(runtime, level, files) do
    candidates =
      for %{path: path, bytes: bytes} <- files, bytes < runtime.compact_below_bytes, do: path

    if length(candidates) < runtime.compact_min_inputs do
      :skip
    else
      with {:ok, runtime} <- decoded_capped(runtime, level, candidates) do
        plan_undersized(runtime, level, candidates)
      end
    end
  end

  defp decoded_capped(runtime, :hour, _candidates), do: {:ok, runtime}

  defp decoded_capped(runtime, :span, [sample | _rest]) do
    case row_width(runtime, sample) do
      {:ok, width} ->
        {:ok,
         %{runtime | compact_max_rows: max(div(runtime.compact_span_decoded_bytes, width), 1)}}

      {:error, reason} ->
        {:error, reason, [sample]}
    end
  end

  defp row_width(runtime, path) do
    sql =
      "SELECT CAST(coalesce(avg(strlen(CAST(sampled AS VARCHAR))), 1) AS BIGINT) " <>
        "FROM (SELECT * FROM read_parquet($1) LIMIT #{@width_sample_rows}) AS sampled"

    case Engine.try_query(Runtime.compact_engine(runtime.name), sql, [path]) do
      {:ok, %{rows: [[width]]}} -> {:ok, max(width, 1)}
      {:error, error} -> {:error, {:sizing_failed, error}}
    end
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

  defp plan_undersized(runtime, level, owned) do
    with {:ok, undersized} <- undersized(runtime, owned) do
      undersized
      |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
      |> Enum.chunk_by(fn {path, _bytes, _rows} -> bucket(path, runtime.compact_bucket_ms) end)
      |> carried_group(runtime, level, [])
    end
  end

  defp carried_group([], _runtime, _carry, _carried), do: :skip

  defp carried_group([bucket_entries | rest], runtime, :span, []) do
    case group(runtime, bucket_entries) do
      :skip -> carried_group(rest, runtime, :span, [])
      {:ok, group} -> {:ok, group}
    end
  end

  defp carried_group([bucket_entries | rest], runtime, :hour, carried) do
    candidates = carried ++ bucket_entries

    if length(candidates) < runtime.compact_min_inputs do
      carried_group(rest, runtime, :hour, candidates)
    else
      case group(runtime, candidates) do
        :skip -> carried_group(rest, runtime, :hour, candidates)
        {:ok, group} -> {:ok, group}
      end
    end
  end

  defp undersized(runtime, paths) do
    paths
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while(%{chunks: [], bytes: 0, rows: 0, count: 0}, &size_chunk(runtime, &1, &2))
    |> case do
      {:error, reason, chunk} -> {:error, reason, chunk}
      %{chunks: chunks} -> {:ok, chunks |> Enum.reverse() |> List.flatten()}
    end
  end

  defp size_chunk(runtime, chunk, acc) do
    case sizes_chunk(runtime, chunk) do
      {:ok, sizes} ->
        found =
          Enum.filter(sizes, fn {_path, size, _rows} -> size < runtime.compact_below_bytes end)

        group_filled(
          %{
            chunks: [found | acc.chunks],
            bytes: acc.bytes + Enum.sum_by(found, fn {_path, size, _rows} -> size end),
            rows: acc.rows + Enum.sum_by(found, fn {_path, _size, rows} -> rows end),
            count: acc.count + length(found)
          },
          runtime
        )

      {:error, reason} ->
        {:halt, {:error, reason, attribute_sizing_failure(runtime, reason, chunk)}}
    end
  end

  defp attribute_sizing_failure(_runtime, _reason, [_single] = chunk), do: chunk

  defp attribute_sizing_failure(runtime, reason, chunk) do
    if engine_call_exited?(reason) do
      chunk
    else
      Enum.filter(chunk, &match?({:error, _}, sizes_chunk(runtime, [&1])))
    end
  end

  defp group_filled(%{bytes: bytes, rows: rows, count: count} = acc, runtime)
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

    undersized
    |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
    |> Enum.take(ceiling)
    |> grouped(runtime)
  end

  defp grouped([], _runtime), do: :skip

  defp grouped([_head | rest] = candidates, runtime) do
    {group, total, row_count} =
      Enum.reduce_while(candidates, {[], 0, 0}, fn {path, bytes, rows},
                                                   {group, total, group_rows} ->
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
      grouped(rest, runtime)
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
         {:ok, snapshot} <- swapped(runtime, table_ref, segment, paths) do
      Logger.info(fn ->
        "compacted #{length(paths)} segment(s) of #{inspect(table_ref)} " <>
          "into #{key} at snapshot #{snapshot}"
      end)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: length(paths), duration_us: elapsed_us(started_at)},
        %{result: :ok, table_ref: table_ref}
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
          span_cap: group.span_cap
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

  defp failed(runtime, table_ref, reason, started_at, opts \\ []) do
    Logger.warning("compaction of #{inspect(table_ref)} failed: #{inspect(reason)}")

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 0, duration_us: elapsed_us(started_at)},
      %{result: :error, table_ref: table_ref}
    )

    recycle_on_exit(runtime, reason)

    failure =
      opts
      |> Keyword.take([:rows, :level, :span_cap])
      |> Map.new()
      |> Map.merge(%{table: table_ref, reason: reason, paths: Keyword.get(opts, :paths, [])})

    {:failed, failure}
  end

  @doc """
  Whether a compaction failure carries a `Smolquery.Engine.CallExited` — what
  `failed/4` recycles the compaction engine on. A final `COPY`'s exit arrives
  wrapped by the store as `{:put_failed, key, {:merge_failed, exit}}`; sizing
  and staging exits arrive bare.
  """
  @spec engine_call_exited?(term()) :: boolean()
  def engine_call_exited?({:put_failed, _key, reason}), do: engine_call_exited?(reason)

  def engine_call_exited?({step, %CallExited{}}) when step in [:sizing_failed, :merge_failed],
    do: true

  def engine_call_exited?(_reason), do: false

  defp recycle_on_exit(runtime, reason) do
    if engine_call_exited?(reason) do
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

  defp placeholders(paths, offset \\ 0),
    do: Enum.map_join(1..length(paths), ", ", &"$#{&1 + offset}")
end
