defmodule Smolquery.StorageService.Scheduler.Backoff do
  @moduledoc """
  How long a table whose compaction failed is left out of the sweep (T-458),
  and why a catalog commit conflict is not such a failure (T-595).

  "The sweep is the retry" is the right shape for a crash, and the wrong
  one for a merge that fails the same way every time. A group that OOMs
  under a row cap already at its floor, or whose put keeps failing, re-ran
  every `compact_interval_ms` for as long as it failed, and each attempt
  was minutes of merge that held a catalog connection: on the sandbox the
  seals queued behind it timed out, so a compacting node could not seal and
  the buffer backlog it was meant to drain grew instead (T-458). Now a
  failed table waits `compact_backoff_base_ms`, doubling per consecutive
  failure up to `compact_backoff_max_ms`, before the sweep looks at it
  again, and a success clears the wait.
  `Smolquery.StorageService.Scheduler.Quarantine` is the stop for a
  *corrupt* input; this is the pace for everything else.
  """

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Caps
  alias Smolquery.StorageService.Scheduler.Failure
  alias Smolquery.StorageService.Scheduler.Planner

  @stuck_after 5
  @conflicts_warn_after 3

  @typedoc """
  A table left out of the sweep until `retry_at`, after `consecutive`
  failures and, when it last lost a commit, `conflicts` of those in a row.
  """
  @type cooldown :: %{
          required(:consecutive) => non_neg_integer(),
          required(:retry_at) => integer(),
          optional(:conflicts) => pos_integer()
        }

  @doc """
  The per-table cooldowns after a sweep — a failing table's compaction backs
  off instead of re-running every sweep (T-458).

  The sweep is the scheduler's retry, and a table whose merge fails the same
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
  still above the floor, which `Smolquery.StorageService.Scheduler.Caps.adjusted_row_caps/3` halves the cap on and
  must see again to calibrate (T-262), and a corruption-shaped failure,
  which `Smolquery.StorageService.Scheduler.Quarantine.adjusted_quarantine/4` counts toward the quarantine that stops it
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
  cooldown. The cooldowns live in the scheduler's state: a restart forgets
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

  @doc "Whether `table_ref` is still waiting out a cooldown."
  @spec cooling_down?(%{Catalog.table_ref() => cooldown()}, Catalog.table_ref()) :: boolean()
  def cooling_down?(cooldowns, table_ref) do
    case cooldowns do
      %{^table_ref => %{retry_at: retry_at}} -> now_ms() < retry_at
      _not_cooling -> false
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp failure_backs_off?(%{reason: {:inputs_not_live, _paths}}, _runtime, _row_caps), do: false

  defp failure_backs_off?(%{level: :span, span_cap: cap, reason: reason}, runtime, _row_caps) do
    if Failure.span_shrinks?(reason),
      do: cap <= runtime.compact_max_bytes,
      else: backs_off?(reason, Planner.span_max_rows())
  end

  defp failure_backs_off?(%{table: table_ref, reason: reason}, runtime, row_caps),
    do: backs_off?(reason, Caps.table_capped(runtime, row_caps, table_ref).compact_max_rows)

  # See `adjusted_cooldowns/6`: a failure with a recovery of its own that
  # needs the next sweep is left to it.
  defp backs_off?(reason, cap) do
    cond do
      Failure.counts_toward_quarantine?(reason) -> false
      Failure.merge_oom?(reason) -> cap <= Caps.row_cap_floor()
      true -> true
    end
  end
end
