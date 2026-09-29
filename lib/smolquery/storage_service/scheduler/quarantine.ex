defmodule Smolquery.StorageService.Scheduler.Quarantine do
  @moduledoc """
  The groups a node stops planning because their inputs read as corrupt
  sweep after sweep (T-310): the stop for a corrupt input, where
  `Smolquery.StorageService.Scheduler.Backoff` is the pace for everything
  else.
  """

  require Logger

  alias Smolquery.StorageService.Scheduler.Failure

  @threshold 5

  @doc "Identical failures in a row before a group is quarantined."
  @spec threshold() :: pos_integer()
  def threshold, do: @threshold

  @doc """
  The quarantine state after a sweep — the groups this node has stopped
  planning, because their inputs read as corrupt sweep after sweep (T-310).

  A failure counts toward quarantine only when all three hold: it carries
  the input paths that failed (the planner's sizing and the job's merge both
  attach them); its reason is corruption-shaped — a DuckDB error reading
  the inputs during sizing or merging, never a store put failure, a catalog
  conflict, or an invariant check, which say nothing about the input bytes;
  and it is neither an OOM nor an engine call exit — both already have
  their own recovery (`Smolquery.StorageService.Scheduler.Caps.adjusted_row_caps/3`,
  the engine recycle in `Smolquery.StorageService.Scheduler.Job`) and can
  legitimately repeat while calibrating.

  The streak is keyed by the group's sorted paths, not the table, because
  the planner can regroup a table's candidates differently sweep to sweep as
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
        if Failure.counts_toward_quarantine?(reason) do
          quarantine_step(acc, table_ref, reason, paths, threshold)
        else
          acc
        end

      _other, acc ->
        acc
    end)
  end

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
end
