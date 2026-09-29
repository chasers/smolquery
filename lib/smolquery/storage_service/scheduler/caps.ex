defmodule Smolquery.StorageService.Scheduler.Caps do
  @moduledoc """
  The per-table limits a node learns from its own failures: the hour level's
  row cap (T-262) and the span level's byte cap (T-592). Both start at the
  runtime's value, tighten on a merge that ran out of memory or time, and
  live in the scheduler's state.
  """

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Failure

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
  live in the scheduler's state: a restart forgets them, and the first OOM
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
        if Failure.merge_oom?(reason),
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
  row caps, these live in the scheduler's state: a restart forgets them and
  the table starts again at the target.
  """
  @spec adjusted_span_caps(%{Catalog.table_ref() => pos_integer()}, [term()], Runtime.t()) ::
          %{Catalog.table_ref() => pos_integer()}
  def adjusted_span_caps(span_caps, outcomes, runtime) do
    Enum.reduce(outcomes, span_caps, fn
      {:failed, %{level: :span, table: table, span_cap: cap, reason: reason}}, caps ->
        if Failure.span_shrinks?(reason), do: shrink_span(caps, table, cap, runtime), else: caps

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

  @doc """
  The per-table bytes a row costs in a span merge, learned from the span
  merges that failed (T-603).

  The planner caps a span group at `compact_span_decoded_bytes` over an
  estimated row width, sampled as text. On the sandbox that sample was low
  by at least 20x on the `bench.otel_logs_v*` tables: groups it held to
  1 GiB each spilled past a 22 GiB temp limit and failed after three and a
  half minutes. A sample of the file cannot see what the merge does with
  it, so the merge's own failure is the measure:

    * a merge that ran out of temp space names the limit it hit
      (`Smolquery.StorageService.Scheduler.Failure.temp_cap_bytes/1`), and
      its group's rows needed more than that, so a row costs at least the
      limit over the rows; the table learns twice that, so the next group
      lands well under the limit instead of just under it;
    * a merge that ran out of memory doubles the width the group was planned
      at. Only these two: a swap that timed out, a store put whose HTTP pool
      died, or an engine call exit says nothing about a row's size, and a
      width only grows, so learning from them would shrink a table's groups
      for good on a few transient faults.

  A learned width only grows, and the planner uses the larger of it and the
  sample. Like the caps, it lives in the scheduler's state: a restart forgets
  it and the table learns it again from one failure.
  """
  @spec adjusted_span_widths(%{Catalog.table_ref() => pos_integer()}, [term()]) ::
          %{Catalog.table_ref() => pos_integer()}
  def adjusted_span_widths(widths, outcomes) do
    Enum.reduce(outcomes, widths, fn
      {:failed, %{level: :span, table: table, rows: rows, reason: reason} = failure}, acc
      when is_integer(rows) and rows > 0 ->
        case learned_width(reason, rows, Map.get(failure, :width)) do
          nil -> acc
          width -> learn_width(acc, table, width, rows)
        end

      _other, acc ->
        acc
    end)
  end

  defp learned_width(reason, rows, planned) do
    case Failure.temp_cap_bytes(reason) do
      {:ok, cap} ->
        div(2 * cap, rows) + 1

      :error ->
        if Failure.merge_oom?(reason) and is_integer(planned), do: planned * 2
    end
  end

  defp learn_width(widths, table, width, rows) do
    case widths do
      %{^table => known} when known >= width ->
        widths

      _lower_or_new ->
        Logger.warning(
          "compaction span level of #{inspect(table)} learned #{width} bytes a row from a " <>
            "failed merge of #{rows} rows; its groups are sized by it from now on (T-603)"
        )

        Map.put(widths, table, width)
    end
  end

  @doc "The floor no row cap tightens below."
  @spec row_cap_floor() :: pos_integer()
  def row_cap_floor, do: @row_cap_floor

  @doc """
  `runtime` with `compact_max_rows` at the table's learned row cap, when it
  has one below the runtime's.
  """
  @spec table_capped(Runtime.t(), %{Catalog.table_ref() => map()}, Catalog.table_ref()) ::
          Runtime.t()
  def table_capped(runtime, row_caps, table_ref) do
    cap =
      case row_caps do
        %{^table_ref => %{cap: cap}} -> min(cap, runtime.compact_max_rows)
        _no_override -> runtime.compact_max_rows
      end

    %{runtime | compact_max_rows: cap}
  end
end
