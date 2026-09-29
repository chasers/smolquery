defmodule Smolquery.StorageService.Scheduler.Failure do
  @moduledoc """
  What kind of failure a compaction failure is, read off its reason.

  Every other part of `Smolquery.StorageService.Scheduler` turns on these
  classes: an OOM tightens a cap, an engine call exit recycles the engine,
  a corruption-shaped failure counts toward quarantine, a call exit stops
  the sweep. Keeping the matching in one place keeps the classes the same
  everywhere they are read.
  """

  alias Smolquery.Engine.CallExited

  @doc """
  Whether a merge ran out of memory, bare or as the store wraps a final
  `COPY`'s failure (`{:put_failed, key, {:merge_failed, error}}`).
  """
  @spec merge_oom?(term()) :: boolean()
  def merge_oom?({:put_failed, _key, reason}), do: merge_oom?(reason)

  def merge_oom?({:merge_failed, %Adbc.Error{message: message}}),
    do: message =~ "Out of Memory"

  def merge_oom?(_reason), do: false

  @doc """
  Whether a compaction failure carries a `Smolquery.Engine.CallExited` — what
  `Smolquery.StorageService.Scheduler.Job` recycles the compaction engine on.
  A final `COPY`'s exit arrives wrapped by the store as
  `{:put_failed, key, {:merge_failed, exit}}`; sizing and staging exits
  arrive bare.
  """
  @spec engine_call_exited?(term()) :: boolean()
  def engine_call_exited?({:put_failed, _key, reason}), do: engine_call_exited?(reason)

  def engine_call_exited?({step, %CallExited{}}) when step in [:sizing_failed, :merge_failed],
    do: true

  def engine_call_exited?(_reason), do: false

  @doc """
  Whether a failure is a call that exited outside the compaction engine: a
  catalog call (`%CallExited{}` bare, T-464), or a store put whose HTTP pool
  died (`{:call_exited, _}`). What stops a sweep: the catalog call is still
  running on the compaction connection, and every later table would queue
  behind it. An engine exit does not stop the sweep; the engine is recycled.
  """
  @spec stops_sweep?(term()) :: boolean()
  def stops_sweep?(%CallExited{}), do: true
  def stops_sweep?({:call_exited, %CallExited{}}), do: true
  def stops_sweep?(_reason), do: false

  @doc """
  Whether a failure counts toward quarantine: a DuckDB error reading the
  inputs during sizing or merging, never a store put failure, a catalog
  conflict or an invariant check, and neither an OOM nor an engine call
  exit, which have recoveries of their own and can repeat while those work.
  """
  @spec counts_toward_quarantine?(term()) :: boolean()
  def counts_toward_quarantine?(reason) do
    corruption_shaped?(reason) and not merge_oom?(reason) and not engine_call_exited?(reason)
  end

  defp corruption_shaped?({:put_failed, _key, reason}), do: corruption_shaped?(reason)

  defp corruption_shaped?({step, %Adbc.Error{}}) when step in [:sizing_failed, :merge_failed],
    do: true

  defp corruption_shaped?(_environmental_or_invariant), do: false

  @doc """
  Whether a span-level failure is one a smaller group would avoid: an OOM,
  or a call that exited for want of time.
  """
  @spec span_shrinks?(term()) :: boolean()
  def span_shrinks?(reason),
    do: merge_oom?(reason) or stops_sweep?(reason) or engine_call_exited?(reason)
end
