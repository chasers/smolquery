defmodule Smolquery.Burst.Boots do
  @moduledoc """
  The clock for a runner boot, between the two FLAME pool hooks that bracket
  it (PL-77).

  `FLAME.Pool` calls `on_grow_start` inside the task that boots the runner
  and `on_grow_end` inside the pool's own process once that task has
  reported, so nothing one hook leaves in its process is there for the
  other. This table is the hand-off: one row per boot in flight, keyed by
  the boot task's pid, written by the start hook and taken by the end hook.
  It is bounded by the pool's `max`, since a boot past it never starts, and
  every row is taken when its boot ends, whichever way.

  Started by `Smolquery.Burst.children/0` beside the pool. Without it the
  hooks still run and report a duration of zero, which is what a unit test
  of the hooks alone sees.
  """

  use GenServer

  @table __MODULE__

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Records that the boot run by `pid` started now."
  @spec started(pid()) :: :ok
  def started(pid) do
    if alive?(), do: :ets.insert(@table, {pid, System.monotonic_time(:millisecond)})

    :ok
  end

  @doc """
  How long the boot run by `pid` took, in milliseconds, and forgets it;
  `nil` when no start was recorded.
  """
  @spec finished(pid()) :: non_neg_integer() | nil
  def finished(pid) do
    with true <- alive?(), [{^pid, started}] <- :ets.take(@table, pid) do
      System.monotonic_time(:millisecond) - started
    else
      _absent -> nil
    end
  end

  defp alive?, do: :ets.whereis(@table) != :undefined

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])

    {:ok, nil}
  end
end
