defmodule Smolquery.Heap do
  @moduledoc """
  Garbage collection policy for the processes that hold large short-lived terms.

  The BEAM's generational collector is tuned for the common case: most terms
  die young, so a minor collection copies the survivors to an old heap and only
  a *major* collection — a full sweep — ever reclaims what has been promoted.
  The write path is the uncommon case. A batch of decoded JSON rows is
  megabytes that live exactly as long as one group commit and are then garbage
  in their entirety, so promotion is pure loss: the rows survive one minor
  collection, move to the old heap, and wait there for a major collection to
  notice they are dead. `bench/profile.exs` measured what that costs — dirty
  CPU schedulers spending several times longer in `gc` than in `emulator`,
  which is major collections of those heaps rather than the Polars encode they
  are supposed to be running.

  `:fullsweep_after` is the flag that exists for this. It caps how many
  generational collections a process may run before the next one is a full
  sweep, and `0` makes every collection a full sweep. It is the right trade
  exactly when a process's *live* set is small and its garbage is large — a
  full sweep costs the live set, not the garbage — and the wrong one when the
  live set is itself large, because then every collection copies it again.

  `:min_heap_size` is the floor a process's heap is grown to. It buys away the
  repeated doubling a process pays on its way up to a size it reaches every
  cycle anyway, and it costs that many words per process for as long as the
  process lives. That is a memory decision rather than a CPU one, which is why
  nothing here sets it by default: the callers that use this module run one
  process per table, so a floor multiplies by the table count.

  Both are `nil`-tolerant. A `nil` leaves the flag alone rather than setting a
  substitute value, so "not configured" and "configured to the emulator's
  default" stay distinguishable, and turning a knob off restores exactly the
  behaviour that preceded it.
  """

  @flags [:fullsweep_after, :min_heap_size]

  @type option ::
          {:fullsweep_after, non_neg_integer() | nil}
          | {:min_heap_size, non_neg_integer() | nil}

  @doc """
  Applies `opts` to the calling process, skipping any whose value is `nil`.

  Both flags take effect from the calling process's next garbage collection;
  neither forces one. Call this from a process's own `init/1` rather than on
  someone else's pid — a flag set here outlives the call that set it, for as
  long as the process runs.

  ## Options

    * `:fullsweep_after` — generational collections allowed between full
      sweeps. `0` makes every collection a full sweep.
    * `:min_heap_size` — heap floor in words.

  """
  @spec tune([option()]) :: :ok
  def tune(opts) do
    opts
    |> Keyword.take(@flags)
    |> Enum.each(fn
      {_flag, nil} -> :ok
      {flag, value} -> :erlang.process_flag(flag, value)
    end)
  end
end
