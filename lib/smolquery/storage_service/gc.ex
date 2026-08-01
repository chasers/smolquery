defmodule Smolquery.StorageService.GC do
  @moduledoc """
  Deletes the one kind of garbage sealing can produce: an upload whose commit
  never followed.

  A seal attempt puts a sealed segment before registering it. Die in between and
  the object exists while no snapshot references it — unreachable, and nothing else
  will ever name it, because the next attempt writes the same key and registers
  that. Those are orphans, and they are the *only* orphans: every other unreferenced
  path in the sealed store is either about to be registered or was never ours.
  (Compaction produces the same orphan the same way — its merged segment is put
  before the swap commits, at a key derived the same deterministic way — so the
  same sweep covers it with nothing added.)

  Each sweep also clears the store's staging directory of writes older than the
  grace period (`Smolquery.Segments.Store.sweep_staging/2`): a killed encoder
  leaves its staged bytes behind, and they are not segments, so no manifest or
  catalog will ever account for them. The same grace period applies for the same
  reason — a staged file younger than the longest merge may still be mid-write.

  ## What is deliberately not garbage

  A path dropped from the current snapshot but present in an older one is not
  garbage — it is time travel, and deleting it would silently break a query planned
  at that snapshot. Expiring old snapshots and then reclaiming their files is
  retention's job, not this one. So the test is membership in
  `Smolquery.Catalog.known_segments/1`, which spans every snapshot, rather than in
  `segments/3`, which spans one.

  ## Two sightings, not a timestamp

  An object is deleted only after being seen unreferenced continuously for
  `gc_grace_ms`, tracked across sweeps in this process. Two reasons not to read the
  object's age instead: a sealed segment's key encodes when its *inputs* were
  written, not when the merge ran, so a claim that waited an hour for a sealer
  produces an "old" key the moment it lands; and asking the store for an mtime is a
  filesystem question an object store answers differently.

  The consequence is a coupling worth stating: `gc_grace_ms` must exceed the longest
  merge, the same way `retire_grace_ms` must exceed the longest query. A merge
  slower than the grace period could have its output swept out from under it before
  it commits — after which the next attempt simply merges again, so the cost is
  wasted work rather than lost rows.

  Sightings are clocked with monotonic time: this is elapsed time within one live
  process, and a wall clock stepped by NTP would expire a candidate early or hold
  it forever. And the sweep re-reads `known_segments/1` immediately before it
  deletes, so a commit that lands mid-sweep — a delayed retry of the same
  deterministic key finally succeeding — keeps its file rather than losing it to
  the sweep-start snapshot.

  ## Restarting forgets what it had seen

  Candidates live in memory, so a restart resets every clock and an orphan waits
  another `gc_grace_ms`. That is the right trade: leaking a file for one more grace
  period is cheap, and persisting this state would mean a second durable thing to
  keep honest for no correctness gain.
  """

  use GenServer

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime, candidates: %{}]

  @doc """
  Starts the sweeper for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.gc(runtime.name))
  end

  @doc """
  Sweeps now, without waiting for the interval.

  Reports what it deleted and what it is still watching, which is what makes the
  grace period observable rather than a guess.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 30_000), do: GenServer.call(Runtime.gc(name), :sweep, timeout)

  @impl GenServer
  def init(%Runtime{} = runtime) do
    {:ok, schedule(%__MODULE__{runtime: runtime})}
  end

  @impl GenServer
  def handle_call(:sweep, _from, state) do
    case run(state) do
      {:ok, report, state} -> {:reply, {:ok, report}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    case run(state) do
      {:ok, _report, state} ->
        {:noreply, schedule(state)}

      {:error, reason} ->
        Logger.warning("sealed-tier sweep failed: #{inspect(reason)}")

        {:noreply, schedule(state)}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  defp schedule(state) do
    Process.send_after(self(), :sweep, state.runtime.gc_interval_ms)

    state
  end

  defp run(state) do
    with {:ok, known} <- Catalog.known_segments(state.runtime.catalog),
         {:ok, keys} <- Store.list(state.runtime.store, "") do
      {expired, watching} = partition(state, unreferenced(state.runtime, keys, known))

      with {:ok, swept} <- sweep_expired(state.runtime, expired),
           {:ok, staged} <-
             Store.sweep_staging(state.runtime.store, state.runtime.gc_grace_ms) do
        :telemetry.execute(
          [:smolquery, :gc, :sweep],
          %{swept: length(swept), staged: length(staged)},
          %{}
        )

        {:ok, %{swept: swept, staged: staged, watching: map_size(watching)},
         %{state | candidates: watching}}
      end
    end
  end

  defp sweep_expired(_runtime, []), do: {:ok, []}

  defp sweep_expired(runtime, keys) do
    with {:ok, known} <- Catalog.known_segments(runtime.catalog) do
      expired = unreferenced(runtime, keys, known)

      with :ok <- delete_all(runtime, expired), do: {:ok, expired}
    end
  end

  defp unreferenced(runtime, keys, known) do
    referenced = MapSet.new(known)

    Enum.reject(keys, &MapSet.member?(referenced, Store.location(runtime.store, &1)))
  end

  defp partition(state, keys) do
    now = System.monotonic_time(:millisecond)
    cutoff = now - state.runtime.gc_grace_ms

    seen = Map.new(keys, &{&1, Map.get(state.candidates, &1, now)})
    {expired, watching} = Enum.split_with(seen, fn {_key, first_seen} -> first_seen <= cutoff end)

    {Enum.map(expired, &elem(&1, 0)), Map.new(watching)}
  end

  defp delete_all(_runtime, []), do: :ok

  defp delete_all(runtime, keys) do
    Logger.info(fn ->
      "sealed-tier sweep deleting #{length(keys)} uncommitted segment(s)"
    end)

    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Store.delete(runtime.store, key) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
