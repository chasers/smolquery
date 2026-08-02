defmodule Smolquery.StorageService.Sealer do
  @moduledoc """
  Answers seal signals: one seal in flight per table, a bounded pool per node.

  A signal names a table and the micro-segments its owner wants sealed. This
  process decides whether that signal becomes work right now, and nothing more —
  the handoff itself runs in a task, so a slow merge never blocks the next
  signal's arrival.

  ## Dropping a signal is always safe

  Three rules discard signals: a signal for a table this node does not own per
  `Smolquery.StorageService.Routing.own?/2` is ignored outright (Milestone 8 L6,
  PL-11 D6 — the seal-work-distribution gate: `Smolquery.StorageService.Client`
  routes a signal to the storage ring's current owner, but a ring change between
  that decision and this cast arriving can make the receiving node a stale owner;
  the gate narrows the two-owner overlap a ring change opens, and the catalog's
  re-diffed commit retries are what keep the remainder from double-registering —
  see `Smolquery.StorageService.Routing`), a table already being sealed coalesces
  (its running attempt covers the same claim), and a signal arriving at
  `max_concurrent_seals` is shed. All three are safe because signalling is
  level-triggered — `Smolquery.BufferService.SealConsumer` re-signals every
  `seal_retry_ms` until the claim is retired, so a dropped signal costs a retry
  interval and nothing else (the retry re-resolves the owner too, so a table
  that moved during a ring change gets a fresh, correctly-routed signal next
  time). That is the whole reason this process needs no queue: the buffer holds
  the only durable record of what wants sealing, and it repeats itself.

  ## Attempts fail rather than block

  A seal attempt runs under a `Task.Supervisor` and is monitored, never linked, so
  a merge that crashes takes down neither this process nor the table it was
  sealing. The table simply becomes eligible again, and the next re-signal retries
  it — with the same claim, so the retry produces the same sealed segment rather
  than a second one.

  ## A claim that can never seal retries forever, and is counted while it does

  The retry loop has no bound, and giving it one would be worse than leaving it:
  the buffer holds the only durable record of what wants sealing, so a sealer that
  gave up would strand a table's tail in the hot tier with nothing left to notice.
  What it does instead is count. `failures/1` reports consecutive failed attempts
  per table, cleared the moment one succeeds, and the log escalates from a warning
  to an error once a table has failed enough times in a row to stop being plausibly
  transient. Distinguishing "retrying" from "stuck" is the part that was missing;
  stopping is not.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime, attempts: %{}, failures: %{}]

  @stuck_after 5

  @doc """
  Starts the sealer for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.sealer(runtime.name))
  end

  @doc """
  Hands a seal signal to the sealer running on `node`, without waiting for the
  seal.

  A cast rather than a call, because the caller is the owning `TableBuffer` and
  its write path must not wait on storage. Returns `:ok` whether the signal
  becomes work or is dropped; see the coalescing rules above.

  `node` defaults to this node — `Smolquery.StorageService.Client` passes the
  storage ring's current owner explicitly (Milestone 8 L6), so the signal
  reaches the sealer that will actually accept it rather than only ever the
  one running alongside the caller.

  The send itself must not touch distribution setup: `GenServer.cast/2` to a
  `{name, node}` tuple blocks the caller for the connection attempt when no
  connection to `node` exists (up to `net_setuptime`), and the caller here is
  the owning `TableBuffer`'s write path. `:noconnect`/`:nosuspend` sends the
  message only over an established connection and hands the connect attempt to
  a throwaway process otherwise — the same shape as Erlang's own
  `gen_server:cast/2`.
  """
  @spec seal_ready(atom(), Store.table_ref(), SealConsumer.claim(), node()) :: :ok
  def seal_ready(name, table_ref, claim, node \\ node()) do
    destination = {Runtime.sealer(name), node}
    message = {:"$gen_cast", {:seal_ready, table_ref, claim}}

    case :erlang.send(destination, message, [:noconnect, :nosuspend]) do
      :ok ->
        :ok

      _unconnected ->
        spawn(fn -> send(destination, message) end)

        :ok
    end
  catch
    _kind, _reason -> :ok
  end

  @doc """
  The tables this node is sealing right now.

  For tests and operators asking what the pool is busy with.
  """
  @spec sealing(atom()) :: [Store.table_ref()]
  def sealing(name), do: GenServer.call(Runtime.sealer(name), :sealing)

  @doc """
  Consecutive failed seal attempts, per table.

  A table appears here once an attempt has failed and drops out the moment one
  succeeds, so a table sealing cleanly is absent rather than zero. This is what an
  operator reads to tell a claim that is merely retrying from one that can never
  seal; see the moduledoc.
  """
  @spec failures(atom()) :: %{Store.table_ref() => pos_integer()}
  def failures(name), do: GenServer.call(Runtime.sealer(name), :failures)

  @impl GenServer
  def init(%Runtime{} = runtime), do: {:ok, %__MODULE__{runtime: runtime}}

  @impl GenServer
  def handle_cast({:seal_ready, table_ref, claim}, state) do
    cond do
      not owner?(state, table_ref) -> {:noreply, ignore_foreign(state, table_ref)}
      sealing?(state, table_ref) -> {:noreply, state}
      at_capacity?(state) -> {:noreply, shed(state, table_ref)}
      true -> {:noreply, start_attempt(state, table_ref, claim)}
    end
  end

  @impl GenServer
  def handle_call(:sealing, _from, state), do: {:reply, Map.values(state.attempts), state}

  @impl GenServer
  def handle_call(:failures, _from, state), do: {:reply, state.failures, state}

  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.fetch(state.attempts, ref) do
      {:ok, table_ref} ->
        {:noreply, state |> record(table_ref, result) |> finish(ref)}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.attempts, ref) do
      {:ok, table_ref} ->
        attempt(:crashed)

        {:noreply, state |> failed(table_ref, "crashed: #{inspect(reason)}") |> finish(ref)}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  defp record(state, table_ref, {:error, reason}) do
    attempt(:error)

    failed(state, table_ref, "failed: #{inspect(reason)}")
  end

  defp record(state, table_ref, _result) do
    attempt(:ok)

    %{state | failures: Map.delete(state.failures, table_ref)}
  end

  defp attempt(result),
    do: :telemetry.execute([:smolquery, :seal, :attempt], %{}, %{result: result})

  defp failed(state, table_ref, description) do
    consecutive = Map.get(state.failures, table_ref, 0) + 1

    log_failure(table_ref, description, consecutive)

    %{state | failures: Map.put(state.failures, table_ref, consecutive)}
  end

  defp log_failure(table_ref, description, consecutive) when consecutive >= @stuck_after do
    Logger.error(
      "seal of #{inspect(table_ref)} #{description} — #{consecutive} consecutive failures, " <>
        "this claim may never seal"
    )
  end

  defp log_failure(table_ref, description, consecutive),
    do:
      Logger.warning("seal of #{inspect(table_ref)} #{description} (#{consecutive} consecutive)")

  defp owner?(state, table_ref),
    do: state.runtime.name |> Routing.resolve() |> Routing.own?(table_ref)

  defp ignore_foreign(state, table_ref) do
    Logger.debug(fn ->
      "seal of #{inspect(table_ref)} ignored: not this node's storage-ring owner"
    end)

    state
  end

  defp sealing?(state, table_ref), do: table_ref in Map.values(state.attempts)

  defp at_capacity?(state),
    do: map_size(state.attempts) >= state.runtime.max_concurrent_seals

  defp shed(state, table_ref) do
    Logger.debug(fn ->
      "seal of #{inspect(table_ref)} shed: #{state.runtime.max_concurrent_seals} already in flight"
    end)

    state
  end

  defp start_attempt(state, table_ref, claim) do
    runtime = state.runtime

    task =
      Task.Supervisor.async_nolink(Runtime.seals(runtime.name), fn ->
        Handoff.seal(runtime.handoff, runtime, table_ref, claim)
      end)

    %{state | attempts: Map.put(state.attempts, task.ref, table_ref)}
  end

  defp finish(state, ref), do: %{state | attempts: Map.delete(state.attempts, ref)}
end
