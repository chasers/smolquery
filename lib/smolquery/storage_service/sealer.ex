defmodule Smolquery.StorageService.Sealer do
  @moduledoc """
  Answers seal signals: one seal in flight per table, a bounded pool per node.

  A signal names a table and the micro-segments its owner wants sealed. This
  process decides whether that signal becomes work right now, and nothing more —
  the handoff itself runs in a task, so a slow merge never blocks the next
  signal's arrival.

  ## Dropping a signal is always safe

  Two rules discard signals: a table already being sealed coalesces (its running
  attempt covers the same claim), and a signal arriving at `max_concurrent_seals`
  is shed. Both are safe because signalling is level-triggered —
  `Smolquery.BufferService.SealConsumer` re-signals every `seal_retry_ms` until
  the claim is retired, so a dropped signal costs a retry interval and nothing
  else. That is the whole reason this process needs no queue: the buffer holds the
  only durable record of what wants sealing, and it repeats itself.

  ## Attempts fail rather than block

  A seal attempt runs under a `Task.Supervisor` and is monitored, never linked, so
  a merge that crashes takes down neither this process nor the table it was
  sealing. The table simply becomes eligible again, and the next re-signal retries
  it — with the same claim, so the retry produces the same sealed segment rather
  than a second one.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime, attempts: %{}]

  @doc """
  Starts the sealer for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.sealer(runtime.name))
  end

  @doc """
  Hands a seal signal to the sealer, without waiting for the seal.

  A cast rather than a call, because the caller is the owning `TableBuffer` and
  its write path must not wait on storage. Returns `:ok` whether the signal
  becomes work or is dropped; see the coalescing rules above.
  """
  @spec seal_ready(atom(), Store.table_ref(), SealConsumer.claim()) :: :ok
  def seal_ready(name, table_ref, claim) do
    GenServer.cast(Runtime.sealer(name), {:seal_ready, table_ref, claim})
  end

  @doc """
  The tables this node is sealing right now.

  For tests and operators asking what the pool is busy with.
  """
  @spec sealing(atom()) :: [Store.table_ref()]
  def sealing(name), do: GenServer.call(Runtime.sealer(name), :sealing)

  @impl GenServer
  def init(%Runtime{} = runtime), do: {:ok, %__MODULE__{runtime: runtime}}

  @impl GenServer
  def handle_cast({:seal_ready, table_ref, claim}, state) do
    cond do
      sealing?(state, table_ref) -> {:noreply, state}
      at_capacity?(state) -> {:noreply, shed(state, table_ref)}
      true -> {:noreply, start_attempt(state, table_ref, claim)}
    end
  end

  @impl GenServer
  def handle_call(:sealing, _from, state), do: {:reply, Map.values(state.attempts), state}

  @impl GenServer
  def handle_info({ref, _result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    {:noreply, finish(state, ref)}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.attempts, ref) do
      {:ok, table_ref} ->
        Logger.warning("seal of #{inspect(table_ref)} crashed: #{inspect(reason)}")

        {:noreply, finish(state, ref)}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

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
