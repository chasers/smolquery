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
  stopping is not. Every failed attempt at or past that threshold also emits
  `[:smolquery, :seal, :stuck]`, which `Smolquery.Telemetry` counts as
  `smolquery_seal_stuck_attempts_total` — a stuck claim should page on a
  nonzero rate, not wait to be found in the logs (T-293).

  The count also paces the retries (T-299). Attempts run one merge-engine
  connection per slot — `start_attempt` pins each to a free slot, so a slow
  table's statements never serialize another table's (see
  `Smolquery.StorageService.Supervisor`). What a doomed claim still burns
  every re-signal is an attempt slot and the engine's shared memory — on
  the eu-central-1 sandbox that stalled sealing cluster-wide. After a
  failure the table cools down for `backoff_ms/2` — `seal_backoff_base_ms`
  doubling per consecutive failure up to `seal_backoff_max_ms` — and
  signals inside the cooldown are shed the same way signals at the
  concurrency bound are: level-triggered re-signalling makes the shed a
  delay, never a lost seal. A success clears the cooldown with the count.
  A cooling table holds no slot, so a table that can seal takes it instead.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime, attempts: %{}, failures: %{}, retry_at: %{}]

  @stuck_after 5
  @backoff_doubling_cap 30

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
      sealing?(state, table_ref, claim) -> {:noreply, state}
      cooling_down?(state, table_ref) -> {:noreply, shed_cooling(state, table_ref)}
      at_capacity?(state) -> {:noreply, shed(state, table_ref)}
      true -> {:noreply, start_attempt(state, table_ref, claim)}
    end
  end

  @impl GenServer
  def handle_call(:sealing, _from, state),
    do: {:reply, state.attempts |> Map.values() |> Enum.map(& &1.table_ref), state}

  @impl GenServer
  def handle_call(:failures, _from, state), do: {:reply, state.failures, state}

  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.fetch(state.attempts, ref) do
      {:ok, attempt} ->
        {:noreply, state |> record(attempt, result) |> finish(ref)}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.attempts, ref) do
      {:ok, attempt} ->
        record_attempt(:crashed, attempt)

        {:noreply,
         state |> failed(attempt.table_ref, "crashed: #{inspect(reason)}") |> finish(ref)}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  defp record(state, attempt, {:error, reason}) do
    record_attempt(:error, attempt)

    failed(state, attempt.table_ref, "failed: #{inspect(reason)}")
  end

  defp record(state, attempt, _result) do
    record_attempt(:ok, attempt)

    %{
      state
      | failures: Map.delete(state.failures, attempt.table_ref),
        retry_at: Map.delete(state.retry_at, attempt.table_ref)
    }
  end

  defp record_attempt(result, attempt) do
    :telemetry.execute(
      [:smolquery, :seal, :attempt],
      %{
        duration_us: System.monotonic_time(:microsecond) - attempt.started_at,
        segments: attempt.segments
      },
      %{result: result, table_ref: attempt.table_ref}
    )
  end

  defp failed(state, table_ref, description) do
    consecutive = Map.get(state.failures, table_ref, 0) + 1

    if consecutive >= @stuck_after do
      :telemetry.execute(
        [:smolquery, :seal, :stuck],
        %{consecutive: consecutive},
        %{table_ref: table_ref}
      )
    end

    log_failure(table_ref, description, consecutive)

    retry_at = now_ms() + backoff_ms(consecutive, state.runtime)

    %{
      state
      | failures: Map.put(state.failures, table_ref, consecutive),
        retry_at: Map.put(state.retry_at, table_ref, retry_at)
    }
  end

  @doc false
  @spec backoff_ms(pos_integer(), Runtime.t()) :: non_neg_integer()
  def backoff_ms(consecutive, runtime) do
    doublings = min(consecutive - 1, @backoff_doubling_cap)

    min(runtime.seal_backoff_base_ms * Integer.pow(2, doublings), runtime.seal_backoff_max_ms)
  end

  defp cooling_down?(state, table_ref) do
    case Map.fetch(state.retry_at, table_ref) do
      {:ok, retry_at} -> now_ms() < retry_at
      :error -> false
    end
  end

  defp shed_cooling(state, table_ref) do
    Logger.debug(fn ->
      consecutive = Map.get(state.failures, table_ref, 0)

      "seal of #{inspect(table_ref)} shed: cooling down after " <>
        "#{consecutive} consecutive failures (T-299)"
    end)

    state
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

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

  # Coalesces per claim, not per table (T-339): a buffer running
  # `max_live_claims` above one signals several distinct claims for one ref,
  # and each deserves its own slot. A re-signal of a claim already being
  # attempted still coalesces, which is what makes level-triggered signalling
  # free — the claim's keys are its identity, frozen with its input set.
  defp sealing?(state, table_ref, claim) do
    Enum.any?(state.attempts, fn {_ref, attempt} ->
      attempt.table_ref == table_ref and attempt.keys == claim[:keys]
    end)
  end

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
    slot = free_slot(state)
    attempt_runtime = %{runtime | merge_engine: {Runtime.engine(runtime.name), slot}}

    task =
      Task.Supervisor.async_nolink(Runtime.seals(runtime.name), fn ->
        Handoff.seal(runtime.handoff, attempt_runtime, table_ref, claim)
      end)

    attempt = %{
      table_ref: table_ref,
      keys: claim[:keys],
      segments: claim_segments(claim),
      started_at: System.monotonic_time(:microsecond),
      slot: slot
    }

    %{state | attempts: Map.put(state.attempts, task.ref, attempt)}
  end

  defp free_slot(state) do
    used = state.attempts |> Map.values() |> MapSet.new(& &1.slot)

    Enum.find(1..state.runtime.max_concurrent_seals, &(not MapSet.member?(used, &1)))
  end

  defp claim_segments(%{ids: ids}) when is_list(ids), do: length(ids)

  defp claim_segments(_claim), do: 0

  defp finish(state, ref), do: %{state | attempts: Map.delete(state.attempts, ref)}
end
