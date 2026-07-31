defmodule Smolquery.BufferService.TableBuffer do
  @moduledoc """
  One table's group commit — where an insert becomes durable.

  This process is the serialization point for a table's writes, and that is a
  feature rather than a bottleneck: group commit needs a single accumulator, and
  the buffer service's ownership ring exists to give each table exactly one.

  ## The ack is withheld until the rows are durable

  `write/4` is a `GenServer.call` whose reply is deliberately *not* sent from
  `handle_call`. Callers accumulate in a pending list while their rows sit in
  memory, and every one of them is answered together, after the segment is in the
  store and its entry is fsynced into the manifest. Nothing here can reply early,
  because the reply is not in the code path that accepts the batch.

  A flush that fails answers every waiting caller with the error and discards the
  rows. That is the honest outcome: those rows were never acked, so the client is
  entitled to retry, and keeping them would write them twice. A segment that made
  it to the store before the manifest append failed is deleted on the spot, and
  recovery would have deleted it anyway.

  ## Flushing is inline, until a benchmark says otherwise

  The encode happens in this process, so a table's throughput is capped at one
  Polars encode per cycle. The alternative — swap the accumulator and encode in a
  `Task` — is two state machines instead of one, and worth it only if the ceiling
  turns out to matter. `bench/buffer.exs` is what decides that; until then this is
  one clear state machine.

  ## Backpressure is immediate

  A batch that would push the accumulator past `max_buffered_rows` or
  `max_buffered_bytes` is refused with `{:error, :buffer_full}` rather than
  queued, for the ingest edge to turn into a 429. The byte bound is measured on
  the in-memory term, not the encoded Parquet, because what it protects is this
  node's heap.

  ## Schema changes flush rather than fail

  A batch whose schema differs from the one accumulating forces a flush and starts
  a fresh accumulator. One segment always has one schema, and additive evolution
  therefore works at the file level for free — `read_parquet(union_by_name = true)`
  handles the read side.

  ## Recovery, and what a crash costs

  On start the buffer recovers its table's manifest, which re-adopts the segments
  it had acked and deletes any that were never acknowledged. A graceful shutdown
  flushes the accumulator first, so a rolling restart loses nothing. A `:kill`
  loses only rows that had not yet been acked — which no caller was told about.

  A crash never re-runs the commit from `terminate/2`. The pre-crash state may
  already be half committed — the log record fsynced, the ETS insert not yet done
  — and committing it again would write the batch twice with no client retry
  involved. Only a shutdown-class exit flushes the tail.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [
    :runtime,
    :table_ref,
    :prefix,
    :schema,
    :timer,
    :signaled_at,
    chunks: [],
    pending: [],
    row_count: 0,
    byte_size: 0
  ]

  @type ack :: %{segment_id: String.t(), row_count: non_neg_integer()}

  @doc """
  A child spec identified by the table the buffer owns.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :table_ref)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient
    }
  end

  @doc """
  Starts the buffer owning `:table_ref`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    runtime = Keyword.fetch!(opts, :runtime)
    table_ref = Keyword.fetch!(opts, :table_ref)

    GenServer.start_link(__MODULE__, {runtime, table_ref}, name: Runtime.via(runtime, table_ref))
  end

  @doc """
  Accumulates `rows` and returns once they are durable.

  The reply arrives after the group commit this batch lands in, so its latency is
  the remaining flush interval plus the encode — not the cost of these rows alone.
  """
  @spec write(GenServer.server(), Schema.t(), [Writer.row()], timeout()) ::
          {:ok, ack()} | {:error, term()}
  def write(buffer, %Schema{} = schema, rows, timeout) do
    GenServer.call(buffer, {:write, schema, rows}, timeout)
  end

  @doc """
  Flushes the accumulator now, without waiting for the interval.

  Returns what the commit returned: a caller draining before shutdown must know
  whether the tail actually became durable.
  """
  @spec flush(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def flush(buffer, timeout \\ 5_000), do: GenServer.call(buffer, :flush, timeout)

  @doc """
  Stamps `ids` as sealed at a catalog snapshot.
  """
  @spec retire(GenServer.server(), [String.t()], non_neg_integer(), timeout()) ::
          :ok | {:error, term()}
  def retire(buffer, ids, snapshot, timeout \\ 5_000),
    do: GenServer.call(buffer, {:retire, ids, snapshot}, timeout)

  @doc """
  Runs the seal check and the grace-period sweep now, without waiting for the tick.
  """
  @spec maintain(GenServer.server(), timeout()) :: :ok
  def maintain(buffer, timeout \\ 5_000), do: GenServer.call(buffer, :maintain, timeout)

  @impl GenServer
  def init({runtime, table_ref}) do
    Process.flag(:trap_exit, true)

    with {:ok, prefix} <- Store.prefix(table_ref),
         {:ok, _report} <- recover(runtime, table_ref) do
      state = %__MODULE__{runtime: runtime, table_ref: table_ref, prefix: prefix}

      {:ok, schedule_maintenance(state)}
    end
  end

  defp recover(runtime, table_ref) do
    case HotManifest.recover(runtime.manifest, table_ref) do
      {:ok, report} -> {:ok, report}
      {:error, reason} -> {:error, {:recovery_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call({:write, _schema, []}, _from, state), do: {:reply, {:error, :no_rows}, state}

  def handle_call({:write, schema, rows}, from, state) do
    count = length(rows)
    bytes = :erlang.external_size(rows)
    state = flush_on_schema_change(state, schema)

    if full?(state, count, bytes) do
      {:reply, {:error, :buffer_full}, state}
    else
      {:noreply, accept(state, schema, rows, count, bytes, from)}
    end
  end

  def handle_call(:flush, _from, state) do
    {result, state} = commit_and_report(state)

    {:reply, result, run_maintenance(state)}
  end

  def handle_call({:retire, ids, snapshot}, _from, state) do
    case HotManifest.retire(state.runtime.manifest, state.table_ref, ids, snapshot) do
      :ok -> {:reply, :ok, run_maintenance(state)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:maintain, _from, state), do: {:reply, :ok, run_maintenance(state)}

  @impl GenServer
  def handle_info({:flush, tag}, %__MODULE__{timer: {_timer, tag}} = state) do
    {:noreply, %{state | timer: nil} |> commit() |> run_maintenance()}
  end

  def handle_info({:flush, _stale}, state), do: {:noreply, state}

  def handle_info(:maintain, state) do
    {:noreply, state |> run_maintenance() |> schedule_maintenance()}
  end

  @impl GenServer
  def terminate(:normal, state), do: commit(state)
  def terminate(:shutdown, state), do: commit(state)
  def terminate({:shutdown, _reason}, state), do: commit(state)
  def terminate(_crash, state), do: state

  defp run_maintenance(state) do
    state
    |> reap()
    |> signal_when_ready()
  end

  defp reap(state) do
    cutoff = now() - state.runtime.retire_grace_ms

    case HotManifest.retired_before(state.runtime.manifest, state.table_ref, cutoff) do
      [] -> state
      expired -> drop(state, Enum.map(expired, & &1.id))
    end
  end

  defp drop(state, ids) do
    case HotManifest.drop(state.runtime.manifest, state.table_ref, ids) do
      :ok ->
        state

      {:error, reason} ->
        Logger.warning(
          "hot tier sweep for #{inspect(state.table_ref)} failed: #{inspect(reason)}"
        )

        state
    end
  end

  defp signal_when_ready(state) do
    unsealed =
      state.runtime.manifest
      |> HotManifest.entries(state.table_ref)
      |> Enum.reject(&Entry.sealed?/1)

    cond do
      unsealed == [] -> %{state | signaled_at: nil}
      not sealable?(state, unsealed) -> state
      due?(state) -> signal(state, unsealed)
      true -> state
    end
  end

  defp sealable?(state, unsealed) do
    bytes = Enum.sum_by(unsealed, & &1.byte_size)
    oldest = unsealed |> Enum.min_by(& &1.added_at) |> Map.fetch!(:added_at)

    bytes >= state.runtime.seal_max_bytes or
      length(unsealed) >= state.runtime.seal_max_files or
      now() - oldest >= state.runtime.seal_max_age_ms
  end

  defp due?(%__MODULE__{signaled_at: nil}), do: true

  defp due?(state), do: now() - state.signaled_at >= state.runtime.seal_retry_ms

  defp signal(state, unsealed) do
    SealConsumer.seal_ready(
      state.runtime.seal_consumer,
      state.table_ref,
      Enum.map(unsealed, & &1.id)
    )

    %{state | signaled_at: now()}
  end

  defp schedule_maintenance(state) do
    Process.send_after(self(), :maintain, state.runtime.maintenance_interval_ms)

    state
  end

  defp now, do: System.os_time(:millisecond)

  defp accept(state, schema, rows, count, bytes, from) do
    state
    |> accumulate(schema, rows, count, bytes, from)
    |> commit_when_full()
  end

  defp flush_on_schema_change(%__MODULE__{chunks: []} = state, _schema), do: state
  defp flush_on_schema_change(%__MODULE__{schema: schema} = state, schema), do: state
  defp flush_on_schema_change(state, _schema), do: commit(state)

  defp accumulate(state, schema, rows, count, bytes, from) do
    %{
      state
      | schema: schema,
        chunks: [rows | state.chunks],
        pending: [from | state.pending],
        row_count: state.row_count + count,
        byte_size: state.byte_size + bytes
    }
    |> schedule()
  end

  defp commit_when_full(state) do
    if state.row_count >= state.runtime.flush_max_rows or
         state.byte_size >= state.runtime.flush_max_bytes do
      commit(state)
    else
      state
    end
  end

  defp commit(state), do: state |> commit_and_report() |> elem(1)

  defp commit_and_report(%__MODULE__{chunks: []} = state), do: {:ok, state}

  defp commit_and_report(state) do
    rows = state.chunks |> Enum.reverse() |> Enum.concat()
    result = persist(state, rows)

    state.pending
    |> Enum.reverse()
    |> reply_all(result)

    {flush_result(result), reset(state)}
  end

  defp flush_result({:ok, _ack}), do: :ok
  defp flush_result({:error, reason}), do: {:error, reason}

  defp persist(state, rows) do
    with {:ok, segment} <-
           Writer.write(rows, state.schema, store: state.runtime.store, prefix: state.prefix),
         {:ok, entry} <- add(state, segment) do
      {:ok, %{segment_id: entry.id, row_count: entry.row_count}}
    end
  end

  defp add(state, segment) do
    case HotManifest.add(state.runtime.manifest, state.table_ref, segment) do
      {:ok, entry} ->
        {:ok, entry}

      {:error, reason} ->
        Store.delete(state.runtime.store, segment.key)

        {:error, reason}
    end
  end

  defp reply_all(pending, reply), do: Enum.each(pending, &GenServer.reply(&1, reply))

  defp reset(state) do
    %{
      state
      | chunks: [],
        pending: [],
        row_count: 0,
        byte_size: 0,
        timer: cancel(state.timer)
    }
  end

  defp full?(state, count, bytes) do
    state.row_count + count > state.runtime.max_buffered_rows or
      state.byte_size + bytes > state.runtime.max_buffered_bytes
  end

  defp schedule(%__MODULE__{timer: nil} = state) do
    tag = make_ref()
    timer = Process.send_after(self(), {:flush, tag}, state.runtime.flush_interval_ms)

    %{state | timer: {timer, tag}}
  end

  defp schedule(state), do: state

  defp cancel(nil), do: nil

  defp cancel({timer, _tag}) do
    Process.cancel_timer(timer)

    nil
  end
end
