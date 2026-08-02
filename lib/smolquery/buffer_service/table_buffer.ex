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

  ## Sealing is signalled against a frozen set

  Crossing a seal threshold does not signal the tail as it stands; it first freezes
  that tail into a claim in the manifest log, and signals the claim. Everything
  written afterwards accumulates for the *next* claim, and while a claim is
  outstanding the re-signal repeats it verbatim. A sealer therefore merges the same
  ids no matter how many times it is told, or which side of the handoff crashed —
  see `Smolquery.BufferService.HotManifest` for why that is what makes sealing
  exactly-once.

  The claim also names its output, derived from its inputs, so a table's sealed
  segment has a stable identity before any bytes exist. One key per claim: how
  large a sealed segment gets is `seal_max_bytes`'s business, since it bounds what
  a claim can hold, and splitting a merge across files is the compactor's problem
  rather than the buffer's.

  ## Recovery, and what a crash costs

  On start the buffer recovers its table's manifest, which re-adopts the segments
  it had acked and deletes any that were never acknowledged, then opens the
  table's manifest log and holds it for its lifetime — reopening the file around
  every append costs more than the append itself. A graceful shutdown flushes
  the accumulator first, so a rolling restart loses nothing. A `:kill` loses
  only rows that had not yet been acked — which no caller was told about.

  A crash never re-runs the commit from `terminate/2`. The pre-crash state may
  already be half committed — the log record fsynced, the ETS insert not yet done
  — and committing it again would write the batch twice with no client retry
  involved. Only a shutdown-class exit flushes the tail.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [
    :runtime,
    :table_ref,
    :prefix,
    :log,
    :schema,
    :timer,
    :signaled_at,
    :load,
    chunks: [],
    pending: [],
    batch_ids: [],
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

  `batch_id` is the batch's idempotency key, or `nil` for at-least-once. A
  batch whose id already committed is answered `{:duplicate, ack}` with the
  original commit's ack and writes nothing; one whose id is still sitting in
  the accumulator joins that commit's pending list instead of accumulating
  its rows twice. The `{:duplicate, ack}` shape exists for the endpoint's
  load accounting — rows that were never accepted must not wait to be
  drained — and callers who do not care treat it as `{:ok, ack}`.
  """
  @spec write(GenServer.server(), Schema.t(), [Writer.row()], timeout(), String.t() | nil) ::
          {:ok, ack()} | {:duplicate, ack()} | {:error, term()}
  def write(buffer, %Schema{} = schema, rows, timeout, batch_id \\ nil) do
    GenServer.call(buffer, {:write, schema, rows, batch_id}, timeout)
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

  @doc """
  Flushes, then signals a seal for every unsealed micro-segment this table
  holds — regardless of `seal_max_bytes`/`seal_max_files`/`seal_max_age_ms`
  or how recently a claim last signalled (Milestone 8 L4's drain).

  A no-op, returning `:ok`, when there is nothing unsealed: draining a table
  that already has none is not an error.
  """
  @spec force_seal(GenServer.server(), timeout()) :: :ok
  def force_seal(buffer, timeout \\ 5_000), do: GenServer.call(buffer, :force_seal, timeout)

  @impl GenServer
  def init({runtime, table_ref}) do
    Process.flag(:trap_exit, true)

    with {:ok, prefix} <- Store.prefix(table_ref),
         {:ok, _report} <- recover(runtime, table_ref),
         {:ok, log} <- HotManifest.open_log(runtime.manifest, table_ref) do
      state = %__MODULE__{
        runtime: runtime,
        table_ref: table_ref,
        prefix: prefix,
        log: log,
        load: publish_load(runtime, table_ref)
      }

      {:ok, schedule_maintenance(state)}
    end
  end

  defp publish_load(runtime, table_ref) do
    load = Load.new()
    Registry.update_value(Runtime.registry(runtime.name), table_ref, fn _value -> load end)

    load
  end

  defp recover(runtime, table_ref) do
    case HotManifest.recover(runtime.manifest, table_ref) do
      {:ok, report} -> {:ok, report}
      {:error, reason} -> {:error, {:recovery_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call({:write, _schema, [], _batch_id}, _from, state),
    do: {:reply, {:error, :no_rows}, state}

  def handle_call({:write, schema, rows, batch_id}, from, state) do
    case committed_ack(state, batch_id) do
      {:ok, ack} -> {:reply, {:duplicate, ack}, state}
      :error -> write_or_join(state, schema, rows, batch_id, from)
    end
  end

  def handle_call(:flush, _from, state) do
    {result, state} = commit_and_report(state)

    {:reply, result, run_maintenance(state)}
  end

  def handle_call({:retire, ids, snapshot}, _from, state) do
    case HotManifest.retire(state.runtime.manifest, state.table_ref, ids, snapshot, state.log) do
      :ok -> {:reply, :ok, run_maintenance(state)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:maintain, _from, state), do: {:reply, :ok, run_maintenance(state)}

  def handle_call(:force_seal, _from, state) do
    {_result, state} = commit_and_report(state)

    {:reply, :ok, state |> reap() |> force_signal()}
  end

  @impl GenServer
  def handle_info({:flush, tag}, %__MODULE__{timer: {_timer, tag}} = state) do
    {:noreply, %{state | timer: nil} |> commit() |> run_maintenance()}
  end

  def handle_info({:flush, _stale}, state), do: {:noreply, state}

  def handle_info(:maintain, state) do
    {:noreply, state |> run_maintenance() |> schedule_maintenance()}
  end

  @impl GenServer
  def terminate(:normal, state), do: state |> commit() |> close_log()
  def terminate(:shutdown, state), do: state |> commit() |> close_log()
  def terminate({:shutdown, _reason}, state), do: state |> commit() |> close_log()
  def terminate(_crash, state), do: close_log(state)

  defp close_log(state) do
    HotManifest.close_log(state.log)

    state
  end

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
    case HotManifest.drop(state.runtime.manifest, state.table_ref, ids, state.log) do
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
    case HotManifest.live_claim(state.runtime.manifest, state.table_ref) do
      {:ok, claim} -> resignal(state, claim)
      :error -> claim_when_sealable(state)
    end
  end

  defp resignal(state, claim) do
    if due?(state), do: signal(state, claim), else: state
  end

  defp claim_when_sealable(state) do
    unsealed =
      state.runtime.manifest
      |> HotManifest.entries(state.table_ref)
      |> Enum.reject(&Entry.sealed?/1)

    cond do
      unsealed == [] -> %{state | signaled_at: nil}
      not sealable?(state, unsealed) -> state
      true -> claim_and_signal(state, unsealed)
    end
  end

  defp force_signal(state) do
    case HotManifest.live_claim(state.runtime.manifest, state.table_ref) do
      {:ok, claim} -> signal(state, claim)
      :error -> force_claim(state)
    end
  end

  defp force_claim(state) do
    unsealed =
      state.runtime.manifest
      |> HotManifest.entries(state.table_ref)
      |> Enum.reject(&Entry.sealed?/1)

    if unsealed == [], do: state, else: claim_and_signal(state, unsealed)
  end

  defp claim_and_signal(state, unsealed) do
    ids = Enum.map(unsealed, & &1.id)

    with {:ok, keys} <- sealed_keys(state, ids),
         {:ok, claim} <-
           HotManifest.claim(state.runtime.manifest, state.table_ref, ids, keys, state.log) do
      signal(state, claim)
    else
      {:error, reason} ->
        Logger.warning("claiming #{inspect(state.table_ref)} failed: #{inspect(reason)}")

        state
    end
  end

  defp sealed_keys(state, ids) do
    with {:ok, key} <- Store.key(state.prefix, sealed_id(state.table_ref, ids)) do
      {:ok, [key]}
    end
  end

  defp sealed_id({dataset, table}, ids) do
    sorted = Enum.sort(ids)
    {:ok, timestamp} = sorted |> List.last() |> Id.timestamp()

    Id.derive(timestamp, [dataset, 0, table, 0, Enum.intersperse(sorted, 0)])
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

  defp signal(state, claim) do
    SealConsumer.seal_ready(state.runtime.seal_consumer, state.table_ref, claim)

    %{state | signaled_at: now()}
  end

  defp schedule_maintenance(state) do
    Process.send_after(self(), :maintain, state.runtime.maintenance_interval_ms)

    state
  end

  defp now, do: System.os_time(:millisecond)

  defp committed_ack(_state, nil), do: :error

  defp committed_ack(state, batch_id),
    do: HotManifest.batch_ack(state.runtime.manifest, state.table_ref, batch_id)

  defp write_or_join(state, schema, rows, batch_id, from) do
    if not is_nil(batch_id) and batch_id in state.batch_ids do
      {:noreply, %{state | pending: [{from, :duplicate} | state.pending]}}
    else
      write_new(state, schema, rows, batch_id, from)
    end
  end

  defp write_new(state, schema, rows, batch_id, from) do
    count = length(rows)
    bytes = :erlang.external_size(rows)
    state = flush_on_schema_change(state, schema)

    if full?(state, count, bytes) do
      {:reply, {:error, :buffer_full}, state}
    else
      {:noreply, accept(state, schema, rows, count, bytes, batch_id, from)}
    end
  end

  defp accept(state, schema, rows, count, bytes, batch_id, from) do
    state
    |> accumulate(schema, rows, count, bytes, batch_id, from)
    |> commit_when_full()
  end

  defp flush_on_schema_change(%__MODULE__{chunks: []} = state, _schema), do: state
  defp flush_on_schema_change(%__MODULE__{schema: schema} = state, schema), do: state
  defp flush_on_schema_change(state, _schema), do: commit(state)

  defp accumulate(state, schema, rows, count, bytes, batch_id, from) do
    %{
      state
      | schema: schema,
        chunks: [rows | state.chunks],
        pending: [{from, :new} | state.pending],
        batch_ids: track_batch(state.batch_ids, batch_id),
        row_count: state.row_count + count,
        byte_size: state.byte_size + bytes
    }
    |> schedule()
  end

  defp track_batch(batch_ids, nil), do: batch_ids
  defp track_batch(batch_ids, batch_id), do: [batch_id | batch_ids]

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
    started = System.monotonic_time(:microsecond)
    result = persist(state, rows)
    duration_us = System.monotonic_time(:microsecond) - started

    Load.drained(state.load, state.row_count)

    if match?({:ok, _ack}, result) do
      Load.sample_rate(state.load, state.row_count, duration_us)
    end

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: state.row_count, bytes: state.byte_size, duration_us: duration_us},
      %{result: if(match?({:ok, _ack}, result), do: :ok, else: :error)}
    )

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
    batch_ids = Enum.reverse(state.batch_ids)

    case HotManifest.add(state.runtime.manifest, state.table_ref, segment, state.log, batch_ids) do
      {:ok, entry} ->
        {:ok, entry}

      {:error, reason} ->
        Store.delete(state.runtime.store, segment.key)

        {:error, reason}
    end
  end

  defp reply_all(pending, result) do
    Enum.each(pending, fn {from, kind} -> GenServer.reply(from, reply_for(kind, result)) end)
  end

  defp reply_for(:new, result), do: result
  defp reply_for(:duplicate, {:ok, ack}), do: {:duplicate, ack}
  defp reply_for(:duplicate, error), do: error

  defp reset(state) do
    %{
      state
      | chunks: [],
        pending: [],
        batch_ids: [],
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
