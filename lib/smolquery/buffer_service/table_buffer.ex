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
  store, its entry is fsynced into the manifest, and the runtime's
  `Smolquery.BufferService.Replicator` reports the commit durable to its own
  standard — one replication round per flush, amortized exactly the way the
  fsync is. Nothing here can reply early, because the reply is not in the code
  path that accepts the batch.

  A flush that fails answers every waiting caller with the error and discards the
  rows. That is the honest outcome: those rows were never acked, so the client is
  entitled to retry, and keeping them would write them twice. A segment that made
  it to the store before the manifest append failed is deleted on the spot, and
  recovery would have deleted it anyway. A commit the replicator refused is
  compensated the same way — entry dropped, bytes deleted, batch ids forgotten —
  so a retry commits fresh instead of deduping into an ack the replicator never
  granted. A follower that durably applied the copy before the refusal may hold
  an orphan until the compensating drop reaches it (T-104's reaper family).

  ## Flushing is pipelined

  This process only accumulates; a flush freezes the accumulator and hands it
  to a linked `Smolquery.BufferService.TableBuffer.Committer`, which owns the
  encode, the manifest log, and the replication round, and answers the
  waiting callers itself. Accumulation continues while a commit is in
  flight, so a table's throughput is no longer one inline encode per cycle —
  `bench/results/otel_logs.md` is the benchmark that said otherwise (one
  table at 39k rows/s under a node that does 57k). Every flush trigger —
  the interval, the size thresholds, a schema change, a drain — hands off
  immediately, so under load the committer runs a FIFO of
  `flush_max_bytes`-sized commits back to back while the accumulator keeps
  filling. The admission bounds measure the accumulator, exactly as they
  did when flushing was inline; what used to wait uncounted in this
  process's mailbox now waits in the committer's queue, the same rows one
  hop later.

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
  it had acked and deletes any that were never acknowledged; the committer then
  opens the table's manifest log and holds it for its lifetime — reopening the
  file around every append costs more than the append itself. The committer's
  registered name is the exclusion that makes this safe across restarts: a new
  buffer waits for its predecessor's committer to release the log before
  recovering. A graceful shutdown flushes the accumulator and syncs the
  committer first, so a rolling restart loses nothing. A `:kill` loses only
  rows that had not yet been acked — which no caller was told about; the
  committer drops any batches still queued behind the crash rather than
  committing them to callers who already saw it fail.

  A crash never re-runs the commit from `terminate/2`. The pre-crash state may
  already be half committed — the log record fsynced, the ETS insert not yet done
  — and committing it again would write the batch twice with no client retry
  involved. Only a shutdown-class exit flushes the tail.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.BufferService.TableBuffer.Committer
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [
    :runtime,
    :table_ref,
    :prefix,
    :committer,
    :schema,
    :timer,
    :signaled_at,
    :load,
    chunks: [],
    pending: [],
    batch_ids: [],
    row_count: 0,
    byte_size: 0,
    in_flight: 0,
    in_flight_ids: MapSet.new()
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

  Mid-drain (Milestone 8 L4), a non-duplicate batch is refused with
  `{:error, :draining}` here as well as at the endpoint. The endpoint's gate
  runs in the caller's process, so a write that passed it just before the
  drain flag went up could otherwise accumulate, commit, and *ack* after the
  drain's point-in-time force-seal — rows durably acked on a node the
  operator was just told is safe to decommission. This process is the
  serialization point, so the check here is the one with a happens-before:
  the flag is set before the drain's force-seal call is enqueued, and every
  write processed after that call sees it.

  The ownership fence is re-checked here for the same reason (T-92,
  `Smolquery.BufferService.RingEpoch`): a write can sit in this mailbox for
  up to the write timeout, long past the lease the endpoint's check ran
  under, and this is the last gate before rows enter the accumulator.
  """
  @spec write(GenServer.server(), Schema.t(), [Writer.row()], timeout(), String.t() | nil) ::
          {:ok, ack()} | {:duplicate, ack()} | {:error, term()}
  def write(buffer, %Schema{} = schema, rows, timeout, batch_id \\ nil) do
    GenServer.call(buffer, {:write, schema, rows, batch_id, :erlang.external_size(rows)}, timeout)
  end

  @doc """
  Records an entry another node's group commit shipped here (T-96).

  The follower half of `Smolquery.BufferService.Replicator.SegmentShipping`:
  store the bytes (`nil` when the store is shared and there is nothing to
  ship), then append the owner's entry verbatim. Runs in the table's own
  buffer process so this node keeps exactly one writer per manifest log,
  replica traffic included.
  """
  @spec accept_replica(GenServer.server(), Entry.t(), binary() | nil, timeout()) ::
          :ok | {:error, term()}
  def accept_replica(buffer, %Entry{} = entry, bytes, timeout) do
    GenServer.call(buffer, {:accept_replica, entry, bytes}, timeout)
  end

  @doc """
  Applies a claim, retire, or drop the table's owner replicated here (T-96).
  """
  @spec apply_replica_mutation(GenServer.server(), :claim | :retire | :drop, map(), timeout()) ::
          :ok | {:error, term()}
  def apply_replica_mutation(buffer, op, args, timeout) do
    GenServer.call(buffer, {:apply_replica_mutation, op, args}, timeout)
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
    load = publish_load(runtime, table_ref)

    with {:ok, prefix} <- Store.prefix(table_ref),
         {:ok, committer} <- start_committer(runtime, table_ref, prefix, load),
         {:ok, _report} <- recover(runtime, table_ref),
         :ok <- Committer.open(committer) do
      state = %__MODULE__{
        runtime: runtime,
        table_ref: table_ref,
        prefix: prefix,
        committer: committer,
        load: load
      }

      {:ok, schedule_maintenance(state)}
    end
  end

  defp start_committer(runtime, table_ref, prefix, load) do
    opts = [
      name: Runtime.committer_via(runtime, table_ref),
      runtime: runtime,
      table_ref: table_ref,
      prefix: prefix,
      load: load
    ]

    case Committer.start_link(opts) do
      {:ok, committer} -> {:ok, committer}
      {:error, {:already_started, previous}} -> await_release(previous, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp await_release(previous, opts) do
    reference = Process.monitor(previous)

    receive do
      {:DOWN, ^reference, :process, _pid, _reason} ->
        case Committer.start_link(opts) do
          {:ok, committer} -> {:ok, committer}
          {:error, {:already_started, next}} -> await_release(next, opts)
          {:error, reason} -> {:error, reason}
        end
    after
      15_000 ->
        Process.demonitor(reference, [:flush])

        {:error, :committer_held}
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
  def handle_call({:write, _schema, [], _batch_id, _bytes}, _from, state),
    do: {:reply, {:error, :no_rows}, state}

  def handle_call({:write, schema, rows, batch_id, bytes}, from, state) do
    case committed_ack(state, batch_id) do
      {:ok, ack} ->
        {:reply, {:duplicate, ack}, state}

      :error ->
        case write_gate(state) do
          :ok -> write_or_join(state, schema, rows, batch_id, bytes, from)
          {:error, _reason} = refusal -> {:reply, refusal, state}
        end
    end
  end

  def handle_call(:flush, _from, %__MODULE__{chunks: []} = state) do
    state = await_in_flight(state)

    {:reply, :ok, run_maintenance(state)}
  end

  def handle_call(:flush, from, state) do
    state =
      %{state | pending: [{from, :flush} | state.pending]}
      |> handoff()

    {:noreply, run_maintenance(state)}
  end

  def handle_call({:retire, ids, snapshot}, _from, state) do
    result =
      Committer.with_log(state.committer, fn log ->
        with :ok <- append_replicas(state, :retire, %{ids: ids, snapshot: snapshot}) do
          HotManifest.retire(state.runtime.manifest, state.table_ref, ids, snapshot, log)
        end
      end)

    case result do
      :ok -> {:reply, :ok, run_maintenance(state)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:accept_replica, entry, bytes}, _from, state) do
    result =
      Committer.with_log(state.committer, fn log ->
        with :ok <- put_replica_bytes(state, entry, bytes),
             {:ok, _entry} <-
               HotManifest.put_entry(state.runtime.manifest, state.table_ref, entry, log) do
          :ok
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:apply_replica_mutation, op, args}, _from, state) do
    {:reply, apply_replica_mutation(state, op, args), state}
  end

  def handle_call(:maintain, _from, state), do: {:reply, :ok, run_maintenance(state)}

  def handle_call(:force_seal, _from, state) do
    state = state |> handoff() |> await_in_flight()

    {:reply, :ok, state |> reap() |> force_signal()}
  end

  @impl GenServer
  def handle_info({:flush, tag}, %__MODULE__{timer: {_timer, tag}} = state) do
    {:noreply, %{state | timer: nil} |> handoff() |> run_maintenance()}
  end

  def handle_info({:flush, _stale}, state), do: {:noreply, state}

  def handle_info({:commit_done, batch_ids}, state) do
    {:noreply, state |> settle(batch_ids) |> run_maintenance()}
  end

  def handle_info(:maintain, state) do
    {:noreply, state |> run_maintenance() |> schedule_maintenance()}
  end

  def handle_info({:EXIT, committer, reason}, %__MODULE__{committer: committer} = state) do
    {:stop, reason, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(:normal, state), do: shutdown_commit(state)
  def terminate(:shutdown, state), do: shutdown_commit(state)
  def terminate({:shutdown, _reason}, state), do: shutdown_commit(state)
  def terminate(_crash, state), do: state

  defp shutdown_commit(state) do
    state = state |> handoff() |> await_in_flight()

    GenServer.stop(state.committer, :normal, 5_000)

    state
  catch
    :exit, _reason -> state
  end

  defp settle(state, batch_ids) do
    %{
      state
      | in_flight: state.in_flight - 1,
        in_flight_ids: Enum.reduce(batch_ids, state.in_flight_ids, &MapSet.delete(&2, &1))
    }
  end

  defp await_in_flight(%__MODULE__{in_flight: 0} = state), do: state

  defp await_in_flight(state) do
    :ok = Committer.sync(state.committer)

    drain_commit_done(state)
  end

  defp drain_commit_done(%__MODULE__{in_flight: 0} = state), do: state

  defp drain_commit_done(state) do
    receive do
      {:commit_done, batch_ids} -> drain_commit_done(settle(state, batch_ids))
    after
      0 -> state
    end
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
    result =
      Committer.with_log(state.committer, fn log ->
        with :ok <- append_replicas_as_owner(state, :drop, %{ids: ids}) do
          HotManifest.drop(state.runtime.manifest, state.table_ref, ids, log)
        end
      end)

    case result do
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
    if RingEpoch.owner?(state.runtime.name, state.table_ref) do
      case HotManifest.live_claim(state.runtime.manifest, state.table_ref) do
        {:ok, claim} -> resignal(state, claim)
        :error -> claim_when_sealable(state)
      end
    else
      state
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
    if RingEpoch.owner?(state.runtime.name, state.table_ref) do
      case HotManifest.live_claim(state.runtime.manifest, state.table_ref) do
        {:ok, claim} -> signal(state, claim)
        :error -> force_claim(state)
      end
    else
      state
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

    result =
      with {:ok, keys} <- sealed_keys(state, ids) do
        Committer.with_log(state.committer, &claim_with_log(state, ids, keys, &1))
      end

    case result do
      {:ok, claim} ->
        signal(state, claim)

      {:error, reason} ->
        Logger.warning("claiming #{inspect(state.table_ref)} failed: #{inspect(reason)}")

        state
    end
  end

  defp claim_with_log(state, ids, keys, log) do
    with :ok <- append_replicas(state, :claim, %{ids: ids, keys: keys}) do
      HotManifest.claim(state.runtime.manifest, state.table_ref, ids, keys, log)
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

  defp write_gate(state) do
    if Drain.draining?(state.runtime.name) do
      {:error, :draining}
    else
      RingEpoch.check_write(state.runtime.name, state.table_ref)
    end
  end

  defp append_replicas(state, op, args) do
    Replicator.append(state.runtime.replicator, %{
      name: state.runtime.name,
      table_ref: state.table_ref,
      op: op,
      args: args
    })
  end

  defp append_replicas_as_owner(state, op, args) do
    if RingEpoch.owner?(state.runtime.name, state.table_ref) do
      append_replicas(state, op, args)
    else
      :ok
    end
  end

  defp put_replica_bytes(_state, _entry, nil), do: :ok

  defp put_replica_bytes(state, entry, bytes) do
    with {:ok, key} <- Store.key(state.prefix, entry.id),
         {:ok, _put} <-
           Store.put(state.runtime.store, key, fn path -> File.write(path, bytes) end) do
      :ok
    end
  end

  defp apply_replica_mutation(state, :claim, %{ids: ids, keys: keys}) do
    result =
      Committer.with_log(state.committer, fn log ->
        HotManifest.claim(state.runtime.manifest, state.table_ref, ids, keys, log)
      end)

    case result do
      {:ok, _claim} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_replica_mutation(state, :retire, %{ids: ids, snapshot: snapshot}) do
    Committer.with_log(state.committer, fn log ->
      HotManifest.retire(state.runtime.manifest, state.table_ref, ids, snapshot, log)
    end)
  end

  defp apply_replica_mutation(state, :drop, %{ids: ids}) do
    Committer.with_log(state.committer, fn log ->
      HotManifest.drop(state.runtime.manifest, state.table_ref, ids, log)
    end)
  end

  defp write_or_join(state, schema, rows, batch_id, bytes, from) do
    cond do
      not is_nil(batch_id) and batch_id in state.batch_ids ->
        {:noreply, %{state | pending: [{from, :duplicate} | state.pending]}}

      not is_nil(batch_id) and MapSet.member?(state.in_flight_ids, batch_id) ->
        settle_in_flight_duplicate(state, schema, rows, batch_id, bytes, from)

      true ->
        write_new(state, schema, rows, batch_id, bytes, from)
    end
  end

  defp settle_in_flight_duplicate(state, schema, rows, batch_id, bytes, from) do
    state = await_in_flight(state)

    case committed_ack(state, batch_id) do
      {:ok, ack} -> {:reply, {:duplicate, ack}, state}
      :error -> write_new(state, schema, rows, batch_id, bytes, from)
    end
  end

  defp write_new(state, schema, rows, batch_id, bytes, from) do
    count = length(rows)
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
    |> handoff_when_full()
  end

  defp flush_on_schema_change(%__MODULE__{chunks: []} = state, _schema), do: state
  defp flush_on_schema_change(%__MODULE__{schema: schema} = state, schema), do: state
  defp flush_on_schema_change(state, _schema), do: handoff(state)

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

  defp handoff_when_full(state) do
    if state.row_count >= state.runtime.flush_max_rows or
         state.byte_size >= state.runtime.flush_max_bytes do
      handoff(state)
    else
      state
    end
  end

  defp handoff(%__MODULE__{chunks: []} = state), do: state

  defp handoff(state) do
    batch_ids = Enum.reverse(state.batch_ids)

    Committer.commit(state.committer, %{
      schema: state.schema,
      rows: state.chunks |> Enum.reverse() |> Enum.concat(),
      pending: Enum.reverse(state.pending),
      batch_ids: batch_ids,
      row_count: state.row_count,
      byte_size: state.byte_size
    })

    %{
      state
      | chunks: [],
        pending: [],
        batch_ids: [],
        row_count: 0,
        byte_size: 0,
        timer: cancel(state.timer),
        in_flight: state.in_flight + 1,
        in_flight_ids: Enum.into(batch_ids, state.in_flight_ids)
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
