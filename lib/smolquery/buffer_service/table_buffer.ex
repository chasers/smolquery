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

  ## The wait is adaptive

  An accumulation window that opens with fewer than `commit_siblings` inserts
  already in flight closes after `flush_idle_interval_ms` rather than
  `flush_interval_ms` — Postgres's `commit_delay`/`commit_siblings`, at the
  file level (T-202). The interval only buys batching when other writers are
  active: at one writer the wait is pure ack latency, and it was most of the
  benchmark's low-VU ack. In flight means handed off and not yet settled —
  callers whose rows are committing now, the writers who come back the moment
  they are acked. Callers are counted rather than commits, because one queued
  commit can hold a hundred of them.

  The idle window is a few milliseconds rather than zero so a burst's
  simultaneous first inserts still coalesce into one commit. The choice is
  made once, when the window's first chunk arms the timer: a window armed
  long just before load vanishes waits the full interval once, and the next
  window adapts. Real concurrency keeps today's behavior — the window opens
  above the threshold and the interval and byte caps rule unchanged.

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
  handles the read side. A payload-kind change forces the same flush: an NDJSON
  passthrough body and a rows/frame batch cannot share a commit, because the
  committer encodes a commit one way or the other.

  ## Sealing is signalled against a frozen set

  Crossing a seal threshold does not signal the tail as it stands; it first freezes
  that tail into a claim in the manifest log, and signals the claim. Everything
  written afterwards accumulates for the *next* claim, and while a claim is
  outstanding the re-signal repeats it verbatim. A sealer therefore merges the same
  ids no matter how many times it is told, or which side of the handoff crashed —
  see `Smolquery.BufferService.HotManifest` for why that is what makes sealing
  exactly-once.

  The claim also names its output, derived from its inputs, so a table's sealed
  segment has a stable identity before any bytes exist. One key per claim, and
  the claim takes the oldest unsealed entries up to a byte valve of
  16 × `seal_max_bytes`. The valve bounds two things at once: how large one
  sealed segment can get, and how many bytes the merge stages in its temp
  table — engine memory and spill are finite, and a claim past them would
  fail the same way on every retry. A backlog past the valve retires in
  valve-sized claims. The remainder still crosses `seal_max_bytes`, so the
  next maintenance tick claims it as soon as this claim retires — a table
  under sustained ingest self-corrects (T-246). Within a claim, calls are
  bounded too: `Smolquery.StorageService.Merge` reads inputs in chunks of
  `merge_inputs_per_call`, so no call carries an unbounded `read_parquet`
  list.

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

  alias Explorer.DataFrame
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

  @claim_max_bytes_factor 16

  defstruct [
    :runtime,
    :table_ref,
    :prefix,
    :committer,
    :schema,
    :timer,
    :signaled_at,
    :load,
    :opened_at,
    chunks: [],
    pending: [],
    batch_ids: [],
    row_count: 0,
    byte_size: 0,
    in_flight: 0,
    in_flight_inserts: 0,
    in_flight_ids: MapSet.new()
  ]

  # `segment_id` is `nil` for a commit that produced no rows: the flush deletes
  # that segment rather than naming it in the manifest, so there is no id to
  # report and a `row_count` of zero is the whole answer.
  @type ack :: %{segment_id: String.t() | nil, row_count: non_neg_integer()}

  @typedoc """
  Rows a flush refused, at their index in the caller's body.

  Structurally what `Smolquery.IngestService.Validator` produces, declared here
  because `buffer_service -> ingest_service` is forbidden and the buffer must
  stay deployable without the ingest edge.
  """
  @type row_errors :: %{index: non_neg_integer(), errors: [%{message: String.t()}]}

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
  def write(buffer, %Schema{} = schema, rows, timeout, batch_id \\ nil) when is_list(rows) do
    GenServer.call(buffer, {:write, schema, rows, batch_id, :erlang.external_size(rows)}, timeout)
  end

  @doc """
  Accumulates an already-columnar batch — same contract as `write/5`, with
  the rows as an `Explorer.DataFrame` instead of a term list (T-139).

  `byte_size` is the caller's estimate of what the batch costs this node —
  the ingest edge passes the wire size it parsed the frame from.
  `:erlang.external_size/1` would answer a few words here: the frame's data
  lives behind a NIF resource, not on the BEAM heap, so the admission bound
  has to be told what it cannot measure.
  """
  @spec write_frame(
          GenServer.server(),
          Schema.t(),
          DataFrame.t(),
          non_neg_integer(),
          timeout(),
          String.t() | nil
        ) :: {:ok, ack()} | {:duplicate, ack()} | {:error, term()}
  def write_frame(
        buffer,
        %Schema{} = schema,
        %DataFrame{} = frame,
        byte_size,
        timeout,
        batch_id \\ nil
      ) do
    GenServer.call(buffer, {:write, schema, frame, batch_id, byte_size}, timeout)
  end

  @doc """
  Accumulates an unparsed NDJSON body and returns once it is durable.

  `row_count` is the sender's count of non-blank lines, not a parse: this node
  does not read the bytes, and the flush is what turns them into a segment.
  Otherwise identical to `write_frame/6`, including the ack and the dedup
  semantics.
  """
  @spec write_ndjson(
          GenServer.server(),
          Schema.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          timeout(),
          String.t() | nil
        ) ::
          {:ok, ack()}
          | {:ok, ack(), [row_errors()]}
          | {:invalid, [row_errors()]}
          | {:duplicate, ack()}
          | {:error, term()}
  def write_ndjson(
        buffer,
        %Schema{} = schema,
        body,
        row_count,
        byte_size,
        timeout,
        batch_id \\ nil
      )
      when is_binary(body) do
    GenServer.call(
      buffer,
      {:write, schema, {:ndjson, body, row_count}, batch_id, byte_size},
      timeout
    )
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

  def handle_info({:commit_done, batch_ids, inserts}, state) do
    {:noreply, state |> settle(batch_ids, inserts) |> run_maintenance()}
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

  defp settle(state, batch_ids, inserts) do
    %{
      state
      | in_flight: state.in_flight - 1,
        in_flight_inserts: state.in_flight_inserts - inserts,
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
      {:commit_done, batch_ids, inserts} -> drain_commit_done(settle(state, batch_ids, inserts))
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
    ids =
      unsealed
      |> claim_batch(state.runtime.seal_max_bytes * @claim_max_bytes_factor)
      |> Enum.map(& &1.id)

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

  defp claim_batch([first | rest], valve) do
    {batch, _total} =
      Enum.reduce_while(rest, {[first], first.byte_size}, fn entry, {batch, total} ->
        if total + entry.byte_size > valve do
          {:halt, {batch, total}}
        else
          {:cont, {[entry | batch], total + entry.byte_size}}
        end
      end)

    Enum.reverse(batch)
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
    count = chunk_count(rows)

    state =
      state
      |> flush_on_schema_change(schema)
      |> flush_on_kind_change(rows)

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

  # An NDJSON passthrough chunk cannot share a commit with rows or a frame:
  # the committer encodes a commit one way, decided by its chunks' kind. Mixing
  # them would crash the encode for every caller in the commit, so a kind
  # change flushes exactly as a schema change does.
  defp flush_on_kind_change(%__MODULE__{chunks: []} = state, _chunk), do: state

  defp flush_on_kind_change(%__MODULE__{chunks: [head | _rest]} = state, chunk) do
    if chunk_kind(head) == chunk_kind(chunk), do: state, else: handoff(state)
  end

  defp chunk_kind({:ndjson, _body, _count}), do: :ndjson
  defp chunk_kind(_rows_or_frame), do: :mergeable

  defp accumulate(state, schema, rows, count, bytes, batch_id, from) do
    %{
      state
      | schema: schema,
        opened_at: state.opened_at || System.monotonic_time(:microsecond),
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

  defp chunk_count(rows) when is_list(rows), do: length(rows)
  defp chunk_count(%DataFrame{} = frame), do: DataFrame.n_rows(frame)

  # Counted by the sender, because counting lines again here would be a second
  # pass over bytes this node is deliberately not reading.
  defp chunk_count({:ndjson, _body, count}), do: count

  defp handoff(%__MODULE__{chunks: []} = state), do: state

  defp handoff(state) do
    batch_ids = Enum.reverse(state.batch_ids)
    inserts = Enum.count(state.pending, fn {_from, kind} -> kind != :flush end)

    # `opened_at` is when this accumulator took its first chunk, so the span it
    # closes is how long the *oldest* caller in the group has been waiting before
    # any commit work starts. That is a real term of the ack and the only one no
    # commit-side timer can see (T-181).
    Committer.commit(state.committer, %{
      schema: state.schema,
      chunks: Enum.reverse(state.chunks),
      pending: Enum.reverse(state.pending),
      batch_ids: batch_ids,
      row_count: state.row_count,
      byte_size: state.byte_size,
      opened_at: state.opened_at,
      handed_off_at: System.monotonic_time(:microsecond)
    })

    %{
      state
      | chunks: [],
        pending: [],
        batch_ids: [],
        row_count: 0,
        byte_size: 0,
        opened_at: nil,
        timer: cancel(state.timer),
        in_flight: state.in_flight + 1,
        in_flight_inserts: state.in_flight_inserts + inserts,
        in_flight_ids: Enum.into(batch_ids, state.in_flight_ids)
    }
  end

  defp full?(state, count, bytes) do
    state.row_count + count > state.runtime.max_buffered_rows or
      state.byte_size + bytes > state.runtime.max_buffered_bytes
  end

  defp schedule(%__MODULE__{timer: nil} = state) do
    tag = make_ref()
    timer = Process.send_after(self(), {:flush, tag}, flush_after(state))

    %{state | timer: {timer, tag}}
  end

  defp schedule(state), do: state

  defp flush_after(state) do
    if state.in_flight_inserts < state.runtime.commit_siblings do
      state.runtime.flush_idle_interval_ms
    else
      state.runtime.flush_interval_ms
    end
  end

  defp cancel(nil), do: nil

  defp cancel({timer, _tag}) do
    Process.cancel_timer(timer)

    nil
  end
end
