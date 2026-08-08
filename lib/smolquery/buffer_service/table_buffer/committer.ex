defmodule Smolquery.BufferService.TableBuffer.Committer do
  @moduledoc """
  The durability half of a table's group commit (T-152, PL-20).

  `Smolquery.BufferService.TableBuffer` accumulates; this process makes
  batches durable — the Polars encode, the store put, the manifest append and
  fsync, and the replication round all run here, and the pending callers are
  answered from here once their rows are durable. That split is what
  pipelines the buffer: batch formation continues in the `TableBuffer` while
  a commit is in flight, instead of stopping for it.

  ## One writer per manifest log, still

  This process owns the table's manifest log for its lifetime. Everything
  else that appends — retires, drops, claims, replica entries — reaches the
  log through `with_log/3`, a call this process serializes with the commits
  it is running. Routing those appends here is also an ordering guarantee:
  a claim queued behind the commit that created its entries cannot ship to a
  follower before the entries themselves have replicated.

  ## Commits queue; nothing re-runs

  Handoffs are casts and process strictly in order, so a burst of flushes
  becomes a queue of commits, each replied to as it lands. Before running a
  commit, the committer probes for its buffer's exit: a killed buffer's
  queued batches were never acked, so they are dropped rather than committed
  to callers who already saw the crash. With nothing queued, the buffer is
  this process's OTP parent, so gen_server handles the exit itself —
  `terminate/2` closes the log either way. The registered name doubles as an
  exclusion lock — a restarted buffer cannot recover the table or reopen the
  log while the previous committer still holds it.

  `sync/2` is the barrier the buffer uses where it must not run ahead: a
  schema-change flush, a drain's force-seal, an in-flight duplicate, a
  graceful shutdown.

  ## Encoding may overlap; nothing else does

  `:encode_concurrency` (default `1`) is how many Polars encodes may be in
  flight at once. Only the encode — the `Smolquery.Segments.Writer` call that
  builds the frame, sorts it on the clustering key and writes the Parquet
  bytes — leaves this process. It is the one step that touches neither the
  manifest log nor ETS, which is why it is the only step that can: the
  manifest append and the replication round stay here, one at a time, because
  the single-writer rule is what lets `Smolquery.BufferService.HotManifest`'s
  log and ETS agree without a lock, and the held file descriptor cannot take
  concurrent writes.

  At the default of `1` this process is exactly what it was: the commit runs
  inline in `handle_cast/2`, no task is spawned, and `sync/2` and `with_log/3`
  answer from the callback that received them. That is deliberate — it is
  what makes the option an A/B switch rather than a rewrite.

  Above `1`, a commit becomes a slot in a single-lane pipeline. Encodes start
  in commit order and up to `:encode_concurrency` run at once; the (N+1)th
  waits, holding its rows exactly where a queued cast holds them today, so
  resident rows are bounded by the slots rather than by the arrival rate.
  Slots *complete* in commit order too, however out of order their encodes
  finish: the log therefore records commits in the order the buffer handed
  them over, `{:commit_done, batch_ids}` reaches the buffer in that same
  order, and a segment id — minted here, before the encode is handed off —
  still sorts by when its commit was formed.

  A slot that fails fails alone. An encode that returns an error, raises, or
  exits answers its own waiters with that error and leaves the log untouched;
  the slots either side of it are unaffected, and any bytes a later step
  orphans are deleted exactly as they are on the serial path.

  `sync/2` and `with_log/3` are barriers against the pipeline as well as
  against each other: both wait for every slot ahead of them to complete, and
  commits handed over after them queue behind. A claim frozen through
  `with_log/3` therefore still cannot name an entry whose commit has not
  replicated, and an entry still encoding is simply not in that claim — it
  goes in the next one.

  ## The heap a commit's rows land on

  A commit is cast here, so its rows are copied onto this process's heap and
  are garbage the moment the commit reports done — the pattern
  `Smolquery.Heap` exists for. `:committer_fullsweep_after` and
  `:committer_min_heap_size` on the runtime set the two flags, at `init/1`,
  and the runtime documents why the defaults are what they are.

  Above `encode_concurrency: 1` the rows are copied on again to the encode's
  task, which is left on the emulator's defaults on purpose: a task that runs
  one encode and exits frees its heap by exiting, and no collection has to
  work that out.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Heap
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [
    :buffer,
    :runtime,
    :table_ref,
    :prefix,
    :load,
    :log,
    encode_concurrency: 1,
    queue: :queue.new(),
    slots: []
  ]

  @type commit :: %{
          schema: Smolquery.Schema.t(),
          columns: [[term()]] | {:ndjson, [Path.t()]},
          pending: [{GenServer.from(), :new | :duplicate | :flush}],
          batch_ids: [String.t()],
          row_count: non_neg_integer(),
          byte_size: non_neg_integer()
        }

  @typep slot :: %{
           commit: commit(),
           task: Task.t(),
           started: integer(),
           encoded: :encoding | {:ok, Segment.t()} | {:error, term()}
         }

  @typep barrier :: :sync | {:with_log, (HotManifest.log() -> term())}

  @doc """
  Starts a committer linked to the calling `TableBuffer`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, {self(), opts}, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Opens the table's manifest log, after the buffer has run recovery.

  Separate from `init/1` because recovery must run between acquiring the
  committer's name — the exclusion against a predecessor still draining —
  and opening the log it will write.
  """
  @spec open(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def open(committer, timeout \\ 15_000), do: GenServer.call(committer, :open, timeout)

  @doc """
  Enqueues a frozen batch. The committer replies to every pending caller
  once the batch is durable, then reports `{:commit_done, batch_ids}` to
  the buffer.
  """
  @spec commit(GenServer.server(), commit()) :: :ok
  def commit(committer, commit), do: GenServer.cast(committer, {:commit, commit})

  @doc """
  Returns once every previously enqueued commit has completed.
  """
  @spec sync(GenServer.server(), timeout()) :: :ok
  def sync(committer, timeout \\ :infinity), do: GenServer.call(committer, :sync, timeout)

  @doc """
  Runs `fun` with the manifest log, serialized against commits.
  """
  @spec with_log(GenServer.server(), (HotManifest.log() -> result), timeout()) :: result
        when result: var
  def with_log(committer, fun, timeout \\ :infinity),
    do: GenServer.call(committer, {:with_log, fun}, timeout)

  @impl GenServer
  def init({buffer, opts}) do
    Process.flag(:trap_exit, true)
    runtime = Keyword.fetch!(opts, :runtime)

    Heap.tune(
      fullsweep_after: runtime.committer_fullsweep_after,
      min_heap_size: runtime.committer_min_heap_size
    )

    {:ok,
     %__MODULE__{
       buffer: buffer,
       runtime: runtime,
       table_ref: Keyword.fetch!(opts, :table_ref),
       prefix: Keyword.fetch!(opts, :prefix),
       load: Keyword.fetch!(opts, :load),
       encode_concurrency: encode_concurrency(runtime)
     }}
  end

  # Refused here rather than coerced: a zero or negative bound would wedge the
  # pipeline silently, and a buffer that cannot start says so at boot.
  defp encode_concurrency(%{encode_concurrency: concurrency})
       when is_integer(concurrency) and concurrency > 0,
       do: concurrency

  @impl GenServer
  def handle_call(:open, _from, state) do
    case HotManifest.open_log(state.runtime.manifest, state.table_ref) do
      {:ok, log} -> {:reply, :ok, %{state | log: log}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:sync, from, state), do: barrier(state, from, :sync)

  def handle_call({:with_log, fun}, from, state), do: barrier(state, from, {:with_log, fun})

  @impl GenServer
  def handle_cast({:commit, commit}, %__MODULE__{encode_concurrency: 1} = state) do
    if buffer_exited?(state) do
      {:stop, :shutdown, state}
    else
      {:noreply, run(state, commit)}
    end
  end

  def handle_cast({:commit, commit}, state) do
    if buffer_exited?(state) do
      {:stop, :shutdown, state}
    else
      {:noreply, state |> enqueue({:commit, commit}) |> pump()}
    end
  end

  @impl GenServer
  def handle_info({ref, encoded}, state) when is_reference(ref) do
    if slot?(state, ref) do
      Process.demonitor(ref, [:flush])

      {:noreply, state |> resolve(ref, encoded) |> pump()}
    else
      {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    if slot?(state, ref) do
      {:noreply, state |> resolve(ref, {:error, {:encode_failed, reason}}) |> pump()}
    else
      {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # An encode still running when the log closes must not outlive it: it would
  # go on writing segments nothing can record, and a `:normal` exit does not
  # reach a linked task. What it leaves behind is a segment with no log
  # record, which recovery already deletes.
  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.slots, &Process.exit(&1.task.pid, :kill))

    close_log(state.log)
  end

  defp close_log(nil), do: :ok
  defp close_log(log), do: HotManifest.close_log(log)

  defp buffer_exited?(%__MODULE__{buffer: buffer}) do
    receive do
      {:EXIT, ^buffer, _reason} -> true
    after
      0 -> false
    end
  end

  defp run(state, commit) do
    started = System.monotonic_time(:microsecond)
    result = persist(state, commit)

    report(state, commit, result, System.monotonic_time(:microsecond) - started)

    state
  end

  defp report(state, commit, result, duration_us) do
    Load.drained(state.load, commit.row_count)

    if match?({:ok, _ack}, result) do
      Load.sample_rate(state.load, commit.row_count, duration_us)
    end

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: commit.row_count, bytes: commit.byte_size, duration_us: duration_us},
      %{result: if(match?({:ok, _ack}, result), do: :ok, else: :error)}
    )

    Enum.each(commit.pending, fn {from, kind} ->
      GenServer.reply(from, reply_for(kind, result))
    end)

    send(state.buffer, {:commit_done, commit.batch_ids})

    :ok
  end

  defp reply_for(:new, result), do: result
  defp reply_for(:duplicate, {:ok, ack}), do: {:duplicate, ack}
  defp reply_for(:duplicate, error), do: error
  defp reply_for(:flush, {:ok, _ack}), do: :ok
  defp reply_for(:flush, error), do: error

  defp persist(state, commit) do
    encoded =
      encode(state.runtime, state.prefix, commit.columns, commit.schema, [])

    durable(state, encoded, commit)
  end

  # `flush_writer: :polars` starts no write pool (`Smolquery.BufferService.Supervisor`),
  # so handing this shape to the writer would call an `Smolquery.Engine` that was
  # never started — an unhandled `:noproc` exit that takes this process, and with
  # it every unacked caller the buffer is holding, JSON and spooled alike. The API
  # refuses the body before it spools one; this clause is what keeps a shape that
  # reaches here anyway an error to its own caller rather than a process exit.
  defp encode(%Runtime{flush_writer: writer}, _prefix, {:ndjson, paths}, _schema, _opts)
       when writer != :duckdb do
    Enum.each(paths, &File.rm/1)

    {:error, {:ndjson_unsupported, writer}}
  end

  # A spooled batch is handed over as paths and written by DuckDB in one COPY. The
  # bodies are deleted once the write has finished with them, whether it succeeded
  # or not: a failed flush answers its waiters with the error and a retry spools
  # its own body again, so keeping these would only leak.
  #
  # The delete is in an `after` rather than after the call, because most of the
  # ways this write fails are not returns. A DuckDB call that times out, a column
  # name the writer refuses to quote, or a connection that dies without replying
  # all leave through an exception or an exit, and a plain statement below the
  # call is simply not reached — one leaked request body each, on a directory
  # nothing sweeps in-process.
  defp encode(runtime, prefix, {:ndjson, paths}, schema, opts) do
    # The id picks the pool member as well as naming the segment, so a table's
    # concurrent encodes spread across connections instead of queueing on one.
    {id, opts} = Keyword.pop_lazy(opts, :id, &Id.generate/0)

    Writer.write(
      {:ndjson, paths},
      schema,
      [
        store: runtime.store,
        prefix: prefix,
        id: id,
        engine: Runtime.engine_for(runtime, id),
        # Under this buffer's own `write_timeout_ms`, so a stuck DuckDB call
        # fails here with a reportable error before the caller gives up on the
        # flush and a late exit arrives with nobody left to answer.
        timeout: Runtime.engine_timeout(runtime)
      ] ++ opts
    )
  after
    Enum.each(paths, &File.rm/1)
  end

  defp encode(runtime, prefix, columns, schema, opts) do
    Writer.write({:columns, columns}, schema, [store: runtime.store, prefix: prefix] ++ opts)
  end

  defp durable(state, encoded, commit) do
    with {:ok, segment} <- encoded,
         {:ok, entry} <- add(state, segment, commit.batch_ids),
         :ok <- replicate(state, segment, entry) do
      {:ok, %{segment_id: entry.id, row_count: entry.row_count}}
    end
  end

  @spec barrier(%__MODULE__{}, GenServer.from(), barrier()) ::
          {:reply, term(), %__MODULE__{}} | {:noreply, %__MODULE__{}}
  defp barrier(state, from, request) do
    if idle?(state) do
      {:reply, run_barrier(state, request), state}
    else
      {:noreply, state |> enqueue({:barrier, from, request}) |> pump()}
    end
  end

  defp idle?(state), do: state.slots == [] and :queue.is_empty(state.queue)

  defp run_barrier(_state, :sync), do: :ok
  defp run_barrier(state, {:with_log, fun}), do: fun.(state.log)

  defp enqueue(state, item), do: %{state | queue: :queue.in(item, state.queue)}

  defp pump(state), do: state |> settle() |> admit()

  # Slots complete in commit order however their encodes finished, so the log
  # records commits in the order the buffer handed them over.
  defp settle(%__MODULE__{slots: []} = state), do: state
  defp settle(%__MODULE__{slots: [%{encoded: :encoding} | _rest]} = state), do: state

  defp settle(%__MODULE__{slots: [slot | rest]} = state) do
    state = %{state | slots: rest}
    duration_us = System.monotonic_time(:microsecond) - slot.started

    report(state, slot.commit, durable(state, slot.encoded, slot.commit), duration_us)

    settle(state)
  end

  defp admit(state) do
    case :queue.peek(state.queue) do
      :empty -> state
      {:value, {:commit, commit}} -> admit_commit(state, commit)
      {:value, {:barrier, from, request}} -> admit_barrier(state, from, request)
    end
  end

  defp admit_commit(state, commit) do
    if length(state.slots) < state.encode_concurrency do
      state |> drop_head() |> start_encode(commit) |> admit()
    else
      state
    end
  end

  defp admit_barrier(state, from, request) do
    if state.slots == [] do
      GenServer.reply(from, run_barrier(state, request))

      state |> drop_head() |> admit()
    else
      state
    end
  end

  defp drop_head(state), do: %{state | queue: :queue.drop(state.queue)}

  # The id is minted here rather than inside the task, so segment ids keep
  # sorting by the commit they belong to — which is what `HotManifest.entries/2`
  # ordering by id means. The columns leave with the task and are dropped from the
  # slot, so a slot holds one copy of its columns rather than two.
  defp start_encode(state, commit) do
    id = Id.generate()
    runtime = state.runtime
    prefix = state.prefix
    columns = commit.columns
    schema = commit.schema

    task = Task.async(fn -> encode(runtime, prefix, columns, schema, id: id) end)

    %{state | slots: state.slots ++ [open_slot(commit, task)]}
  end

  @spec open_slot(commit(), Task.t()) :: slot()
  defp open_slot(commit, task) do
    %{
      commit: %{commit | columns: []},
      task: task,
      started: System.monotonic_time(:microsecond),
      encoded: :encoding
    }
  end

  defp slot?(state, ref), do: Enum.any?(state.slots, &(&1.task.ref == ref))

  defp resolve(state, ref, encoded) do
    slots =
      Enum.map(state.slots, fn slot ->
        if slot.task.ref == ref, do: %{slot | encoded: encoded}, else: slot
      end)

    %{state | slots: slots}
  end

  defp add(state, segment, batch_ids) do
    case HotManifest.add(state.runtime.manifest, state.table_ref, segment, state.log, batch_ids) do
      {:ok, entry} ->
        {:ok, entry}

      {:error, reason} ->
        Store.delete(state.runtime.store, segment.key)

        {:error, reason}
    end
  end

  defp replicate(state, segment, entry) do
    commit = %{
      name: state.runtime.name,
      table_ref: state.table_ref,
      store: state.runtime.store,
      segment: segment,
      entry: entry
    }

    case Replicator.commit(state.runtime.replicator, commit) do
      :ok ->
        :ok

      {:error, reason} ->
        compensate(state, entry, reason)
    end
  end

  defp compensate(state, entry, reason) do
    case HotManifest.drop(state.runtime.manifest, state.table_ref, [entry.id], state.log) do
      :ok ->
        :ok

      {:error, drop_reason} ->
        Logger.warning(
          "compensating drop for #{inspect(state.table_ref)} failed: #{inspect(drop_reason)}"
        )
    end

    {:error, reason}
  end
end
