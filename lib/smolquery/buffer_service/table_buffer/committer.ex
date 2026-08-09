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
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Replicator
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [:buffer, :runtime, :table_ref, :prefix, :load, :log]

  @type commit :: %{
          schema: Smolquery.Schema.t(),
          chunks: [[Writer.row()] | Explorer.DataFrame.t()],
          pending: [{GenServer.from(), :new | :duplicate | :flush}],
          batch_ids: [String.t()],
          row_count: non_neg_integer(),
          byte_size: non_neg_integer()
        }

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

    {:ok,
     %__MODULE__{
       buffer: buffer,
       runtime: Keyword.fetch!(opts, :runtime),
       table_ref: Keyword.fetch!(opts, :table_ref),
       prefix: Keyword.fetch!(opts, :prefix),
       load: Keyword.fetch!(opts, :load)
     }}
  end

  @impl GenServer
  def handle_call(:open, _from, state) do
    case HotManifest.open_log(state.runtime.manifest, state.table_ref) do
      {:ok, log} -> {:reply, :ok, %{state | log: log}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  def handle_call({:with_log, fun}, _from, state), do: {:reply, fun.(state.log), state}

  @impl GenServer
  def handle_cast({:commit, commit}, state) do
    if buffer_exited?(state) do
      {:stop, :shutdown, state}
    else
      {:noreply, run(state, commit)}
    end
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %__MODULE__{log: nil}), do: :ok
  def terminate(_reason, state), do: HotManifest.close_log(state.log)

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
    duration_us = System.monotonic_time(:microsecond) - started

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

    state
  end

  defp reply_for(:new, result), do: result
  defp reply_for(:duplicate, {:ok, ack}), do: {:duplicate, ack}
  defp reply_for(:duplicate, error), do: error
  defp reply_for(:flush, {:ok, _ack}), do: :ok
  defp reply_for(:flush, error), do: error

  defp persist(state, commit) do
    with {:ok, merged} <- Writer.merge_chunks(commit.chunks, commit.schema),
         {:ok, segment} <-
           Writer.write(merged, commit.schema,
             store: state.runtime.store,
             prefix: state.prefix,
             compression: state.runtime.compression
           ),
         {:ok, entry} <- add(state, segment, commit.batch_ids),
         :ok <- replicate(state, segment, entry) do
      {:ok, %{segment_id: entry.id, row_count: entry.row_count}}
    end
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
