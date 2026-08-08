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

  ## Encodes run concurrently; everything durable stays serial (T-169)

  The encode — merge, sort, Parquet, the store put — is the group commit's
  CPU, and it needs nothing from the log, so up to `encode_concurrency`
  commits encode in parallel as linked tasks. What each encode *becomes*
  durable through — the manifest append and fsync, the replication round,
  the callers' replies, the load accounting — runs in this process, one
  commit at a time, in encode-completion order. Completion order is not
  handoff order, and nothing here needs it to be: segments are independent
  entries, batch-id dedup is set-shaped on both ends, and a caller only
  ever waits on its own commit.

  The with_log ordering guarantee survives because a commit's manifest add
  and its replication round finish inside one serialized step: a claim
  reaching the log can only run between finishes, so every entry it can
  freeze has already replicated, exactly as when commits were fully serial.

  ## Commits queue; nothing re-runs

  Handoffs are casts; a burst of flushes becomes encode tasks up to the
  concurrency bound and a FIFO of waiting commits behind them. Before
  accepting a commit, the committer probes for its buffer's exit: a killed
  buffer's queued batches were never acked, so they are dropped rather than
  committed to callers who already saw the crash. With nothing queued, the
  buffer is this process's OTP parent, so gen_server handles the exit
  itself — `terminate/2` closes the log either way, and the encode tasks
  are linked, so they die with it. The registered name doubles as an
  exclusion lock — a restarted buffer cannot recover the table or reopen
  the log while the previous committer still holds it.

  `sync/2` is the barrier the buffer uses where it must not run ahead: a
  schema-change flush, a drain's force-seal, an in-flight duplicate, a
  graceful shutdown. It replies once every queued and encoding commit has
  finished.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  defstruct [
    :buffer,
    :runtime,
    :table_ref,
    :prefix,
    :load,
    :log,
    queue: :queue.new(),
    encoding: %{},
    syncers: []
  ]

  @typedoc """
  A group commit handed over by the buffer.

  `opened_at` and `handed_off_at` are monotonic microseconds stamping when the
  accumulator took its first chunk and when it cast — the two spans of the ack
  that happen before this process sees the commit, and so the two no timer here
  could measure (T-181). Both are optional: a commit built by hand carries
  neither, and an unmeasured span is reported as 0.
  """
  @type commit :: %{
          :schema => Smolquery.Schema.t(),
          :chunks => [[Writer.row()] | Explorer.DataFrame.t()],
          :pending => [{GenServer.from(), :new | :duplicate | :flush}],
          :batch_ids => [String.t()],
          :row_count => non_neg_integer(),
          :byte_size => non_neg_integer(),
          optional(:opened_at) => integer() | nil,
          optional(:handed_off_at) => integer() | nil
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

  def handle_call(:sync, from, state) do
    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | syncers: [from | state.syncers]}}
    end
  end

  def handle_call({:with_log, fun}, _from, state), do: {:reply, fun.(state.log), state}

  @impl GenServer
  def handle_cast({:commit, commit}, state) do
    if buffer_exited?(state) do
      {:stop, :shutdown, state}
    else
      {:noreply, accept(state, commit)}
    end
  end

  @impl GenServer
  def handle_info({ref, result}, state) when is_map_key(state.encoding, ref) do
    Process.demonitor(ref, [:flush])
    {{commit, started}, encoding} = Map.pop(state.encoding, ref)

    %{state | encoding: encoding}
    |> finish(commit, result, started)
    |> start_queued()
    |> settle_syncers()
    |> then(&{:noreply, &1})
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.encoding, ref) do
    {{commit, started}, encoding} = Map.pop(state.encoding, ref)

    %{state | encoding: encoding}
    |> finish(commit, {:error, {:encode_crashed, reason}}, started)
    |> start_queued()
    |> settle_syncers()
    |> then(&{:noreply, &1})
  end

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

  defp idle?(state), do: :queue.is_empty(state.queue) and map_size(state.encoding) == 0

  defp accept(state, commit) do
    if map_size(state.encoding) < state.runtime.encode_concurrency do
      start_encode(state, commit)
    else
      %{state | queue: :queue.in(commit, state.queue)}
    end
  end

  defp start_queued(state) do
    with false <- map_size(state.encoding) >= state.runtime.encode_concurrency,
         {{:value, commit}, queue} <- :queue.out(state.queue) do
      start_encode(%{state | queue: queue}, commit)
    else
      _at_capacity_or_empty -> state
    end
  end

  defp start_encode(state, commit) do
    started = System.monotonic_time(:microsecond)
    runtime = state.runtime
    prefix = state.prefix

    task = Task.async(fn -> encode(runtime, prefix, commit) end)

    %{state | encoding: Map.put(state.encoding, task.ref, {commit, started})}
  end

  defp encode(runtime, prefix, commit) do
    if ndjson_commit?(commit) do
      encode_ndjson(runtime, prefix, commit)
    else
      with {:ok, merged} <- Writer.merge_chunks(commit.chunks, commit.schema) do
        Writer.write(merged, commit.schema,
          store: runtime.store,
          prefix: prefix,
          compression: runtime.compression
        )
      end
    end
  end

  defp ndjson_commit?(%{chunks: [{:ndjson, _body, _count} | _rest]}), do: true
  defp ndjson_commit?(_commit), do: false

  # The bytes reach disk here rather than at the API, because the API no longer
  # knows which node owns the table — #108's partitions send most batches to a
  # peer, and a path on the sender's disk means nothing to the owner. Spooling on
  # the owner is what makes the unparsed body survive the hop.
  #
  # The segment id is minted before the write so the pool can be selected on it,
  # spreading a table's concurrent encodes across DuckDB instances instead of
  # queueing them all on one ADBC connection.
  defp encode_ndjson(runtime, prefix, commit) do
    id = Id.generate()
    paths = spool(runtime, id, commit.chunks)

    try do
      Writer.write({:ndjson, paths}, commit.schema,
        store: runtime.store,
        prefix: prefix,
        id: id,
        engine: Runtime.engine_for(runtime, id),
        compression: runtime.compression
      )
    after
      Enum.each(paths, &File.rm/1)
    end
  end

  defp spool(runtime, id, chunks) do
    File.mkdir_p!(runtime.spool_dir)

    chunks
    |> Enum.with_index()
    |> Enum.map(fn {{:ndjson, body, _count}, index} ->
      path = Path.join(runtime.spool_dir, "#{id}-#{index}.ndjson")
      File.write!(path, body)
      path
    end)
  end

  defp finish(state, commit, encoded, started) do
    encoded_at = System.monotonic_time(:microsecond)

    {result, added_at} = commit_durably(state, commit, encoded, encoded_at)

    done_at = System.monotonic_time(:microsecond)
    duration_us = done_at - started

    Load.drained(state.load, commit.row_count)

    if match?({:ok, _ack}, result) do
      Load.sample_rate(state.load, commit.row_count, duration_us)
    end

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{
        rows: commit.row_count,
        bytes: commit.byte_size,
        duration_us: duration_us,
        accumulate_us: span(commit[:opened_at], commit[:handed_off_at]),
        queue_us: span(commit[:handed_off_at], started),
        encode_us: encoded_at - started,
        manifest_us: added_at - encoded_at,
        replicate_us: done_at - added_at
      },
      %{result: if(match?({:ok, _ack}, result), do: :ok, else: :error)}
    )

    Enum.each(commit.pending, fn {from, kind} ->
      GenServer.reply(from, reply_for(kind, result))
    end)

    send(state.buffer, {:commit_done, commit.batch_ids})

    state
  end

  # Split so the manifest append and the replication round can be timed apart:
  # they are the two serialized steps, and T-181 exists because the encode —
  # the one step that already runs in parallel — turned out to be 5% of the ack.
  defp commit_durably(state, commit, {:ok, segment}, _encoded_at) do
    added = add(state, segment, commit.batch_ids)
    added_at = System.monotonic_time(:microsecond)

    result =
      with {:ok, entry} <- added,
           :ok <- replicate(state, segment, entry) do
        {:ok, %{segment_id: entry.id, row_count: entry.row_count}}
      end

    {result, added_at}
  end

  defp commit_durably(_state, _commit, {:error, _reason} = error, encoded_at),
    do: {error, encoded_at}

  # A commit built before this instrumentation shipped, or one a test hands over
  # by hand, carries no stamps; an unmeasured span is 0 rather than a crash.
  defp span(nil, _to), do: 0
  defp span(_from, nil), do: 0
  defp span(from, to), do: to - from

  defp settle_syncers(state) do
    if idle?(state) do
      Enum.each(state.syncers, &GenServer.reply(&1, :ok))

      %{state | syncers: []}
    else
      state
    end
  end

  defp reply_for(:new, result), do: result
  defp reply_for(:duplicate, {:ok, ack}), do: {:duplicate, ack}
  defp reply_for(:duplicate, error), do: error
  defp reply_for(:flush, {:ok, _ack}), do: :ok
  defp reply_for(:flush, error), do: error

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
