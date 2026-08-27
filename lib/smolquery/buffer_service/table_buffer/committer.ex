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

  ## The heap goes back down (T-330)

  A commit arrives here as a copy, so this process's heap grows to hold every
  payload it accepts. The heap does not shrink on its own: the garbage lands
  on the old heap, and only a fullsweep collects an old heap. So this process
  starts under `Smolquery.BufferService.Runtime`'s `fullsweep_after` — `0` by
  default, which makes every collection a fullsweep — and hibernates whenever
  it settles idle, with an empty queue and no encode in flight. That is the
  moment it releases the commit map, and a hibernate fullsweeps the heap down
  to the live set.

  Without both, a buffer pod ratchets to its cgroup ceiling and is OOM-killed
  holding garbage. One measurement on a loaded pod: 1,892 MB of process heaps,
  a live set of 0.0 MB, and 2,394 MB freed by a single forced collection.

  ## A map schema takes the DuckDB writer, whatever shape its rows arrived in

  Explorer cannot write a `MAP(STRING, STRING)` or `VARIANT` column (see
  `Smolquery.Schema`), so under `flush_writer: :duckdb` a rows or frame commit
  against such a schema is re-encoded as the NDJSON the passthrough path would
  have carried and written by DuckDB. The hot segment then always carries the
  sealed table's type. Under `flush_writer: :polars` the commit is refused with
  `{:error, {:flush_writer_unsupported, :polars, type}}` — a deployment's
  configuration, which the API reports as such and not as the caller's mistake.
  """

  use GenServer

  require Logger

  alias Explorer.DataFrame
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
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
          :chunks => [
            [Writer.row()] | Explorer.DataFrame.t() | {:ndjson, binary(), non_neg_integer()}
          ],
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
    GenServer.start_link(__MODULE__, {self(), opts},
      name: Keyword.fetch!(opts, :name),
      spawn_opt: Runtime.spawn_opt(Keyword.fetch!(opts, :runtime))
    )
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
  once the batch is durable, then reports `{:commit_done, batch_ids,
  insert_count}` to the buffer — the count is the commit's `:new` and
  `:duplicate` callers, which the buffer's adaptive wait settles against
  its in-flight total.
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
    |> noreply()
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.encoding, ref) do
    {{commit, started}, encoding} = Map.pop(state.encoding, ref)

    %{state | encoding: encoding}
    |> finish(commit, {:error, {:encode_crashed, reason}}, started)
    |> start_queued()
    |> settle_syncers()
    |> noreply()
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

  defp noreply(state) do
    if idle?(state) do
      {:noreply, state, :hibernate}
    else
      {:noreply, state}
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
    cond do
      ndjson_commit?(commit) ->
        encode_ndjson(runtime, prefix, commit)

      Schema.explorer_writable?(commit.schema) ->
        with {:ok, merged} <- Writer.merge_chunks(commit.chunks, commit.schema) do
          Writer.write(merged, commit.schema,
            store: runtime.store,
            prefix: prefix,
            compression: runtime.compression
          )
        end

      runtime.flush_writer == :duckdb ->
        encode_ndjson(runtime, prefix, %{commit | chunks: Enum.map(commit.chunks, &as_ndjson/1)})

      true ->
        {:ok, %Field{type: type}} = Schema.explorer_unwritable(commit.schema)
        {:error, {:flush_writer_unsupported, runtime.flush_writer, type}}
    end
  end

  defp ndjson_commit?(%{chunks: [{:ndjson, _body, _count} | _rest]}), do: true
  defp ndjson_commit?(_commit), do: false

  defp as_ndjson(rows) when is_list(rows),
    do: {:ndjson, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n", length(rows)}

  defp as_ndjson(%DataFrame{} = frame),
    do: {:ndjson, DataFrame.dump_ndjson!(frame), DataFrame.n_rows(frame)}

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
    engine = Runtime.engine_for(runtime, id)
    paths = spool(runtime, id, commit.chunks)

    try do
      case write_ndjson(runtime, prefix, commit, id, engine, paths) do
        {:ok, segment} -> {:ok, segment}
        {:error, _reason} -> salvage(runtime, prefix, commit, id, engine, paths)
      end
    after
      Enum.each(paths, &File.rm/1)
    end
  end

  defp write_ndjson(runtime, prefix, commit, id, engine, paths) do
    Writer.write({:ndjson, paths}, commit.schema,
      store: runtime.store,
      prefix: prefix,
      id: id,
      engine: engine,
      compression: runtime.compression
    )
  end

  # A group commit merges several callers' bodies, so one unparseable value would
  # otherwise fail every caller that shared the commit — including the ones whose
  # rows were fine. Worse, the failure is deterministic, so a client retrying a bad
  # batch would keep poisoning its neighbours.
  #
  # Only reached when the whole-commit COPY already failed, so the cost is paid by
  # the batch that caused it. Each body is checked on its own, the good ones commit
  # together, and a bad one is decoded through the per-row validator so its caller
  # gets the same `insertErrors` the JSON route would have given it. Rows that are
  # valid inside a bad body are re-spooled and committed too — rejecting a whole
  # request for one row is what this path is fixing.
  defp salvage(runtime, prefix, commit, id, engine, paths) do
    {good, bad} =
      paths
      |> Enum.with_index()
      |> Enum.split_with(fn {path, _index} ->
        Writer.readable_ndjson?(engine, path, commit.schema)
      end)

    if bad == [] do
      {:error, :ndjson_commit_failed}
    else
      recovered = Enum.map(bad, &recover(runtime, commit, id, &1))

      salvaged =
        Enum.map(good, fn {path, _index} -> path end) ++ Enum.flat_map(recovered, & &1.paths)

      rejected = Map.new(recovered, &{&1.index, &1.errors})

      rewrite(runtime, prefix, commit, id, engine, salvaged, rejected)
    end
  after
    Enum.each(Path.wildcard(Path.join(runtime.spool_dir, "#{id}-*-valid.ndjson")), &File.rm/1)
  end

  # Nothing left to write means every body in the commit was bad, so the callers
  # are answered from their errors alone and no segment is made durable.
  defp rewrite(_runtime, _prefix, _commit, _id, _engine, [], rejected), do: {:rejected, rejected}

  defp rewrite(runtime, prefix, commit, id, engine, keep, rejected) do
    case write_ndjson(runtime, prefix, commit, id, engine, keep) do
      {:ok, segment} -> {:ok, segment, rejected}
      {:error, _reason} -> {:rejected, rejected}
    end
  end

  # The bad body, split: which rows the schema rejects, and the ones it takes.
  #
  # The validator is injected rather than called by name. Row validation belongs
  # to the ingest edge, and `buffer_service -> ingest_service` is forbidden — the
  # buffer has to stay deployable without it. `:row_validator` is configured the
  # way `:seal_consumer` already is, so the errors a caller sees here are the
  # errors the JSON route would have given it, without the dependency.
  defp recover(runtime, commit, id, {path, index}) do
    {valid, errors} =
      path
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(fn line ->
        case JSON.decode(line) do
          {:ok, row} -> row
          {:error, _reason} -> line
        end
      end)
      |> then(&validate_rows(runtime, commit.schema, &1))

    %{index: index, errors: errors, paths: respool(runtime, id, index, valid)}
  end

  # No validator configured means no way to attribute rows to a caller, so the
  # body is rejected whole rather than guessed at.
  defp validate_rows(%{row_validator: {module, function}}, schema, rows),
    do: apply(module, function, [schema, rows])

  defp validate_rows(_runtime, _schema, rows),
    do:
      {[],
       Enum.map(Enum.with_index(rows), fn {_row, index} ->
         %{index: index, errors: [%{message: "no row validator configured"}]}
       end)}

  defp respool(_runtime, _id, _index, []), do: []

  defp respool(runtime, id, index, rows) do
    path = Path.join(runtime.spool_dir, "#{id}-#{index}-valid.ndjson")

    File.write!(path, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")

    [path]
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

    {encoded, rejected} = split_rejected(encoded)
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
      %{
        result: if(match?({:ok, _ack}, result), do: :ok, else: :error),
        table_ref: state.table_ref
      }
    )

    reply_to_pending(commit.pending, result, rejected)

    send(state.buffer, {:commit_done, commit.batch_ids, inserts(commit.pending)})

    state
  end

  defp inserts(pending), do: Enum.count(pending, fn {_from, kind} -> kind != :flush end)

  # Split so the manifest append and the replication round can be timed apart:
  # they are the two serialized steps, and T-181 exists because the encode —
  # the one step that already runs in parallel — turned out to be 5% of the ack.
  defp commit_durably(state, commit, {:ok, segment}, _encoded_at) do
    case populated(state, segment) do
      {:ok, :rows} ->
        added = add(state, segment, commit.batch_ids)
        added_at = System.monotonic_time(:microsecond)

        result =
          with {:ok, entry} <- added,
               :ok <- replicate(state, segment, entry) do
            {:ok, %{segment_id: entry.id, row_count: entry.row_count}}
          end

        {result, added_at}

      {:ok, :empty} ->
        {{:ok, %{segment_id: nil, row_count: 0}}, System.monotonic_time(:microsecond)}
    end
  end

  defp commit_durably(_state, _commit, {:error, _reason} = error, encoded_at),
    do: {error, encoded_at}

  # Nothing survived the salvage, so there is no segment to make durable and the
  # rejected callers are answered from their own errors alone.
  defp commit_durably(_state, _commit, {:rejected, _rejected} = rejected, encoded_at),
    do: {rejected, encoded_at}

  # A body that carries no row still produces a valid Parquet file, and taking it
  # the rest of the way costs a manifest append, an fsync, a replication round,
  # and a permanent entry whose bounds are all `nil` — one the pruner can never
  # exclude, so every query over the table reads it forever. The spooled NDJSON
  # route makes this reachable from outside: a whitespace-only body counts as
  # lines at the edge, which is what admits it, and nothing before here parses
  # enough to know better. So the segment is dropped instead of recorded, and the
  # caller is told the truth: zero rows, no segment.
  defp populated(_state, %Segment{row_count: count}) when count > 0, do: {:ok, :rows}

  defp populated(state, %Segment{} = segment) do
    case Store.delete(state.runtime.store, segment.key) do
      :ok ->
        {:ok, :empty}

      {:error, reason} ->
        # The file is unreferenced either way — no manifest entry names it — so
        # `HotManifest.recover/2` deletes it as an orphan at the next recovery
        # of this table. Losing the disk until then is not worth failing a
        # request that asked for nothing.
        Logger.warning("could not delete an empty segment #{segment.key}: #{inspect(reason)}")

        {:ok, :empty}
    end
  end

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

  # The salvage path answers some callers differently from the rest of their
  # commit, so replies are indexed rather than broadcast. Only `:new` entries
  # carry a chunk — `:duplicate` and `:flush` join a commit without adding one —
  # so the index that identifies a rejected body counts `:new` entries alone,
  # which is the order the buffer spooled them in.
  defp reply_to_pending(pending, result, rejected) when map_size(rejected) == 0 do
    Enum.each(pending, fn {from, kind} -> GenServer.reply(from, reply_for(kind, result)) end)
  end

  defp reply_to_pending(pending, result, rejected) do
    pending
    |> Enum.reduce(0, fn {from, kind}, index ->
      case {kind, Map.fetch(rejected, index)} do
        {:new, {:ok, errors}} ->
          GenServer.reply(from, rejected_reply(result, errors))
          index + 1

        {:new, :error} ->
          GenServer.reply(from, reply_for(kind, result))
          index + 1

        _not_a_chunk ->
          GenServer.reply(from, reply_for(kind, result))
          index
      end
    end)
    |> then(fn _index -> :ok end)
  end

  # A caller whose body held bad rows is answered the way the JSON route answers
  # one: partial failure is a result, not an error. Its valid rows are in the
  # segment that just committed, so the ack is real; `errors` names the rows the
  # schema refused, at their index in that caller's body.
  defp rejected_reply({:ok, ack}, errors), do: {:ok, ack, errors}
  defp rejected_reply({:rejected, _rejected}, errors), do: {:invalid, errors}
  defp rejected_reply(error, _errors), do: error

  defp split_rejected({:ok, segment, rejected}), do: {{:ok, segment}, rejected}
  defp split_rejected({:rejected, rejected}), do: {{:rejected, rejected}, rejected}
  defp split_rejected(encoded), do: {encoded, %{}}

  # `{:rejected, rejected}` is this module's internal shape for a salvage whose
  # rewrite made nothing durable. Callers named in the map get `{:invalid, errors}`
  # via `rejected_reply/2`; everyone else's rows were lost with the commit, and no
  # caller speaks the internal shape, so they are answered with the commit error.
  defp reply_for(kind, {:rejected, _rejected}),
    do: reply_for(kind, {:error, :ndjson_commit_failed})

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

  # The owed record lands before the local drop, so a crash between the two
  # leaves the entry intact everywhere (an unreplied write) rather than a
  # replica-only zombie the drop can no longer reach (F-2, tla/FINDINGS.md).
  defp compensate(state, entry, reason) do
    owe_replica_drop(state, entry)

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

  defp owe_replica_drop(state, entry) do
    case HotManifest.owe_drop(state.runtime.manifest, state.table_ref, [entry.id], state.log) do
      :ok ->
        :ok

      {:error, owe_reason} ->
        Logger.warning(
          "recording the owed replica drop for #{inspect(state.table_ref)} failed: " <>
            "#{inspect(owe_reason)} — a replica copy of #{entry.id} may outlive this " <>
            "compensation"
        )
    end
  end
end
