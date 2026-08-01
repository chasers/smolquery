defmodule Smolquery.BufferService.Client do
  @moduledoc """
  The only way in or out of the buffer service.

  Every other service reaches the hot tier through these functions and nothing
  else — no `GenServer.call` into a `TableBuffer`, no `Registry` lookup, no ETS
  read. That rule is what makes splitting the buffer service into its own
  deployment a config change rather than a rewrite: the transport is this module's
  business, and today it happens to be a function call.

  ## Ownership is checked here, not there

  A table belongs to exactly one buffer node. `write_batch/3` refuses a table this
  node does not own rather than quietly accepting rows that would then be
  invisible to the node that does own it. Milestone 3 runs a single-node ring, so
  the refusal is unreachable in practice and tested by configuring a ring this node
  is absent from — which is also the seam Milestone 8's network transport slots
  into.

  ## Only one kind of failure is safe to retry

  A buffer is a restartable process, so a batch can find that the pid it just
  looked up has gone. `:noproc` — the call never reached anyone — is retried,
  because nothing can have happened. Every other exit is passed through, and a
  mid-call `:killed` deliberately so: the buffer may have committed the segment
  and died before replying, and a silent retry would write those rows twice.
  Resolving that ambiguity is the caller's business, exactly as it is for a
  timeout.

  The retry is bounded and paced rather than immediate, because a `Registry`
  unregisters a dead process asynchronously. Retrying without pause simply
  rediscovers the same corpse, so a crashed buffer would surface as a spurious
  `{:error, :buffer_unavailable}` instead of the momentary blip it is. Only the
  crash path pays that delay.

  ## Availability is checked, not assumed

  The runtime a caller needs lives in `:persistent_term`, which outlives the
  processes it describes — a published runtime whose manifest has stopped would
  otherwise turn every call into an `ArgumentError` from a dead ETS table. So each
  entry point confirms the manifest is running and reports
  `{:error, :buffer_service_unavailable}` when it is not, which is the truth: the
  hot tier is not there to answer.

  ## The batch carries its schema

  The buffer service never reads the catalog, so it cannot look a table's schema
  up; the ingest edge validated against the catalog before forwarding and passes
  what it validated. See `Smolquery.BufferService.TableBuffer` for what happens
  when a batch's schema differs from the one currently accumulating.

  ## Usage

      batch = %{schema: schema, rows: [%{"id" => 1}]}

      {:ok, ack} = Smolquery.BufferService.Client.write_batch(Smolquery.BufferService, table, batch)
      #=> {:ok, %{segment_id: "01K...", row_count: 1}}

  """

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @type batch :: %{required(:schema) => Schema.t(), required(:rows) => [Writer.row()]}

  @retries 5
  @retry_interval_ms 10

  @doc """
  Writes a forward-batch, returning once its rows are durable and queryable.

  The instance name is always explicit — a defaulted leading argument here would
  make `write_batch(table, batch)` and `write_batch(name, table)` impossible to
  tell apart, which has already cost this codebase a bug once in
  `Smolquery.Engine`.

  Errors a caller must expect:

    * `{:error, :buffer_full}` — shed load; the ingest edge turns this into a 429
    * `{:error, {:not_owner, node}}` — route to that node instead
    * `{:error, :buffer_service_unavailable}` — this node does not run the
      `:buffer` role

  """
  @spec write_batch(atom(), Store.table_ref(), batch()) ::
          {:ok, TableBuffer.ack()} | {:error, term()}
  def write_batch(name, table_ref, %{schema: %Schema{} = schema, rows: rows})
      when is_list(rows) do
    with {:ok, runtime} <- runtime(name),
         :ok <- ensure_owner(runtime, table_ref) do
      deliver(runtime, table_ref, schema, rows, @retries)
    end
  end

  @doc """
  The table's hot manifest — every micro-segment this node holds for it.

  Entries carry the flush-time min-max stats a planner prunes on, and `sealed_at`,
  which is what makes the seal handoff exactly-once. Applying that rule is the
  planner's job: at catalog snapshot `S`, include an entry only if it is unsealed
  or `sealed_at > S`.
  """
  @spec hot_manifest(atom(), Store.table_ref()) :: {:ok, [Entry.t()]} | {:error, term()}
  def hot_manifest(name, table_ref) do
    with {:ok, runtime} <- runtime(name) do
      {:ok, HotManifest.entries(runtime.manifest, table_ref)}
    end
  end

  @doc """
  Flushes a table's accumulator now.

  For a caller that needs the tail durable without waiting out the interval — a
  test, or a drain before shutdown. Ordinary writes should let group commit do its
  job.

  A table with no buffer running has nothing accumulated, so flushing it is `:ok`
  rather than an error.
  """
  @spec flush(atom(), Store.table_ref()) :: :ok | {:error, term()}
  def flush(name, table_ref) do
    with {:ok, runtime} <- runtime(name),
         :ok <- ensure_owner(runtime, table_ref),
         {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.flush(buffer)
    else
      {:error, :noproc} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The node owning `table_ref`.
  """
  @spec owner(atom(), Store.table_ref()) :: {:ok, node()} | {:error, term()}
  def owner(name, table_ref) do
    with {:ok, runtime} <- runtime(name) do
      {:ok, Ring.owner(runtime.ring, table_ref)}
    end
  end

  defp deliver(runtime, table_ref, schema, rows, retries) do
    case buffer(runtime, table_ref) do
      {:ok, buffer} -> TableBuffer.write(buffer, schema, rows, runtime.write_timeout_ms)
      {:error, :noproc} -> retry(runtime, table_ref, schema, rows, retries)
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, {:noproc, _call} -> retry(runtime, table_ref, schema, rows, retries)
  end

  defp retry(_runtime, _table_ref, _schema, _rows, 0), do: {:error, :buffer_unavailable}

  defp retry(runtime, table_ref, schema, rows, retries) do
    Process.sleep(@retry_interval_ms)

    deliver(runtime, table_ref, schema, rows, retries - 1)
  end

  defp runtime(name) do
    with {:ok, runtime} <- Runtime.fetch(name),
         true <- is_pid(Process.whereis(Runtime.manifest(runtime.name))) do
      {:ok, runtime}
    else
      _unavailable -> {:error, :buffer_service_unavailable}
    end
  end

  defp ensure_owner(runtime, table_ref) do
    if Ring.own?(runtime.ring, table_ref) do
      :ok
    else
      {:error, {:not_owner, Ring.owner(runtime.ring, table_ref)}}
    end
  end

  defp buffer(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{pid, _value}] -> if Process.alive?(pid), do: {:ok, pid}, else: {:error, :noproc}
      [] -> start_buffer(runtime, table_ref)
    end
  end

  defp start_buffer(runtime, table_ref) do
    supervisor = {:via, PartitionSupervisor, {Runtime.buffers(runtime.name), table_ref}}
    spec = {TableBuffer, runtime: runtime, table_ref: table_ref}

    case DynamicSupervisor.start_child(supervisor, spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end
end
