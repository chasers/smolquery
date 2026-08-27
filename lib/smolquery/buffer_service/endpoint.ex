defmodule Smolquery.BufferService.Endpoint do
  @moduledoc """
  What a buffer node does when a call arrives for a table it owns.

  Every transport lands here, so this is the buffer service's server side: the
  functions are identical whether the caller was in the same BEAM or across the
  cluster, and neither the caller nor this module has to know which.

  It is also the *only* module a remote peer may invoke — `:gen_rpc`'s allowlist
  names it and nothing else, so the exported surface here is the whole remote
  attack surface. Keep it to operations that are safe to expose, and keep them
  returning tagged tuples: gen_rpc passes a remote `throw` back as a bare value,
  indistinguishable from success, so nothing here may throw.

  A node that does not run the `:buffer` role has no runtime published and
  answers `{:error, :buffer_service_unavailable}` — which is the truth, and lets
  a caller try the next owner rather than crash.
  """

  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @typedoc """
  A forward-batch: rows as a term list, already validated, or `:ndjson` — the
  bytes the client sent, forwarded unparsed, with `:row_count` the sender's
  count of non-blank lines and `:byte_size` what the body costs the owner's
  accumulator (a rows batch is measured on arrival). The flush parses NDJSON,
  so a value the schema cannot take is
  reported per row after the fact (the committer's salvage), not before.
  """
  @type batch :: %{
          required(:schema) => Schema.t(),
          optional(:rows) => [Writer.row()],
          optional(:ndjson) => binary(),
          optional(:row_count) => non_neg_integer(),
          optional(:byte_size) => non_neg_integer(),
          optional(:batch_id) => String.t()
        }

  @retries 5
  @retry_interval_ms 10

  @doc """
  Accumulates a forward-batch, returning once its rows are durable and queryable.

  Admission runs here, before the buffer's mailbox (PL-9): a batch whose
  Little's-law wait estimate exceeds the runtime's `ack_budget_ms` is refused
  with `{:error, {:overloaded, predicted_ms}}` instead of queueing toward a
  timeout. The prediction rides along so a caller knows how far behind the
  buffer is — the API turns it into a `retry-after`.

  A batch carrying a `:batch_id` is idempotent: one whose id already
  committed is answered with the original ack before admission even runs, so
  a client retrying into an overloaded buffer is not refused for work that is
  already done.

  Ownership is fenced before admission (`Smolquery.BufferService.RingEpoch`,
  T-92): a clustered node refuses a write for a table the epoch-stamped
  configuration does not give it, so two nodes with divergent `:pg` views
  cannot both accept the same table's writes. The dedup answer above is
  deliberately not fenced — repeating an ack for a batch this node already
  committed is safe from any node.
  """
  @spec write_batch(atom(), Store.table_ref(), batch()) ::
          {:ok, TableBuffer.ack()}
          | {:ok, TableBuffer.ack(), [TableBuffer.row_errors()]}
          | {:invalid, [TableBuffer.row_errors()]}
          | {:error, term()}
  def write_batch(name, table_ref, %{schema: %Schema{} = schema} = batch) do
    with {:ok, runtime} <- runtime(name),
         {:ok, payload} <- payload(runtime, batch) do
      batch_id = Map.get(batch, :batch_id)

      case committed_ack(runtime, table_ref, batch_id) do
        {:ok, ack} ->
          deduped(payload_count(payload))

          {:ok, ack}

        :error ->
          admit(runtime, table_ref, schema, payload, Map.get(batch, :byte_size), batch_id)
      end
    end
  end

  defp payload(_runtime, %{rows: rows}) when is_list(rows), do: {:ok, rows}

  # NDJSON crosses the wire as the bytes the client sent, so there is nothing to
  # decode here and nothing to validate yet — the flush parses it. `row_count` is
  # the sender's line count, and the ack is built from it, so it is carried
  # rather than recomputed.
  defp payload(_runtime, %{ndjson: body, row_count: count, byte_size: bytes})
       when is_binary(body) and is_integer(count) and is_integer(bytes),
       do: {:ok, {:ndjson, body, count}}

  defp payload(_runtime, _batch), do: {:error, :invalid_batch}

  defp payload_count(rows) when is_list(rows), do: length(rows)
  defp payload_count({:ndjson, _body, count}), do: count

  defp admit(runtime, table_ref, schema, payload, byte_size, batch_id) do
    with :ok <- RingEpoch.check_write(runtime.name, table_ref) do
      if Drain.draining?(runtime.name) do
        {:error, :draining}
      else
        deliver(runtime, table_ref, schema, payload, byte_size, batch_id, @retries)
      end
    end
  end

  defp committed_ack(_runtime, _table_ref, nil), do: :error

  defp committed_ack(runtime, table_ref, batch_id),
    do: HotManifest.batch_ack(runtime.manifest, table_ref, batch_id)

  @doc """
  Accepts a replicated group commit from a table's owner (T-96).

  The follower half of segment shipping: `bytes` land in this node's store
  and `entry` is appended verbatim to this node's manifest log, inside the
  table's own buffer process. A shipment stamped with an epoch older than
  this node's own configuration is refused — fencing applies to every write
  path, replica traffic included (PL-13).
  """
  @spec accept_replica(atom(), Store.table_ref(), Entry.t(), binary() | nil, term()) ::
          :ok | {:error, term()}
  def accept_replica(name, table_ref, %Entry{} = entry, bytes, epoch) do
    with {:ok, runtime} <- runtime(name),
         :ok <- replica_epoch_check(name, epoch),
         {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.accept_replica(buffer, entry, bytes, runtime.write_timeout_ms)
    end
  catch
    :exit, {:noproc, _call} -> {:error, :buffer_unavailable}
  end

  @doc """
  Applies a claim, retire, or drop a table's owner replicated here (T-96).

  A node holding nothing for the table answers `:ok` without starting a
  buffer: owners fan mutations out past their followers to *every* node that
  might hold stale copies (T-104), and on the nodes that hold none the no-op
  must stay cheap — a manifest read, a registry lookup, a file stat — not a
  buffer start and a manifest recovery. Holding nothing is all three checks:
  no manifest entries, no running buffer (an `accept_replica` queued in the
  buffer's mailbox may be about to create entries, and the mutation that
  compensates it must stay ordered behind it), and no manifest log on disk
  (a restarting node's entries live there until recovery replays them, and
  fast-pathing a gating claim in that window would falsely ack it).
  """
  @spec apply_replica_mutation(
          atom(),
          Store.table_ref(),
          :claim | :retire | :drop | :release | :reconciled,
          map(),
          term()
        ) :: :ok | {:error, term()}
  def apply_replica_mutation(name, table_ref, op, args, epoch)
      when op in [:claim, :retire, :drop, :release, :reconciled] do
    with {:ok, runtime} <- runtime(name),
         :ok <- replica_epoch_check(name, epoch) do
      if holds_nothing?(runtime, table_ref) do
        :ok
      else
        apply_held_mutation(runtime, table_ref, op, args)
      end
    end
  catch
    :exit, {:noproc, _call} -> {:error, :buffer_unavailable}
  end

  defp holds_nothing?(runtime, table_ref) do
    HotManifest.empty?(runtime.manifest, table_ref) and
      running_buffer(runtime, table_ref) == {:error, :noproc} and
      not log_on_disk?(runtime, table_ref)
  end

  defp log_on_disk?(runtime, table_ref) do
    case HotManifest.log_path(runtime.manifest, table_ref) do
      {:ok, path} -> File.exists?(path)
      {:error, _reason} -> false
    end
  end

  defp apply_held_mutation(runtime, table_ref, op, args) do
    with {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.apply_replica_mutation(buffer, op, args, runtime.control_timeout_ms)
    end
  end

  defp replica_epoch_check(name, epoch) do
    local = RingEpoch.current_epoch(name)

    if is_integer(local) and is_integer(epoch) and epoch < local do
      {:error, {:stale_epoch, local}}
    else
      :ok
    end
  end

  @doc """
  Every micro-segment this node holds for a table.
  """
  @spec hot_manifest(atom(), Store.table_ref()) :: {:ok, [Entry.t()]} | {:error, term()}
  def hot_manifest(name, table_ref) do
    with {:ok, runtime} <- runtime(name) do
      {:ok, HotManifest.entries(runtime.manifest, table_ref)}
    end
  end

  @doc """
  The sealed-segment keys of the table's release tombstones (T-386).

  What the compactor asks before it merges: a registered segment under a
  tombstoned key is a released claim's orphan awaiting reconciliation, and
  folding it into a merged segment would bake its rows in past the
  reconciler's reach.
  """
  @spec tombstones(atom(), Store.table_ref()) :: {:ok, [String.t()]} | {:error, term()}
  def tombstones(name, table_ref) do
    with {:ok, runtime} <- runtime(name) do
      {:ok,
       runtime.manifest
       |> HotManifest.tombstones(table_ref)
       |> Enum.flat_map(& &1.keys)}
    end
  end

  @doc """
  Stamps segments as sealed at the catalog snapshot a sealer committed them in.

  A buffer dying between the lookup and the call comes back as
  `{:error, :buffer_unavailable}` rather than an exit — retire is idempotent, so
  the sealer just retries.
  """
  @spec retire(atom(), Store.table_ref(), [String.t()], non_neg_integer(), [String.t()] | nil) ::
          :ok | {:error, term()}
  def retire(name, table_ref, ids, snapshot, keys \\ nil) do
    with {:ok, runtime} <- runtime(name),
         {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.retire(buffer, ids, snapshot, keys)
    end
  catch
    :exit, {:noproc, _call} -> {:error, :buffer_unavailable}
  end

  @doc """
  Clears a released claim's tombstone, on the storage side's confirmation that
  no segment under `keys` stays registered (T-386).
  """
  @spec release_reconciled(atom(), Store.table_ref(), [String.t()]) :: :ok | {:error, term()}
  def release_reconciled(name, table_ref, keys) do
    with {:ok, runtime} <- runtime(name),
         {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.release_reconciled(buffer, keys)
    end
  catch
    :exit, {:noproc, _call} -> {:error, :buffer_unavailable}
  end

  @doc """
  Flushes a table's accumulator without waiting out the interval.

  A table with no buffer running has nothing accumulated, so this never starts
  one — starting a buffer means a full manifest recovery, all to flush an empty
  accumulator.
  """
  @spec flush(atom(), Store.table_ref()) :: :ok | {:error, term()}
  def flush(name, table_ref) do
    with {:ok, runtime} <- runtime(name),
         {:ok, buffer} <- running_buffer(runtime, table_ref) do
      TableBuffer.flush(buffer)
    else
      {:error, :noproc} -> :ok
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, {:noproc, _call} -> :ok
  end

  defp deliver(runtime, table_ref, schema, payload, byte_size, batch_id, retries) do
    case buffer(runtime, table_ref) do
      {:ok, buffer} ->
        admit_and_write(runtime, table_ref, buffer, schema, payload, byte_size, batch_id)

      {:error, :noproc} ->
        retry(runtime, table_ref, schema, payload, byte_size, batch_id, retries)

      {:error, reason} ->
        {:error, reason}
    end
  catch
    :exit, {:noproc, _call} ->
      retry(runtime, table_ref, schema, payload, byte_size, batch_id, retries)
  end

  defp admit_and_write(runtime, table_ref, buffer, schema, payload, byte_size, batch_id) do
    load = load(runtime, table_ref)
    count = payload_count(payload)

    case Load.admit(load, count, runtime.ack_budget_ms) do
      :ok ->
        Load.enter(load, count)

        case buffer_write(buffer, runtime, schema, payload, byte_size, batch_id) do
          {:ok, ack} ->
            {:ok, ack}

          # The flush found rows this body's schema refuses. Its valid rows are in
          # the segment that committed, so this is a partial success, exactly as
          # the JSON route reports one.
          {:ok, ack, errors} ->
            {:ok, ack, errors}

          # Nothing in this body survived, so nothing was made durable for it.
          # No `Load.leave/2` here: the rows were accumulated, so the commit's
          # own `Load.drained/2` already removed them — leaving too would drive
          # the meter negative, the one drift direction admission cannot absorb.
          {:invalid, errors} ->
            {:invalid, errors}

          {:duplicate, ack} ->
            Load.leave(load, count)
            deduped(count)

            {:ok, ack}

          {:error, reason} ->
            Load.leave(load, count)

            {:error, reason}
        end

      {:error, reason} ->
        :telemetry.execute(
          [:smolquery, :buffer, :admission],
          %{rows: count},
          %{outcome: :refused}
        )

        {:error, reason}
    end
  end

  defp buffer_write(buffer, runtime, schema, rows, _byte_size, batch_id) when is_list(rows) do
    TableBuffer.write(buffer, schema, rows, runtime.write_timeout_ms, batch_id)
  end

  defp buffer_write(buffer, runtime, schema, {:ndjson, body, count}, byte_size, batch_id) do
    TableBuffer.write_ndjson(
      buffer,
      schema,
      body,
      count,
      byte_size,
      runtime.write_timeout_ms,
      batch_id
    )
  end

  defp deduped(count) do
    :telemetry.execute([:smolquery, :buffer, :dedup], %{rows: count}, %{})
  end

  defp load(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{_pid, load}] when is_reference(load) -> load
      _absent_or_unpublished -> nil
    end
  end

  defp retry(_runtime, _table_ref, _schema, _payload, _byte_size, _batch_id, 0),
    do: {:error, :buffer_unavailable}

  defp retry(runtime, table_ref, schema, payload, byte_size, batch_id, retries) do
    Process.sleep(@retry_interval_ms)

    deliver(runtime, table_ref, schema, payload, byte_size, batch_id, retries - 1)
  end

  defp runtime(name) do
    with {:ok, runtime} <- Runtime.fetch(name),
         true <- is_pid(Process.whereis(Runtime.manifest(runtime.name))) do
      {:ok, runtime}
    else
      _unavailable -> {:error, :buffer_service_unavailable}
    end
  end

  defp buffer(runtime, table_ref) do
    case running_buffer(runtime, table_ref) do
      {:ok, pid} -> {:ok, pid}
      {:error, :noproc} -> start_buffer(runtime, table_ref)
    end
  end

  defp running_buffer(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{pid, _value}] -> if Process.alive?(pid), do: {:ok, pid}, else: {:error, :noproc}
      [] -> {:error, :noproc}
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
