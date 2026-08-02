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

  @type batch :: %{
          required(:schema) => Schema.t(),
          required(:rows) => [Writer.row()],
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
          {:ok, TableBuffer.ack()} | {:error, term()}
  def write_batch(name, table_ref, %{schema: %Schema{} = schema, rows: rows} = batch)
      when is_list(rows) do
    with {:ok, runtime} <- runtime(name) do
      batch_id = Map.get(batch, :batch_id)

      case committed_ack(runtime, table_ref, batch_id) do
        {:ok, ack} ->
          deduped(rows)

          {:ok, ack}

        :error ->
          admit(runtime, table_ref, schema, rows, batch_id)
      end
    end
  end

  defp admit(runtime, table_ref, schema, rows, batch_id) do
    with :ok <- RingEpoch.check_write(runtime.name, table_ref) do
      if Drain.draining?(runtime.name) do
        {:error, :draining}
      else
        deliver(runtime, table_ref, schema, rows, batch_id, @retries)
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
  @spec apply_replica_mutation(atom(), Store.table_ref(), :claim | :retire | :drop, map(), term()) ::
          :ok | {:error, term()}
  def apply_replica_mutation(name, table_ref, op, args, epoch)
      when op in [:claim, :retire, :drop] do
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
    HotManifest.entries(runtime.manifest, table_ref) == [] and
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
  Stamps segments as sealed at the catalog snapshot a sealer committed them in.

  A buffer dying between the lookup and the call comes back as
  `{:error, :buffer_unavailable}` rather than an exit — retire is idempotent, so
  the sealer just retries.
  """
  @spec retire(atom(), Store.table_ref(), [String.t()], non_neg_integer()) ::
          :ok | {:error, term()}
  def retire(name, table_ref, ids, snapshot) do
    with {:ok, runtime} <- runtime(name),
         {:ok, buffer} <- buffer(runtime, table_ref) do
      TableBuffer.retire(buffer, ids, snapshot)
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

  defp deliver(runtime, table_ref, schema, rows, batch_id, retries) do
    case buffer(runtime, table_ref) do
      {:ok, buffer} -> admit_and_write(runtime, table_ref, buffer, schema, rows, batch_id)
      {:error, :noproc} -> retry(runtime, table_ref, schema, rows, batch_id, retries)
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, {:noproc, _call} -> retry(runtime, table_ref, schema, rows, batch_id, retries)
  end

  defp admit_and_write(runtime, table_ref, buffer, schema, rows, batch_id) do
    load = load(runtime, table_ref)
    count = length(rows)

    case Load.admit(load, count, runtime.ack_budget_ms) do
      :ok ->
        Load.enter(load, count)

        case TableBuffer.write(buffer, schema, rows, runtime.write_timeout_ms, batch_id) do
          {:ok, ack} ->
            {:ok, ack}

          {:duplicate, ack} ->
            Load.leave(load, count)
            deduped(rows)

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

  defp deduped(rows) do
    :telemetry.execute([:smolquery, :buffer, :dedup], %{rows: length(rows)}, %{})
  end

  defp load(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{_pid, load}] when is_reference(load) -> load
      _absent_or_unpublished -> nil
    end
  end

  defp retry(_runtime, _table_ref, _schema, _rows, _batch_id, 0),
    do: {:error, :buffer_unavailable}

  defp retry(runtime, table_ref, schema, rows, batch_id, retries) do
    Process.sleep(@retry_interval_ms)

    deliver(runtime, table_ref, schema, rows, batch_id, retries - 1)
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
