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

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Load
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
  """
  @spec write_batch(atom(), Store.table_ref(), batch()) ::
          {:ok, TableBuffer.ack()} | {:error, term()}
  def write_batch(name, table_ref, %{schema: %Schema{} = schema, rows: rows} = batch)
      when is_list(rows) do
    with {:ok, runtime} <- runtime(name) do
      batch_id = Map.get(batch, :batch_id)

      case committed_ack(runtime, table_ref, batch_id) do
        {:ok, ack} -> {:ok, ack}
        :error -> deliver(runtime, table_ref, schema, rows, batch_id, @retries)
      end
    end
  end

  defp committed_ack(_runtime, _table_ref, nil), do: :error

  defp committed_ack(runtime, table_ref, batch_id),
    do: HotManifest.batch_ack(runtime.manifest, table_ref, batch_id)

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

    with :ok <- Load.admit(load, count, runtime.ack_budget_ms) do
      Load.enter(load, count)

      case TableBuffer.write(buffer, schema, rows, runtime.write_timeout_ms, batch_id) do
        {:ok, ack} ->
          {:ok, ack}

        {:duplicate, ack} ->
          Load.leave(load, count)

          {:ok, ack}

        {:error, reason} ->
          Load.leave(load, count)

          {:error, reason}
      end
    end
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
