defmodule Smolquery.BufferService.Drain do
  @moduledoc """
  Operator-initiated ring exit for one node's buffer instance (Milestone 8
  L4, PL-11 D4).

  Draining is not what `SIGTERM` already does — M7 L7's shutdown flushes
  every accumulator in place for a rolling restart that keeps this node's
  ring position, because the same node comes back and re-adopts its tables.
  Draining is for an operator actually shrinking the fleet: a table this
  node owns needs its unsealed tail durably sealed *before* the ring stops
  naming this node its owner, because there is no replica to hand it to
  (single-copy, the same accepted window documented since Milestone 3) — a
  successor picking up ownership after this node leaves starts with an
  empty hot manifest, and any row still sitting here unsealed at that
  moment is gone for good, not merely delayed.

  ## Sequence

  1. Flag this instance draining (`draining?/1`) — `Endpoint.write_batch/3`
     refuses new writes for tables this node owns with `{:error, :draining}`
     from this point on, so nothing lands here after the point-in-time seal
     below. This is an honest, bounded write-unavailability window, not a
     correctness gap: a caller retries and, once step 4 completes, reaches
     whatever node the ring now names instead — the same shape as any other
     "this owner is temporarily unreachable" failure the write path already
     has to tolerate.
  2. Force-seal every table with a running `TableBuffer` on this node
     (`TableBuffer.force_seal/2`), regardless of the size/age thresholds
     that gate a normal seal.
  3. Poll each forced table's hot manifest until every entry is sealed —
     `StorageService.Handoff` completed the merge/registration and called
     back `Client.retire/3` — or `:timeout_ms` elapses.
  4. Leave the ring (`Smolquery.Cluster.PgGroup.leave/3`) — reached only on success; a
     step-3 timeout does **not** leave, and does not clear the draining
     flag either. A retried `drain/2` call finds the same tables already
     claimed (so it re-polls the same in-flight handoff rather than forcing
     a second, redundant seal) and returns `{:error, {:drain_timeout,
     table_refs}}` so an operator can see *why* — a slow object-store
     upload, a wedged catalog commit — rather than the node silently
     refusing to leave.
  """

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Cluster.PgGroup

  @default_poll_ms 200
  @default_timeout_ms 30_000

  @doc """
  Drains `name`'s buffer instance on this node.

  ## Options

    * `:timeout_ms` — how long to wait for every forced seal to retire
      before giving up (default #{@default_timeout_ms})
    * `:poll_ms` — how often the retirement check runs (default
      #{@default_poll_ms})

  """
  @spec drain(atom(), keyword()) :: :ok | {:error, term()}
  def drain(name, opts \\ []) do
    with {:ok, runtime} <- Runtime.fetch(name) do
      :persistent_term.put(draining_key(name), true)

      tables = owned_tables(runtime)
      Enum.each(tables, &force_seal_table(runtime, &1))

      timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
      poll_ms = Keyword.get(opts, :poll_ms, @default_poll_ms)
      deadline = System.monotonic_time(:millisecond) + timeout_ms

      case wait_for_retirement(runtime, tables, deadline, poll_ms) do
        :ok -> leave(name)
        {:error, _reason} = error -> error
      end
    end
  end

  @doc """
  Whether `name` is mid-drain on this node.

  `Endpoint.write_batch/3` checks this before accepting a write for a table
  this node owns; a node that never drained answers `false` at no cost
  (`:persistent_term.get/2`'s default).
  """
  @spec draining?(atom()) :: boolean()
  def draining?(name), do: :persistent_term.get(draining_key(name), false)

  defp leave(name) do
    name
    |> Runtime.supervisor()
    |> Process.whereis()
    |> then(&PgGroup.leave(Smolquery.BufferService, name, &1))
  end

  defp draining_key(name), do: {__MODULE__, name}

  defp owned_tables(runtime) do
    Registry.select(Runtime.registry(runtime.name), [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  defp force_seal_table(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{pid, _load}] -> TableBuffer.force_seal(pid)
      [] -> :ok
    end
  catch
    :exit, reason ->
      Logger.warning("drain: force-seal of #{inspect(table_ref)} failed: #{inspect(reason)}")
      :ok
  end

  defp wait_for_retirement(_runtime, [], _deadline, _poll_ms), do: :ok

  defp wait_for_retirement(runtime, tables, deadline, poll_ms) do
    case unretired(runtime, tables) do
      [] ->
        :ok

      pending ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:drain_timeout, pending}}
        else
          Process.sleep(poll_ms)
          wait_for_retirement(runtime, tables, deadline, poll_ms)
        end
    end
  end

  defp unretired(runtime, tables) do
    Enum.filter(tables, fn table_ref ->
      runtime.manifest
      |> HotManifest.entries(table_ref)
      |> Enum.any?(&(not Entry.sealed?(&1)))
    end)
  end
end
