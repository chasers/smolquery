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
     refuses new writes with `{:error, :draining}` from this point on, and
     `TableBuffer` refuses them again at its own mailbox, which is the check
     with a happens-before: a write that passed the endpoint's gate just
     before the flag went up cannot slip into an accumulator after the
     force-seal below and be acked on a node about to leave. This is an
     honest, bounded write-unavailability window, not a correctness gap: a
     caller retries and, once step 3 completes, reaches whatever node the
     ring now names instead.
  2. Force-seal every table with a running `TableBuffer` on this node
     (`TableBuffer.force_seal/2`), regardless of the size/age thresholds
     that gate a normal seal — and do it again on *every* poll pass, over a
     freshly-read registry. Re-deriving catches a buffer that started
     between the flag and the first pass; re-forcing catches entries a
     first pass could not claim because a normal seal's claim was still
     live — they become claimable the moment that claim retires, and a
     single point-in-time force would strand them into a guaranteed
     timeout.
  3. Between force passes, poll until every entry of every owned table is
     sealed — `StorageService.Handoff` completed the merge/registration and
     called back `Client.retire/4` — then leave the ring
     (`Smolquery.Cluster.PgGroup`). A `:timeout_ms` expiry does **not**
     leave, and does not clear the draining flag either: the retried
     `drain/2` re-polls the same in-flight handoffs and returns
     `{:error, {:drain_timeout, table_refs}}` so an operator can see *why*
     — a slow object-store upload, a wedged catalog commit — rather than
     the node silently refusing to leave.
  """

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Cluster.PgGroup

  @default_poll_ms 200
  @default_timeout_ms 120_000

  @doc """
  Drains `name`'s buffer instance on this node.

  ## Options

    * `:timeout_ms` — how long to wait for every forced seal to retire
      before giving up (default #{@default_timeout_ms}). A forced claim holds
      a table's whole unsealed tail. A large tail merges in several bounded
      engine calls (`merge_inputs_per_call`), so the default gives that one
      big seal room to finish. Merge time grows with the tail, so no fixed
      default covers every backlog — pass a larger `:timeout_ms` to drain a
      node that has been failing to seal for a long time
    * `:poll_ms` — how often the retirement check runs (default
      #{@default_poll_ms})

  """
  @spec drain(atom(), keyword()) :: :ok | {:error, term()}
  def drain(name, opts \\ []) do
    case Runtime.fetch(name) do
      {:ok, runtime} ->
        :persistent_term.put(draining_key(name), true)

        timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
        poll_ms = Keyword.get(opts, :poll_ms, @default_poll_ms)
        deadline = System.monotonic_time(:millisecond) + timeout_ms

        case wait_for_retirement(runtime, deadline, poll_ms) do
          :ok -> leave(name)
          {:error, _reason} = error -> error
        end

      :error ->
        {:error, :buffer_service_unavailable}
    end
  end

  @doc """
  The rollout half of leaving (T-97): refuse new writes, flush what is
  accumulated, leave the ring — and seal nothing.

  `drain/2` is for shrinking the fleet: it force-seals because the node's
  disk holds the only copy and nobody is coming back for it. A rolling
  restart is the opposite situation — the same pod returns in seconds, and
  under replication (T-96) every acked row is already on a follower's disk,
  so force-sealing every table on every deploy would be pure churn. The
  flush pass commits (and therefore replicates) whatever is accumulated but
  not yet acked-and-shipped, so the successor's copy is complete before the
  ring stops naming this node.

  Without a replicating `Smolquery.BufferService.Replicator` this is still
  safe but not seamless: the unsealed tail becomes unreachable until the pod
  returns and re-adopts, which the planner's expected-nodes rule (T-94)
  surfaces as a failed read rather than a short answer.

  A flush that fails is logged and skipped rather than blocking the exit —
  the pod is stopping either way, and rows that failed to flush were never
  acked. The flush wait is sized to the commit's real bound — a replicated
  commit holds the buffer for up to its `write_timeout_ms` — so a busy
  table's handoff flush is not abandoned at a smaller default while its
  commit is still in flight.
  """
  @spec handoff(atom()) :: :ok | {:error, term()}
  def handoff(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} ->
        :persistent_term.put(draining_key(name), true)
        Enum.each(owned_tables(runtime), &flush_table(runtime, &1))
        leave(name)

      :error ->
        {:error, :buffer_service_unavailable}
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
    Smolquery.BufferService
    |> PgGroup.Member.process_name(name)
    |> PgGroup.Member.leave()
  end

  defp draining_key(name), do: {__MODULE__, name}

  defp owned_tables(runtime) do
    Registry.select(Runtime.registry(runtime.name), [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  defp flush_table(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{pid, _load}] -> TableBuffer.flush(pid, commit_bound_ms(runtime))
      [] -> :ok
    end
  catch
    :exit, reason ->
      Logger.warning("handoff: flush of #{inspect(table_ref)} failed: #{inspect(reason)}")
      :ok
  end

  defp force_seal_table(runtime, table_ref) do
    case Registry.lookup(Runtime.registry(runtime.name), table_ref) do
      [{pid, _load}] -> TableBuffer.force_seal(pid, commit_bound_ms(runtime))
      [] -> :ok
    end
  catch
    :exit, reason ->
      Logger.warning("drain: force-seal of #{inspect(table_ref)} failed: #{inspect(reason)}")
      :ok
  end

  defp commit_bound_ms(runtime), do: runtime.write_timeout_ms + 1_000

  defp wait_for_retirement(runtime, deadline, poll_ms) do
    tables = owned_tables(runtime)
    Enum.each(tables, &force_seal_table(runtime, &1))

    case unretired(runtime, tables) do
      [] ->
        :ok

      pending ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:drain_timeout, pending}}
        else
          Process.sleep(poll_ms)
          wait_for_retirement(runtime, deadline, poll_ms)
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
