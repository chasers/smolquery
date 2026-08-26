defmodule Smolquery.BufferService.Replicator.SegmentShipping do
  @moduledoc """
  RF-copy durability by shipping the committed unit to ring successors
  (T-96, PL-5 Stage 1).

  The owner runs group commit exactly as before — encode, store-put,
  manifest-fsync — then, before any waiter is acked, ships the encoded
  segment bytes and the manifest entry to its followers and waits for their
  fsync acks: one replication round-trip per *flush*, amortized the same way
  the fsync is. Followers are the next `replication_factor - 1` distinct
  nodes clockwise on the ring (`Smolquery.BufferService.Ring.successors/3`),
  so the node a ring change promotes is a node that already holds every
  acked byte, and `Smolquery.BufferService.Adopter` replay is the promotion
  path with nothing added.

  ## Ack-all, and what a refusal does

  The ack rule is all-replicas (PL-5): any follower refusing or unreachable
  fails the flush. A ring with fewer nodes than `replication_factor` fails
  every write with `{:error, {:underreplicated, have, want}}` rather than
  silently acking fewer copies than configured — Kafka's last-replica-standing
  lesson, imported (PL-13).

  A flush that fails *after* the local commit would leave the owner holding
  an entry its callers were told failed — and possibly a follower holding
  one the owner compensated away. So a failed shipment is compensated: the
  entry is dropped locally and a drop is best-effort shipped to every
  follower.

  The best-effort drop alone is not enough (F-2, `tla/FINDINGS.md`,
  `tla/SegmentShipping.tla`): a follower that applied the entry — its ack
  lost — and then missed the drop holds a zombie copy that the planner's
  every-member manifest merge serves immediately, and a retried batch
  double-counts those rows forever under a fresh entry id (the compensating
  local drop also forgets the `batch_id` record, so even an idempotent retry
  commits fresh). The committer therefore also records the drop as OWED in
  the manifest log (`Smolquery.BufferService.HotManifest.owe_drop/4`), and
  the owning table buffer re-ships it on the seal cadence until the replicas
  ack, settling the record only then. The residual that remains is an owner
  that dies for good before the settle: a promoted follower re-answers the
  batch's `:batch_id` with the original ack, which is why idempotency keys
  stay the recommended write mode under replication.

  ## Mutations ship first, and past the followers

  Claims, retires, and drops replicate *before* the owner applies them
  (`append/2`): all three are idempotent, so a follower that is ahead is
  harmless, while a follower left behind freezes divergent claims — and two
  different claims over the same rows double-commit them to the catalog.

  A follower that answers a claim with `:partial_claim` is healed rather
  than retried at (T-289): the diff names the ids the follower lacks, the
  owner re-ships each missing entry — bytes and manifest record, the same
  `accept_replica` a group commit uses — and the claim is applied once
  more. Without this, the owner recomputes the same id set every
  maintenance tick, so a follower that lost some entries (a partial disk
  loss, a missed commit) answers the same refusal every ~5 s forever and
  the table never seals. The heal re-ships at most a bounded batch per
  attempt: it runs inside the owner's group-commit path, where every
  re-shipped segment is every write waiter's added latency, so a larger
  divergence converges across attempts instead of stalling the table's
  ingest in one. Only `missing` ids heal; a diff naming `sealed` ids means
  the follower already retired them and the owner is the one behind —
  re-shipping an unsealed entry over a sealed one would set up a
  double-commit at the next retire, so that divergence fails the claim
  loudly instead.

  A follower that answers a *release* with `:claim_mismatch` heals the same
  way (T-297): the refusal names the live claim the follower actually holds,
  the owner releases that claim on the follower first — release moves
  entries to pending, the pre-claim state, so nothing seals from it without
  a new claim only the owner can freeze — and then re-applies its own
  release, which the now-claimless follower absorbs as `:ok`. Without this,
  an owner releasing an oversized claim against a follower that froze a
  different set retries the identical release every tick forever, and
  sealing stalls with no path that heals it. One case stays out of the
  heal: a follower live claim holding ids the owner has already *sealed*.
  Releasing those to pending on the follower would set up a double-commit
  after a ring change, so that divergence fails loudly instead, naming the
  sealed ids — the same asymmetry the claim heal draws (T-291 owns the
  reconcile).

  Two divergences sit outside the heal's reach, deliberately documented
  rather than silently covered. A follower recreated *empty* never answers
  `:partial_claim` at all — `Smolquery.BufferService.Endpoint` acks a
  mutation for a table the node holds nothing of, so the claim replicates
  in name only and the under-replication is invisible here (T-292). And a
  follower that sealed an id and already reaped it past `retire_grace_ms`
  reports it as `missing` — the drop erased the record that would have put
  it in `sealed` — so the guard above cannot see that healing it would
  regress a retired entry (T-291). Both need the owner to learn history
  the peer no longer holds, which is reconciliation, not re-shipping.

  After the followers ack, the same mutation fans out best-effort to every
  other node the read side might ask (`Routing.manifest_nodes/1`): a ring
  change strands replicated copies on an ex-owner or ex-follower that the
  successor set no longer includes, and without hearing retires and drops
  those copies would sit unsealed forever — read-correct (the planner
  dedupes and prefers the furthest-along copy) but never reaped (T-104). A
  node holding nothing for the table answers `:ok` for free, and a node
  missing the fan-out just stays stale until the next mutation or its own
  reap; only the followers' acks gate the operation.

  The fan-out runs in its own task, off the group-commit path: its target
  set deliberately includes nodes that are down (`:expected_nodes` — a
  crashed ex-owner is exactly who it exists to reach), and a best-effort
  courtesy must not stall every claim, retire, and drop in the buffer for a
  dead node's transport timeout. Two mutations' fan-outs may therefore reach
  a stale holder out of order — harmless for the same reason missing one is:
  each mutation strictly advances an entry (claim, seal, drop), so the later
  state wins and the earlier arrival degrades to the no-op its idempotency
  already promises.

  ## Shared stores ship no bytes

  When `Smolquery.Segments.Store.shared?/1` — the T-26 fork — the segment
  already lives somewhere every node reads, and only the manifest entry
  ships. This is PL-5's "Stage 1 shrinks to replicating the manifest log".

  ## Configuration

      config :smolquery, Smolquery.BufferService,
        replicator: {Smolquery.BufferService.Replicator.SegmentShipping,
                     replication_factor: 2}

  `:targets` overrides follower resolution — a
  `(name, table_ref -> {:ok, [{transport, node, instance}]} | {:error, term()})`
  fun, the seam single-BEAM tests stand two instances up with. `:holders`
  overrides the fan-out's stale-holder resolution the same way — a
  `(name -> [{transport, node, instance}])` fun.
  """

  @behaviour Smolquery.BufferService.Replicator

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Routing
  alias Smolquery.BufferService.Transport
  alias Smolquery.Segments.Store

  @default_replication_factor 2
  @heal_batch_limit 64

  @impl Smolquery.BufferService.Replicator
  def new(opts) do
    %{
      replication_factor: Keyword.get(opts, :replication_factor, @default_replication_factor),
      targets: Keyword.get(opts, :targets),
      holders: Keyword.get(opts, :holders)
    }
  end

  @impl Smolquery.BufferService.Replicator
  def commit(config, commit) do
    with {:ok, targets} <- targets(config, commit.name, commit.table_ref),
         {:ok, bytes} <- segment_bytes(commit.store, commit.segment.key) do
      args = [commit.table_ref, commit.entry, bytes, RingEpoch.current_epoch(commit.name)]

      case ship(targets, commit.name, :accept_replica, args) do
        :ok -> :ok
        {:error, reason} -> compensate(targets, commit, reason)
      end
    end
  end

  @impl Smolquery.BufferService.Replicator
  def append(config, mutation) do
    with {:ok, targets} <- targets(config, mutation.name, mutation.table_ref) do
      epoch = RingEpoch.current_epoch(mutation.name)
      args = [mutation.table_ref, mutation.op, mutation.args, epoch]

      with :ok <- ship_mutation(targets, mutation, args, epoch) do
        config
        |> other_holders(mutation.name, targets)
        |> ship_best_effort(mutation.name, args)
      end
    end
  end

  defp ship_mutation(targets, mutation, args, epoch) do
    Enum.reduce_while(targets, :ok, fn target, :ok ->
      case apply_mutation(target, mutation, args, epoch) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp apply_mutation(target, mutation, args, epoch) do
    case ship([target], mutation.name, :apply_replica_mutation, args) do
      {:error,
       {:replication_failed, _node, {:partial_claim, %{missing: [_ | _] = missing, sealed: []}}}}
      when mutation.op == :claim ->
        heal_then_retry(target, mutation, args, epoch, missing)

      {:error, {:replication_failed, _node, {:claim_mismatch, %{live_ids: live_ids}}}}
      when mutation.op == :release ->
        release_diverged_then_retry(target, mutation, args, epoch, live_ids)

      outcome ->
        outcome
    end
  end

  defp release_diverged_then_retry(
         {_transport, node, _instance} = target,
         mutation,
         args,
         epoch,
         live_ids
       ) do
    case Enum.filter(live_ids, &sealed_on_owner?(mutation, &1)) do
      [] ->
        released = [mutation.table_ref, :release, %{ids: live_ids}, epoch]

        with :ok <- ship([target], mutation.name, :apply_replica_mutation, released),
             :ok <- ship([target], mutation.name, :apply_replica_mutation, args) do
          Logger.warning(
            "healed a diverged release on #{inspect(mutation.table_ref)}: released " <>
              "#{node}'s own live claim (#{inspect(live_ids)}) before the owner's (T-297)"
          )

          :ok
        end

      sealed ->
        {:error, {:replication_failed, node, {:diverged_claim_sealed_on_owner, sealed}}}
    end
  end

  defp sealed_on_owner?(mutation, id) do
    case HotManifest.entry(mutation.manifest, mutation.table_ref, id) do
      {:ok, entry} -> Entry.sealed?(entry)
      :error -> false
    end
  end

  defp heal_then_retry({_transport, node, _instance} = target, mutation, args, epoch, missing) do
    {batch, rest} = Enum.split(missing, @heal_batch_limit)

    with :ok <- reship(target, mutation, epoch, batch) do
      case rest do
        [] -> retry_claim(target, mutation, args, batch)
        _more -> heal_in_progress(node, mutation, batch, rest)
      end
    end
  end

  defp retry_claim({_transport, node, _instance} = target, mutation, args, healed) do
    with :ok <- ship([target], mutation.name, :apply_replica_mutation, args) do
      Logger.warning(
        "healed a partial claim on #{inspect(mutation.table_ref)}: re-shipped " <>
          "#{length(healed)} missing entries to #{node} (#{inspect(healed)})"
      )

      :ok
    end
  end

  defp heal_in_progress(node, mutation, batch, rest) do
    remaining = length(rest)

    Logger.warning(
      "partial claim heal on #{inspect(mutation.table_ref)} in progress: re-shipped " <>
        "#{length(batch)} missing entries to #{node}, #{remaining} remain for the next attempt"
    )

    {:error, {:replication_failed, node, {:heal_in_progress, remaining}}}
  end

  defp reship(target, mutation, epoch, ids) do
    Enum.reduce_while(ids, :ok, fn id, :ok ->
      case reship_entry(target, mutation, epoch, id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp reship_entry({_transport, node, _instance} = target, mutation, epoch, id) do
    with {:ok, entry} <- owner_entry(node, mutation, id),
         {:ok, bytes} <- reshippable_bytes(node, mutation.store, id, entry.key) do
      ship([target], mutation.name, :accept_replica, [mutation.table_ref, entry, bytes, epoch])
    end
  end

  defp owner_entry(node, mutation, id) do
    case HotManifest.entry(mutation.manifest, mutation.table_ref, id) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:replication_failed, node, {:heal_failed, id, :not_on_owner}}}
    end
  end

  defp reshippable_bytes(node, store, id, key) do
    case segment_bytes(store, key) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:replication_failed, node, {:heal_failed, id, reason}}}
    end
  end

  @impl Smolquery.BufferService.Replicator
  def redundancy(config), do: config.replication_factor - 1

  defp targets(%{targets: resolve}, name, table_ref) when is_function(resolve, 2),
    do: resolve.(name, table_ref)

  defp targets(config, name, table_ref) do
    routing = Routing.resolve(name)
    replicas = Ring.successors(routing.ring, table_ref, config.replication_factor)

    if length(replicas) < config.replication_factor do
      {:error, {:underreplicated, length(replicas), config.replication_factor}}
    else
      followers =
        replicas
        |> Enum.reject(&(&1 == node()))
        |> Enum.map(&{Routing.transport(routing, &1), &1, name})

      {:ok, followers}
    end
  end

  defp segment_bytes(store, key) do
    if Store.shared?(store) do
      {:ok, nil}
    else
      case File.read(Store.location(store, key)) do
        {:ok, bytes} -> {:ok, bytes}
        {:error, reason} -> {:error, {:segment_unreadable, key, reason}}
      end
    end
  end

  defp ship(targets, name, function, [table_ref | _rest] = args) do
    Enum.reduce_while(targets, :ok, fn {transport, node, instance}, :ok ->
      case Transport.invoke(
             transport,
             node,
             {:bulk, table_ref},
             function,
             [instance | args],
             timeout(name)
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:replication_failed, node, reason}}}
        other -> {:halt, {:error, {:replication_failed, node, other}}}
      end
    end)
  end

  defp other_holders(%{holders: resolve}, name, _followers) when is_function(resolve, 1),
    do: resolve.(name)

  defp other_holders(_config, name, followers) do
    follower_nodes = Enum.map(followers, fn {_transport, node, _instance} -> node end)
    routing = Routing.resolve(name)

    name
    |> Routing.manifest_nodes()
    |> Enum.reject(&(&1 == node() or &1 in follower_nodes))
    |> Enum.map(&{Routing.transport(routing, &1), &1, name})
  end

  defp ship_best_effort([], _name, _args), do: :ok

  defp ship_best_effort(targets, name, [table_ref | _rest] = args) do
    timeout = timeout(name)

    {:ok, _pid} =
      Task.start(fn ->
        Enum.each(targets, fn {transport, node, instance} ->
          _outcome =
            Transport.invoke(
              transport,
              node,
              {:bulk, table_ref},
              :apply_replica_mutation,
              [instance | args],
              timeout
            )
        end)
      end)

    :ok
  end

  defp compensate(targets, commit, reason) do
    epoch = RingEpoch.current_epoch(commit.name)
    drop = [commit.table_ref, :drop, %{ids: [commit.entry.id]}, epoch]

    _best_effort = ship(targets, commit.name, :apply_replica_mutation, drop)

    {:error, reason}
  end

  defp timeout(name) do
    Routing.resolve(name).write_timeout_ms
  end
end
