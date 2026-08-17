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
  follower. The residual window — a follower that applied the entry,
  missed the drop, and was promoted before the owner could compensate —
  re-answers the batch's `:batch_id` with the original ack, which is why
  idempotency keys are the recommended write mode under replication.

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
  maintenance tick, so a follower that lost entries (a recreated pod, a
  missed commit) answers the same refusal every ~5 s forever and the table
  never seals. Only `missing` ids heal; a diff naming `sealed` ids means
  the follower already retired them and the owner is the one behind —
  re-shipping an unsealed entry over a sealed one would set up a
  double-commit at the next retire, so that divergence fails the claim
  loudly instead.

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
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Routing
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.Transport
  alias Smolquery.Segments.Store

  @default_replication_factor 2

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

      with :ok <- ship_mutation(targets, mutation, args) do
        config
        |> other_holders(mutation.name, targets)
        |> ship_best_effort(mutation.name, args)
      end
    end
  end

  defp ship_mutation(targets, mutation, args) do
    Enum.reduce_while(targets, :ok, fn target, :ok ->
      case apply_mutation(target, mutation, args) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp apply_mutation({transport, node, instance} = target, mutation, args) do
    case Transport.invoke(
           transport,
           node,
           :bulk,
           :apply_replica_mutation,
           [instance | args],
           timeout(mutation.name)
         ) do
      :ok ->
        :ok

      {:error, {:partial_claim, %{missing: [_ | _] = missing, sealed: []}}}
      when mutation.op == :claim ->
        heal_then_retry(target, mutation, args, missing)

      {:error, reason} ->
        {:error, {:replication_failed, node, reason}}

      other ->
        {:error, {:replication_failed, node, other}}
    end
  end

  defp heal_then_retry({transport, node, instance} = target, mutation, args, missing) do
    with :ok <- reship(target, mutation, missing) do
      Logger.warning(
        "healed a partial claim on #{inspect(mutation.table_ref)}: re-shipped " <>
          "#{length(missing)} missing entries to #{node} (#{inspect(missing)})"
      )

      case Transport.invoke(
             transport,
             node,
             :bulk,
             :apply_replica_mutation,
             [instance | args],
             timeout(mutation.name)
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, {:replication_failed, node, reason}}
        other -> {:error, {:replication_failed, node, other}}
      end
    end
  end

  defp reship({_transport, node, _instance} = target, mutation, missing) do
    case Runtime.fetch(mutation.name) do
      {:ok, runtime} ->
        reship_all(target, mutation, runtime, missing)

      :error ->
        {:error, {:replication_failed, node, {:heal_failed, :runtime_unavailable}}}
    end
  end

  defp reship_all({_transport, node, _instance} = target, mutation, runtime, missing) do
    epoch = RingEpoch.current_epoch(mutation.name)

    Enum.reduce_while(missing, :ok, fn id, :ok ->
      case reship_entry(target, mutation, runtime, id, epoch) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:replication_failed, node, reason}}}
      end
    end)
  end

  defp reship_entry({transport, node, instance}, mutation, runtime, id, epoch) do
    with {:ok, entry} <- owner_entry(runtime.manifest, mutation.table_ref, id),
         {:ok, bytes} <- segment_bytes(runtime.store, entry.key) do
      case Transport.invoke(
             transport,
             node,
             :bulk,
             :accept_replica,
             [instance, mutation.table_ref, entry, bytes, epoch],
             timeout(mutation.name)
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, {:heal_failed, id, reason}}
        other -> {:error, {:heal_failed, id, other}}
      end
    end
  end

  defp owner_entry(manifest, table_ref, id) do
    case HotManifest.entry(manifest, table_ref, id) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:heal_failed, id, :not_on_owner}}
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

  defp ship(targets, name, function, args) do
    Enum.reduce_while(targets, :ok, fn {transport, node, instance}, :ok ->
      case Transport.invoke(transport, node, :bulk, function, [instance | args], timeout(name)) do
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

  defp ship_best_effort(targets, name, args) do
    timeout = timeout(name)

    {:ok, _pid} =
      Task.start(fn ->
        Enum.each(targets, fn {transport, node, instance} ->
          _outcome =
            Transport.invoke(
              transport,
              node,
              :bulk,
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
