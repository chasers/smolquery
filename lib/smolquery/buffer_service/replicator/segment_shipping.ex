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

  ## Mutations ship first

  Claims, retires, and drops replicate *before* the owner applies them
  (`append/2`): all three are idempotent, so a follower that is ahead is
  harmless, while a follower left behind freezes divergent claims — and two
  different claims over the same rows double-commit them to the catalog.

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
  fun, the seam single-BEAM tests stand two instances up with.
  """

  @behaviour Smolquery.BufferService.Replicator

  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Routing
  alias Smolquery.BufferService.Transport
  alias Smolquery.Segments.Store

  @default_replication_factor 2

  @impl Smolquery.BufferService.Replicator
  def new(opts) do
    %{
      replication_factor: Keyword.get(opts, :replication_factor, @default_replication_factor),
      targets: Keyword.get(opts, :targets)
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

      ship(
        targets,
        mutation.name,
        :apply_replica_mutation,
        [mutation.table_ref, mutation.op, mutation.args, epoch]
      )
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
