defmodule Smolquery.Cluster.Topology do
  @moduledoc """
  A read-model of the fleet, for `SmolqueryWeb.ClusterLive.Index`.

  Everything here is a snapshot — reads over what already exists
  (`Smolquery.Cluster.Membership`, `Smolquery.Cluster.PgGroup`,
  `Smolquery.BufferService.RingEpoch`, `Smolquery.BufferService.ExpectedNodes`,
  `Smolquery.BufferService.Drain`), nothing new is tracked or cached. A row's
  per-node fields (`buffer_epoch`, `draining?`) live in ETS/`persistent_term`
  on the node they describe, so reading them for anything but `node()` means
  an RPC — skipped entirely for nodes this node doesn't currently see as
  alive, since a call to an unconnected node only pays a connection attempt
  to learn what `Smolquery.Cluster.Membership` already told us.

  Storage has no epoch/drain analog (`Smolquery.StorageService.Routing` is
  `PgGroup` + `RingCache` only), so a row carries only membership for it.

  `roles` is what the node *runs* — its own `Smolquery.Roles.enabled/0`,
  read over the same RPC as the epoch — which is not the same as ring
  membership: a drained buffer node still holds the `buffer` role, and a
  node can run `query` without belonging to any ring. The page shows roles;
  the membership fields drive the drain button and the expected-node check.
  A node that is not alive has no roles to report, so it carries `[]`.

  Degrades to a single row for `node()` — alive, no ring/storage membership
  claimed unless this node's own roles say so, nil epoch, not draining —
  wherever clustering is disabled, the same way every other reader in this
  codebase falls back to single-node behavior.
  """

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.ExpectedNodes
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.Cluster
  alias Smolquery.Cluster.Membership
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Cluster.Pods
  alias Smolquery.Roles
  alias Smolquery.StorageService

  @rpc_timeout_ms 2_000

  @type row :: %{
          node: node(),
          pod: String.t(),
          alive: boolean(),
          roles: [Roles.t()],
          buffer_member: boolean(),
          storage_member: boolean(),
          expected_buffer: boolean(),
          buffer_epoch: non_neg_integer() | nil,
          draining: boolean()
        }

  @doc """
  One row per node the fleet currently knows about — every alive node, plus
  any node `Smolquery.BufferService.ExpectedNodes` still expects but that
  isn't alive right now (a crashed owner, showing up as missing rather than
  disappearing).
  """
  @spec fleet() :: [row()]
  def fleet do
    alive = alive_nodes()
    buffer_members = PgGroup.nodes(BufferService, BufferService, static_fallback(:buffer))
    storage_members = PgGroup.nodes(StorageService, StorageService, static_fallback(:storage))
    expected_buffer = expected_buffer_nodes(alive, buffer_members)

    alive
    |> Enum.concat(expected_buffer)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(&row(&1, alive, buffer_members, storage_members, expected_buffer))
  end

  defp row(node, alive, buffer_members, storage_members, expected_buffer) do
    is_alive = node in alive

    %{
      node: node,
      pod: Pods.pod_of_node(node),
      alive: is_alive,
      roles: if(is_alive, do: rpc(node, Roles, :enabled, []) || [], else: []),
      buffer_member: node in buffer_members,
      storage_member: node in storage_members,
      expected_buffer: node in expected_buffer,
      buffer_epoch: if(is_alive, do: rpc(node, RingEpoch, :current_epoch, [BufferService])),
      draining: is_alive and rpc(node, Drain, :draining?, [BufferService]) == true
    }
  end

  defp alive_nodes do
    if Cluster.enabled?(), do: Membership.members(), else: [node()]
  end

  defp static_fallback(role) do
    if Roles.enabled?(role), do: [node()], else: []
  end

  defp expected_buffer_nodes(alive, buffer_members) do
    case Enum.find(buffer_members, &(&1 in alive)) do
      nil -> []
      live_node -> rpc(live_node, ExpectedNodes, :list, [BufferService]) || []
    end
  end

  defp rpc(node, mod, fun, args) do
    if node == node() do
      apply(mod, fun, args)
    else
      case :rpc.call(node, mod, fun, args, @rpc_timeout_ms) do
        {:badrpc, _reason} -> nil
        result -> result
      end
    end
  end
end
