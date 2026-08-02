defmodule Smolquery.Cluster.PgGroup do
  @moduledoc """
  Which nodes host a named instance of a ring-owning service, right now.

  A node's membership in a group is tracked with `:pg` (Erlang/OTP's own
  distributed process-group registry, scoped under `Smolquery.Cluster.pg_scope/0`)
  rather than polled or gossiped — a node's membership disappears the moment its
  representative process exits, gracefully or not, with no separate heartbeat
  protocol to build. `join/3` is called with the hosting supervisor's own pid,
  alive for exactly as long as that node's subtree is, so a crash removes the
  node from the group the same way an explicit `leave/3` does.

  Shared by `Smolquery.BufferService.Routing`/`Supervisor`/`Drain` (Milestone 8
  L4, buffer ownership) and `Smolquery.StorageService.Routing`/`Supervisor`
  (Milestone 8 L6, storage seal-work ownership) — `scope` is each service's
  own top-level module (`Smolquery.BufferService`, `Smolquery.StorageService`),
  so the same instance `name` never collides between the two services' groups.

  Only meaningful when `Smolquery.Cluster.enabled?/0` — a single-node
  deployment has no Postgres to discover peers through, and `nodes/3` falls
  back to whatever static list the caller already had, so nothing here changes
  single-node behavior.
  """

  alias Smolquery.Cluster

  @doc """
  Joins `pid` to `scope`'s `name` group — this node now hosts `name`.

  A no-op when clustering is off.
  """
  @spec join(module(), atom(), pid()) :: :ok
  def join(scope, name, pid) do
    if Cluster.enabled?() do
      :ok = :pg.join(Cluster.pg_scope(), group(scope, name), pid)
    end

    :ok
  end

  @doc """
  Removes `pid` from `scope`'s `name` group.

  A no-op when clustering is off.
  """
  @spec leave(module(), atom(), pid() | nil) :: :ok
  def leave(_scope, _name, nil), do: :ok

  def leave(scope, name, pid) do
    if Cluster.enabled?() do
      :pg.leave(Cluster.pg_scope(), group(scope, name), pid)
    end

    :ok
  end

  @doc """
  Every node currently hosting `scope`'s `name`, or `static_fallback` when
  clustering is off (or no node has joined yet).
  """
  @spec nodes(module(), atom(), [node()]) :: [node()]
  def nodes(scope, name, static_fallback) do
    if Cluster.enabled?() do
      live(scope, name, static_fallback)
    else
      static_fallback
    end
  end

  defp live(scope, name, static_fallback) do
    case Cluster.pg_scope()
         |> :pg.get_members(group(scope, name))
         |> Enum.map(&node/1)
         |> Enum.uniq() do
      [] -> static_fallback
      nodes -> Enum.sort(nodes)
    end
  end

  defp group(scope, name), do: {scope, name}
end
