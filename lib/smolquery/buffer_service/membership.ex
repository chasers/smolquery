defmodule Smolquery.BufferService.Membership do
  @moduledoc """
  Which nodes host a named buffer instance, right now (Milestone 8 L4).

  The ring stops being a static `ring:` config list and starts being
  whichever nodes are actually alive and hosting this instance — tracked
  with `:pg` (Erlang/OTP's own distributed process-group registry, scoped
  under `Smolquery.Cluster.pg_scope/0`), which already does exactly what a
  dynamic ring needs: a node's membership disappears the moment its
  representative process exits, gracefully or not, with no separate
  heartbeat or gossip protocol to build here.

  `join/2`/`leave/2` are called with the hosting
  `Smolquery.BufferService.Supervisor`'s own pid — alive for exactly as long
  as this node's buffer subtree is, so a crash removes the node from the
  ring the same way an explicit `leave/2` (`Smolquery.BufferService.Drain`)
  does.

  Only meaningful when `Smolquery.Cluster.enabled?/0` — a single-node
  deployment has no Postgres to discover peers through, and `nodes/2` falls
  back to whatever static list the caller already had (today's `ring:`
  configuration, unchanged), so nothing here changes single-node behavior.
  """

  alias Smolquery.Cluster

  @doc """
  Joins `pid` to `name`'s group — this node now hosts `name`.

  A no-op when clustering is off.
  """
  @spec join(atom(), pid()) :: :ok
  def join(name, pid) do
    if Cluster.enabled?() do
      :ok = :pg.join(Cluster.pg_scope(), group(name), pid)
    end

    :ok
  end

  @doc """
  Removes `pid` from `name`'s group — the ring-exit step of a drain.

  A no-op when clustering is off.
  """
  @spec leave(atom(), pid() | nil) :: :ok
  def leave(_name, nil), do: :ok

  def leave(name, pid) do
    if Cluster.enabled?() do
      :pg.leave(Cluster.pg_scope(), group(name), pid)
    end

    :ok
  end

  @doc """
  Every node currently hosting `name`, or `static_fallback` when clustering
  is off (or no node has joined yet).
  """
  @spec nodes(atom(), [node()]) :: [node()]
  def nodes(name, static_fallback) do
    if Cluster.enabled?() do
      live(name, static_fallback)
    else
      static_fallback
    end
  end

  defp live(name, static_fallback) do
    case Cluster.pg_scope() |> :pg.get_members(group(name)) |> Enum.map(&node/1) |> Enum.uniq() do
      [] -> static_fallback
      nodes -> Enum.sort(nodes)
    end
  end

  defp group(name), do: {__MODULE__, name}
end
