defmodule Smolquery.Application do
  @moduledoc """
  Boots the service subtrees this node's roles enable.

  Each role contributes an independent top-level subtree, so a service's crash
  domain is its own and a node runs only what it was given. `SMOLQUERY_ROLES`
  selects them; see `Smolquery.Roles`.

  Order within a node does not encode a dependency: services reach each other
  through client modules that tolerate a peer being absent, so a node starting
  `:storage` before `:buffer` is not a problem for either.
  """

  use Application

  alias Smolquery.Roles

  @impl true
  def start(_type, _args) do
    Smolquery.InternalSecret.ensure()

    children =
      [Smolquery.Telemetry, Smolquery.Cluster.RingCache] ++
        Smolquery.Cluster.children() ++ Enum.flat_map(Roles.enabled(), &subtree/1)

    Supervisor.start_link(children, strategy: :one_for_one, name: Smolquery.Supervisor)
  end

  defp subtree(:api), do: [SmolqueryApi.Supervisor]
  defp subtree(:web), do: [SmolqueryWeb.Supervisor]
  defp subtree(:query), do: [Smolquery.QueryService.Supervisor]
  defp subtree(:buffer), do: [Smolquery.BufferService.Supervisor]
  defp subtree(:storage), do: [Smolquery.StorageService.Supervisor]
  defp subtree(:ingest), do: [Smolquery.IngestService.Supervisor]
end
