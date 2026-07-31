defmodule Smolquery.Application do
  @moduledoc """
  Boots the service subtrees this node's roles enable.

  Each role contributes an independent top-level subtree, so a service's crash
  domain is its own and a node runs only what it was given. `SMOLQUERY_ROLES`
  selects them; see `Smolquery.Roles`.

  Roles whose services are not implemented yet contribute nothing — the role is
  accepted, and its subtree appears when its milestone lands.
  """

  use Application

  alias Smolquery.Roles

  @impl true
  def start(_type, _args) do
    children = Enum.flat_map(Roles.enabled(), &subtree/1)

    Supervisor.start_link(children, strategy: :one_for_one, name: Smolquery.Supervisor)
  end

  defp subtree(:query), do: [Smolquery.QueryService.Supervisor]
  defp subtree(:ingest), do: []
  defp subtree(:buffer), do: []
  defp subtree(:storage), do: []
end
