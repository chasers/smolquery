defmodule Smolquery.Auth.OIDC do
  @moduledoc """
  OIDC role subtree composition.

  The provider cache is deliberately started only for an explicitly selected
  OIDC role. Static mode has no provider process and no OIDC network traffic.
  """

  alias Smolquery.Auth.OIDC.{Config, Provider}

  @doc "Returns the provider child specification for a role runtime."
  @spec children(:static | :oidc, Config.t() | nil, atom()) :: [{module(), keyword()}]
  def children(:oidc, config, name) do
    [{Provider, config: config, name: Module.concat(name, "OIDCProvider")}]
  end

  def children(:static, nil, _name), do: []
end
