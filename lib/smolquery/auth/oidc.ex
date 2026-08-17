defmodule Smolquery.Auth.OIDC do
  @moduledoc """
  OIDC role subtree composition.

  The provider cache is deliberately started only for an explicitly selected
  OIDC role. Static mode has no provider process and no OIDC network traffic.
  """

  alias Smolquery.Auth.OIDC.{Config, Discovery, Provider}

  @doc "Returns the provider child specification for a role runtime."
  @spec children(:static | :oidc, Config.t() | nil, atom(), Discovery.http_client() | nil) ::
          [{module(), keyword()}]
  def children(mode, config, name, http_client \\ nil)

  def children(:oidc, config, name, http_client) do
    opts = [config: config, name: Module.concat(name, "OIDCProvider")]
    opts = if is_nil(http_client), do: opts, else: Keyword.put(opts, :http_client, http_client)
    [{Provider, opts}]
  end

  def children(:static, nil, _name, nil), do: []

  @doc false
  @spec provider_http_client!(keyword()) :: Discovery.http_client() | nil
  def provider_http_client!(config) do
    case Keyword.get(config, :oidc_provider_http_client) do
      nil -> nil
      client when is_function(client, 2) -> client
      value -> raise ArgumentError, "invalid OIDC provider HTTP client: #{inspect(value)}"
    end
  end
end
