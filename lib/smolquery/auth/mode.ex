defmodule Smolquery.Auth.Mode do
  @moduledoc """
  Shared authentication-mode resolution for role runtimes.
  """

  @type t :: :static | :oidc

  @spec runtime_mode!(keyword(), String.t(), atom()) :: t()
  def runtime_mode!(config, service, role) do
    case Keyword.get(config, :auth_mode) do
      :static ->
        :static

      :oidc ->
        :oidc

      nil ->
        raise ArgumentError,
              "#{service} refuses to boot without an authentication mode: set " <>
                "SMOLQUERY_AUTH_MODE to static (or oidc when supported) on every node " <>
                "running the #{inspect(role)} role"

      mode ->
        raise ArgumentError,
              "SMOLQUERY_AUTH_MODE has invalid value #{inspect(mode)} for #{service}; " <>
                "expected static or oidc on the #{inspect(role)} role"
    end
  end
end
