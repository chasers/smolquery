defmodule Smolquery.Auth.Static do
  @moduledoc """
  Trusted adapters for the static authentication mode.

  Static credentials are verified by their transport-specific adapters. This
  module only supplies the normalized identities and capabilities after that
  verification succeeds. Source keys are constructor-owned labels, never
  credential material.
  """

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Principal

  @api_source "api-service"
  @web_source "web-operator"
  @api_capabilities [:query, :ingest, :catalog_manage]
  @web_capabilities [:web_access, :query, :catalog_manage, :platform_operate]

  @doc """
  Resolves the explicit authentication mode from application configuration.

  OIDC is rejected until its runtime support is available; it never falls
  through to static authentication.
  """
  @spec mode!(keyword(), String.t(), atom()) :: :static
  def mode!(config, service, role) do
    case Keyword.get(config, :auth_mode) do
      :static -> :static
      :oidc -> unsupported!(service, role)
      nil -> missing!(service, role)
      mode -> invalid!(mode, service, role)
    end
  end

  @doc """
  Builds the stable service context used by an authenticated API key.
  """
  @spec api_context() :: Context.t()
  def api_context do
    {:ok, principal} = Principal.local(@api_source, :api_key, :service)
    {:ok, context} = Context.single_tenant(principal, @api_capabilities)
    context
  end

  @doc """
  Builds the stable operator context used by authenticated web Basic auth.
  """
  @spec web_context() :: Context.t()
  def web_context do
    {:ok, principal} = Principal.local(@web_source, :basic, :user)
    {:ok, context} = Context.single_tenant(principal, @web_capabilities)
    context
  end

  defp missing!(service, role) do
    raise ArgumentError,
          "#{service} refuses to boot without an authentication mode: set " <>
            "SMOLQUERY_AUTH_MODE to static (or oidc when supported) on every node " <>
            "running the #{inspect(role)} role"
  end

  defp invalid!(mode, service, role) do
    raise ArgumentError,
          "SMOLQUERY_AUTH_MODE has invalid value #{inspect(mode)} for #{service}; " <>
            "expected static or oidc on the #{inspect(role)} role"
  end

  defp unsupported!(service, role) do
    raise ArgumentError,
          "#{service} cannot start in oidc authentication mode yet; " <>
            "SMOLQUERY_AUTH_MODE=oidc is not supported for the #{inspect(role)} role"
  end
end
