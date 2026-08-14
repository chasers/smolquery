defmodule Smolquery.Auth.Context do
  @moduledoc """
  The authenticated principal and capabilities for one request.

  `:single_tenant` is an explicit scope sentinel for the current deployment.
  It is intentionally not derived from an issuer, email domain, provider group,
  or other identity-provider claim; a future tenant membership lookup can
  replace it without changing the principal contract.
  """

  alias Smolquery.Auth.Principal

  @capabilities [:web_access, :query, :ingest, :catalog_manage, :platform_operate]
  @single_tenant :single_tenant

  @enforce_keys [:principal, :scope, :capabilities]
  defstruct [:principal, :scope, :capabilities]

  @type capability :: :web_access | :query | :ingest | :catalog_manage | :platform_operate
  @type scope :: :single_tenant
  @type t :: %__MODULE__{
          principal: Principal.t(),
          scope: scope(),
          capabilities: MapSet.t(capability())
        }

  @doc """
  Builds a context with the explicit single-tenant scope.
  """
  @spec single_tenant(Principal.t(), capability() | [capability()] | MapSet.t()) ::
          {:ok, t()} | {:error, term()}
  def single_tenant(%Principal{} = principal, capabilities) do
    if Principal.valid?(principal) do
      with {:ok, capabilities} <- normalize_capabilities(capabilities) do
        {:ok,
         %__MODULE__{principal: principal, scope: @single_tenant, capabilities: capabilities}}
      end
    else
      {:error, :invalid_principal}
    end
  end

  def single_tenant(_principal, _capabilities), do: {:error, :invalid_principal}

  @doc """
  Reports whether a context grants a known capability.

  Unknown capability checks return `false` rather than converting input into an
  atom or granting access by accident.
  """
  @spec granted?(t(), term()) :: boolean()
  def granted?(%__MODULE__{} = context, capability) when capability in @capabilities do
    valid?(context) and MapSet.member?(context.capabilities, capability)
  end

  def granted?(_context, _capability), do: false

  @doc """
  Reports whether a term is a valid context produced by this module.
  """
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{principal: principal, scope: @single_tenant, capabilities: capabilities}) do
    Principal.valid?(principal) and valid_capability_set?(capabilities)
  end

  def valid?(_term), do: false

  @doc """
  Returns the closed set of capabilities accepted by the context contract.
  """
  @spec capabilities() :: [capability()]
  def capabilities, do: @capabilities

  @doc """
  Returns the explicit single-tenant scope sentinel.
  """
  @spec single_tenant_scope() :: scope()
  def single_tenant_scope, do: @single_tenant

  defp normalize_capabilities(capability) when capability in @capabilities do
    {:ok, MapSet.new([capability])}
  end

  defp normalize_capabilities(%MapSet{map: map} = capabilities) when is_map(map) do
    normalize_capabilities(MapSet.to_list(capabilities))
  end

  defp normalize_capabilities(capabilities) when is_list(capabilities) do
    if Enum.all?(capabilities, &(&1 in @capabilities)) do
      {:ok, MapSet.new(capabilities)}
    else
      {:error, :invalid_capabilities}
    end
  end

  defp normalize_capabilities(_capabilities), do: {:error, :invalid_capabilities}

  defp valid_capability_set?(%MapSet{map: map} = capabilities) when is_map(map) do
    capabilities
    |> MapSet.to_list()
    |> Enum.all?(&(&1 in @capabilities))
  end

  defp valid_capability_set?(_capabilities), do: false
end
