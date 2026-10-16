defmodule Smolquery.Auth.Context do
  @moduledoc """
  The authenticated principal, capabilities, scope, and expiry contract for
  one request.

  `:single_tenant` is an explicit scope sentinel for the current deployment.
  It is intentionally not derived from an issuer, email domain, provider
  group, or other identity-provider claim; a future tenant membership lookup
  can replace it without changing the principal contract.

  `expires_at` is a Unix epoch timestamp in integer seconds. It is required
  for OIDC principals and optional for local static principals. A context is
  active only while `now < expires_at`; the context is expired at exactly the
  boundary. Authenticator clock-skew handling occurs before this context is
  constructed.

  Structural checks prove shape only, not authentication provenance. Only
  trusted authenticators and mappers may construct contexts; client input must
  not be decoded directly into structs or capabilities. Authentication
  provenance is established before construction; policy checks expiry and
  capability access.
  """

  alias Smolquery.Auth.Principal

  @capabilities [:web_access, :query, :ingest, :catalog_manage, :platform_operate]
  @single_tenant :single_tenant
  @enforce_keys [:principal, :scope, :capabilities]
  defstruct [:principal, :scope, :capabilities, :expires_at]

  @type capability :: :web_access | :query | :ingest | :catalog_manage | :platform_operate
  @type scope :: :single_tenant
  @type t :: %__MODULE__{
          principal: Principal.t(),
          scope: scope(),
          capabilities: MapSet.t(capability()),
          expires_at: non_neg_integer() | nil
        }

  @doc """
  Builds a context with the explicit single-tenant scope.

  The `:expires_at` option is an integer Unix epoch timestamp in seconds. It
  is required for OIDC principals and defaults to `nil` for local principals.
  """
  @spec single_tenant(Principal.t(), capability() | [capability()] | MapSet.t()) ::
          {:ok, t()} | {:error, term()}
  @spec single_tenant(Principal.t(), capability() | [capability()] | MapSet.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def single_tenant(principal, capabilities, opts \\ [])

  def single_tenant(%Principal{} = principal, capabilities, opts) do
    if Principal.well_formed?(principal) do
      with {:ok, capabilities} <- normalize_capabilities(capabilities),
           {:ok, expires_at} <- options(opts),
           :ok <- validate_context_expiry(principal, expires_at) do
        {:ok,
         %__MODULE__{
           principal: principal,
           scope: @single_tenant,
           capabilities: capabilities,
           expires_at: expires_at
         }}
      end
    else
      {:error, :invalid_principal}
    end
  end

  def single_tenant(_principal, _capabilities, _opts), do: {:error, :invalid_principal}

  @doc """
  Reports whether a well-formed context grants a known capability.
  """
  @spec granted?(t(), term()) :: boolean()
  def granted?(%__MODULE__{} = context, capability) when capability in @capabilities do
    well_formed?(context) and MapSet.member?(context.capabilities, capability)
  end

  def granted?(_context, _capability), do: false

  @doc """
  Reports whether a capability belongs to the closed capability contract.
  """
  @spec capability?(term()) :: boolean()
  def capability?(capability), do: capability in @capabilities

  @doc """
  Reports whether a term has the structure of a context produced by this
  module. This does not prove authentication provenance.
  """
  @spec well_formed?(term()) :: boolean()
  def well_formed?(%__MODULE__{
        principal: principal,
        scope: @single_tenant,
        capabilities: capabilities,
        expires_at: expires_at
      }) do
    Principal.well_formed?(principal) and valid_capability_set?(capabilities) and
      validate_context_expiry(principal, expires_at) == :ok
  end

  def well_formed?(_term), do: false

  @doc """
  Reports whether a context is active at a non-negative Unix epoch timestamp.

  An expiry is strict: a context is inactive when `now == expires_at`.
  """
  @spec active?(term(), non_neg_integer()) :: boolean()
  def active?(%__MODULE__{expires_at: expires_at} = context, now)
      when is_integer(now) and now >= 0 do
    well_formed?(context) and (is_nil(expires_at) or now < expires_at)
  end

  def active?(_context, _now), do: false

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
    if valid_mapset_map?(map) do
      normalize_capabilities(MapSet.to_list(capabilities))
    else
      {:error, :invalid_capabilities}
    end
  end

  defp normalize_capabilities(capabilities) when is_list(capabilities) do
    if Enum.all?(capabilities, &(&1 in @capabilities)) do
      {:ok, MapSet.new(capabilities)}
    else
      {:error, :invalid_capabilities}
    end
  end

  defp normalize_capabilities(_capabilities), do: {:error, :invalid_capabilities}

  defp valid_capability_set?(%MapSet{map: map}) when is_map(map),
    do: valid_mapset_map?(map)

  defp valid_capability_set?(_capabilities), do: false

  defp valid_mapset_map?(map) do
    Enum.all?(map, fn {capability, value} -> value == [] and capability in @capabilities end)
  end

  defp options(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) -> {:error, :invalid_options}
      duplicate_keys?(opts) -> {:error, :invalid_options}
      true -> Enum.reduce_while(opts, {:ok, nil}, &reduce_option/2)
    end
  end

  defp options(_opts), do: {:error, :invalid_options}

  defp reduce_option({:expires_at, value}, _acc) do
    if valid_expiry?(value),
      do: {:cont, {:ok, value}},
      else: {:halt, {:error, {:invalid_option, :expires_at}}}
  end

  defp reduce_option({key, _value}, _acc), do: {:halt, {:error, {:unknown_option, key}}}

  defp duplicate_keys?(opts) do
    keys = Keyword.keys(opts)
    length(keys) != length(Enum.uniq(keys))
  end

  defp validate_context_expiry(%Principal{authn: :oidc}, nil),
    do: {:error, :oidc_requires_expiry}

  defp validate_context_expiry(_principal, expires_at) do
    if valid_expiry?(expires_at),
      do: :ok,
      else: {:error, {:invalid_option, :expires_at}}
  end

  defp valid_expiry?(nil), do: true
  defp valid_expiry?(value), do: is_integer(value) and value >= 0
end
