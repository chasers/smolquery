defmodule Smolquery.Auth.Policy do
  @moduledoc """
  Capability authorization for authenticated contexts.

  Structural context checks prove shape only. Trusted authenticators and
  mappers construct contexts, and client input must not be decoded directly
  into structs or capabilities. Expiry is checked here using the system Unix
  epoch clock; authenticator clock-skew handling occurs before construction.

  The tagged errors preserve the boundary between a missing, malformed, or
  expired identity (`:unauthenticated`), an unknown requested capability
  (`:invalid_capability`), and an identified principal without a known
  requested capability (`:forbidden`).
  """

  alias Smolquery.Auth.Context

  @type authorization_error :: :unauthenticated | :invalid_capability | :forbidden

  @doc """
  Authorizes a capability for a context using the current Unix epoch time.
  """
  @spec authorize(term(), term()) :: :ok | {:error, authorization_error()}
  def authorize(context, capability),
    do: authorize(context, capability, System.system_time(:second))

  @doc """
  Authorizes a capability for a context at a supplied non-negative Unix epoch
  timestamp. This arity supports deterministic authorization tests.
  """
  @spec authorize(term(), term(), non_neg_integer()) ::
          :ok | {:error, authorization_error()}
  def authorize(nil, _capability, _now), do: {:error, :unauthenticated}

  def authorize(%Context{} = context, capability, now) do
    cond do
      not Context.well_formed?(context) -> {:error, :unauthenticated}
      not Context.active?(context, now) -> {:error, :unauthenticated}
      not Context.capability?(capability) -> {:error, :invalid_capability}
      Context.granted?(context, capability) -> :ok
      true -> {:error, :forbidden}
    end
  end

  def authorize(_context, _capability, _now), do: {:error, :unauthenticated}
end
