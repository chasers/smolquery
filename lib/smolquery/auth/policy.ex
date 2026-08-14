defmodule Smolquery.Auth.Policy do
  @moduledoc """
  Capability authorization for authenticated contexts.

  The tagged errors preserve the boundary between a missing identity (`401`)
  and an identified principal without the requested capability (`403`).
  """

  alias Smolquery.Auth.Context

  @type authorization_error :: :unauthenticated | :forbidden

  @doc """
  Authorizes a capability for a context.
  """
  @spec authorize(nil | Context.t(), term()) :: :ok | {:error, authorization_error()}
  def authorize(nil, _capability), do: {:error, :unauthenticated}

  def authorize(%Context{} = context, capability) do
    cond do
      not Context.valid?(context) -> {:error, :unauthenticated}
      Context.granted?(context, capability) -> :ok
      true -> {:error, :forbidden}
    end
  end

  def authorize(_context, _capability), do: {:error, :unauthenticated}
end
