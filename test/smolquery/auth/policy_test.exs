defmodule Smolquery.Auth.PolicyTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Policy
  alias Smolquery.Auth.Principal

  defp context(capabilities) do
    {:ok, principal} = Principal.local("static:test", :api_key, :service)
    {:ok, context} = Context.single_tenant(principal, capabilities)
    context
  end

  test "grants a capability held by the context" do
    assert :ok = Policy.authorize(context(:query), :query)
  end

  test "forbids a known capability the context does not hold" do
    assert {:error, :forbidden} = Policy.authorize(context(:query), :ingest)
    assert {:error, :forbidden} = Policy.authorize(context(:query), :unknown)
  end

  test "reports a missing or invalid context as unauthenticated" do
    assert {:error, :unauthenticated} = Policy.authorize(nil, :query)
    assert {:error, :unauthenticated} = Policy.authorize(:not_a_context, :query)

    malformed = %Context{
      principal: %Principal{id: "", authn: :api_key, kind: :service},
      scope: :single_tenant,
      capabilities: MapSet.new([:query])
    }

    assert {:error, :unauthenticated} = Policy.authorize(malformed, :query)

    assert {:error, :unauthenticated} =
             Policy.authorize(%{malformed | capabilities: %MapSet{map: :forged}}, :query)
  end
end
