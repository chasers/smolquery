defmodule Smolquery.Auth.PolicyTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Policy
  alias Smolquery.Auth.Principal

  defp context(capabilities, opts \\ []) do
    {:ok, principal} = Principal.local("static:test", :api_key, :service)
    {:ok, context} = Context.single_tenant(principal, capabilities, opts)
    context
  end

  test "grants a capability held by an active context" do
    assert :ok = Policy.authorize(context(:query), :query, 100)
  end

  test "forbids a known capability the context does not hold" do
    assert {:error, :forbidden} = Policy.authorize(context(:query), :ingest, 100)
  end

  test "rejects an unknown capability after structural and expiry checks" do
    assert {:error, :invalid_capability} = Policy.authorize(context(:query), :unknown, 100)

    assert {:error, :unauthenticated} =
             Policy.authorize(context(:query, expires_at: 100), :unknown, 100)
  end

  test "fails closed for an OIDC context without expiry" do
    assert {:ok, principal} = Principal.oidc("https://idp.example", "user-1", :user)

    context = %Context{
      principal: principal,
      scope: :single_tenant,
      capabilities: MapSet.new([:platform_operate])
    }

    assert {:error, :unauthenticated} =
             Policy.authorize(context, :platform_operate, 4_102_444_800)
  end

  test "reports a missing, malformed, or expired context as unauthenticated" do
    assert {:error, :unauthenticated} = Policy.authorize(nil, :query, 100)
    assert {:error, :unauthenticated} = Policy.authorize(:not_a_context, :query, 100)

    malformed = %Context{
      principal: %Principal{id: "", authn: :api_key, kind: :service},
      scope: :single_tenant,
      capabilities: MapSet.new([:query])
    }

    assert {:error, :unauthenticated} = Policy.authorize(malformed, :query, 100)

    assert {:error, :unauthenticated} =
             Policy.authorize(%{malformed | capabilities: %MapSet{map: :forged}}, :query, 100)

    assert {:error, :unauthenticated} =
             Policy.authorize(context(:query, expires_at: 100), :query, 100)
  end
end
