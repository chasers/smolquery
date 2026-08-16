defmodule Smolquery.Auth.ContextTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Principal

  defp principal do
    {:ok, principal} = Principal.local("static:test", :api_key, :service)
    principal
  end

  defp oidc_principal do
    {:ok, principal} = Principal.oidc("https://idp.example", "user-1", :user)
    principal
  end

  test "uses an explicit single-tenant sentinel and optional expiry" do
    assert {:ok, context} =
             Context.single_tenant(principal(), [:query, :query, :ingest], expires_at: 100)

    assert context.scope == :single_tenant
    assert context.scope == Context.single_tenant_scope()
    assert context.capabilities == MapSet.new([:query, :ingest])
    assert context.expires_at == 100
    assert Context.well_formed?(context)
  end

  test "defaults expiry to nil and accepts one capability or a MapSet" do
    assert {:ok, one} = Context.single_tenant(principal(), :query)
    assert {:ok, many} = Context.single_tenant(principal(), MapSet.new([:query, :query]))

    assert one.expires_at == nil
    assert one.capabilities == MapSet.new([:query])
    assert many.capabilities == MapSet.new([:query])
    assert Context.granted?(one, :query)
    refute Context.granted?(one, :ingest)
  end

  test "requires expiry for OIDC contexts" do
    assert {:error, :oidc_requires_expiry} =
             Context.single_tenant(oidc_principal(), :query)

    assert {:error, :oidc_requires_expiry} =
             Context.single_tenant(oidc_principal(), :query, expires_at: nil)

    assert {:ok, context} =
             Context.single_tenant(oidc_principal(), :query, expires_at: 100)

    assert Context.well_formed?(context)
    refute Context.well_formed?(%{context | expires_at: nil})
    refute Context.active?(%{context | expires_at: nil}, 0)
  end

  test "identifies known capabilities without converting input" do
    assert Context.capability?(:query)
    refute Context.capability?(:admin)
    refute Context.capability?("query")

    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), [:query, :admin])
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), ["query"])
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), :admin)
    refute Context.granted?(nil, :admin)
  end

  test "rejects malformed expiry options" do
    assert {:error, {:invalid_option, :expires_at}} =
             Context.single_tenant(principal(), :query, expires_at: -1)

    assert {:error, {:invalid_option, :expires_at}} =
             Context.single_tenant(principal(), :query, expires_at: "later")

    assert {:error, :invalid_options} =
             Context.single_tenant(principal(), :query, %{expires_at: 1})
  end

  test "expires before and at the boundary" do
    assert {:ok, context} = Context.single_tenant(principal(), :query, expires_at: 100)

    assert Context.active?(context, 99)
    refute Context.active?(context, 100)
    refute Context.active?(context, 101)
    refute Context.active?(context, -1)
    refute Context.active?(context, "100")

    assert {:ok, never_expires} = Context.single_tenant(principal(), :query)
    assert Context.active?(never_expires, 0)
  end

  test "rejects malformed principal and expiry structure" do
    malformed = %Principal{id: "", authn: :api_key, kind: :service}

    assert {:error, :invalid_principal} = Context.single_tenant(malformed, :query)

    assert {:ok, context} = Context.single_tenant(principal(), :query)
    malformed_expiry = %{context | expires_at: -1}
    refute Context.well_formed?(malformed_expiry)
    refute Context.active?(malformed_expiry, 0)
  end

  test "structural checks do not grant from a forged context" do
    malformed = %Principal{id: "", authn: :api_key, kind: :service}

    context = %Context{
      principal: malformed,
      scope: :single_tenant,
      capabilities: MapSet.new([:query])
    }

    refute Context.well_formed?(context)
    refute Context.granted?(context, :query)
  end

  test "rejects malformed MapSet internals without raising" do
    non_map = %MapSet{map: :forged}
    invalid_entry = %MapSet{map: %{query: :forged}}

    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), non_map)
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), invalid_entry)

    for capabilities <- [non_map, invalid_entry] do
      context = %Context{
        principal: principal(),
        scope: :single_tenant,
        capabilities: capabilities
      }

      refute Context.well_formed?(context)
      refute Context.granted?(context, :query)
    end
  end
end
