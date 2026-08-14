defmodule Smolquery.Auth.ContextTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Principal

  defp principal do
    {:ok, principal} = Principal.local("static:test", :api_key, :service)
    principal
  end

  test "uses an explicit single-tenant sentinel" do
    assert {:ok, context} = Context.single_tenant(principal(), [:query, :query, :ingest])

    assert context.scope == :single_tenant
    assert context.scope == Context.single_tenant_scope()
    assert context.capabilities == MapSet.new([:query, :ingest])
    assert Context.valid?(context)
  end

  test "accepts one capability or a MapSet and deduplicates" do
    assert {:ok, one} = Context.single_tenant(principal(), :query)
    assert {:ok, many} = Context.single_tenant(principal(), MapSet.new([:query, :query]))

    assert one.capabilities == MapSet.new([:query])
    assert many.capabilities == MapSet.new([:query])
    assert Context.granted?(one, :query)
    refute Context.granted?(one, :ingest)
  end

  test "rejects unknown capabilities without converting input" do
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), [:query, :admin])
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), ["query"])
    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), :admin)
    refute Context.granted?(nil, :admin)
  end

  test "rejects a malformed principal" do
    malformed = %Principal{id: "", authn: :api_key, kind: :service}

    assert {:error, :invalid_principal} = Context.single_tenant(malformed, :query)
  end

  test "does not grant from a forged context" do
    malformed = %Principal{id: "", authn: :api_key, kind: :service}

    context = %Context{
      principal: malformed,
      scope: :single_tenant,
      capabilities: MapSet.new([:query])
    }

    refute Context.valid?(context)
    refute Context.granted?(context, :query)
  end

  test "rejects a malformed MapSet without raising" do
    capabilities = %MapSet{map: :forged}

    assert {:error, :invalid_capabilities} = Context.single_tenant(principal(), capabilities)

    context = %Context{
      principal: principal(),
      scope: :single_tenant,
      capabilities: capabilities
    }

    refute Context.valid?(context)
    refute Context.granted?(context, :query)
  end
end
