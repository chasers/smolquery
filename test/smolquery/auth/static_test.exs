defmodule Smolquery.Auth.StaticTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Static

  describe "mode!/3" do
    test "accepts explicit static mode" do
      assert Static.mode!([auth_mode: :static], "the API", :api) == :static
    end

    test "rejects missing, invalid, and unsupported modes" do
      assert_raise ArgumentError, ~r/SMOLQUERY_AUTH_MODE/, fn ->
        Static.mode!([], "the API", :api)
      end

      assert_raise ArgumentError, ~r/invalid value/, fn ->
        Static.mode!([auth_mode: :other], "the API", :api)
      end

      assert_raise ArgumentError, ~r/not supported/, fn ->
        Static.mode!([auth_mode: :oidc], "the API", :api)
      end
    end
  end

  test "static contexts use stable non-secret identities and capabilities" do
    api = Static.api_context()
    web = Static.web_context()

    assert api.principal.authn == :api_key
    assert api.principal.kind == :service
    assert MapSet.equal?(api.capabilities, MapSet.new([:query, :ingest, :catalog_manage]))
    assert web.principal.authn == :basic
    assert web.principal.kind == :user

    assert MapSet.equal?(
             web.capabilities,
             MapSet.new([:web_access, :query, :catalog_manage, :platform_operate])
           )

    for secret <- ["api-service", "web-operator", "smolquery-dev", "operator-secret"] do
      refute api.principal.id =~ secret
      refute inspect(api) =~ secret
      refute web.principal.id =~ secret
      refute inspect(web) =~ secret
    end
  end
end
