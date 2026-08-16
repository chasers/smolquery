defmodule Smolquery.Auth.ModeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Mode

  test "accepts explicit static and OIDC modes" do
    assert Mode.runtime_mode!([auth_mode: :static], "the API", :api) == :static
    assert Mode.runtime_mode!([auth_mode: :oidc], "the API", :api) == :oidc
  end

  test "fails closed for missing and invalid modes" do
    assert_raise ArgumentError, ~r/SMOLQUERY_AUTH_MODE/, fn ->
      Mode.runtime_mode!([], "the API", :api)
    end

    assert_raise ArgumentError, ~r/invalid value/, fn ->
      Mode.runtime_mode!([auth_mode: :unknown], "the API", :api)
    end
  end
end
