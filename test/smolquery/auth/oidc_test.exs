defmodule Smolquery.Auth.OIDCTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.OIDC

  test "composes a provider only for OIDC mode" do
    config = %{issuer: "https://issuer.example"}
    assert [{Smolquery.Auth.OIDC.Provider, child}] = OIDC.children(:oidc, config, :api)
    assert child[:config] == config
    assert child[:name] == Module.concat(:api, "OIDCProvider")
    assert OIDC.children(:static, nil, :api) == []
  end
end
