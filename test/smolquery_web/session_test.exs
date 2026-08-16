defmodule SmolqueryWeb.SessionTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.{Context, Principal}
  alias SmolqueryWeb.Session

  test "round trips only normalized identity and capabilities" do
    {:ok, principal} =
      Principal.oidc("https://issuer.example/", "subject", :user, display_name: "Ada")

    {:ok, context} =
      Context.single_tenant(principal, [:web_access, :query],
        expires_at: System.system_time(:second) + 60
      )

    assert {:ok, encoded} = Session.encode(context)

    assert encoded["iss"] == "https://issuer.example/"
    assert encoded["sub"] == "subject"
    refute Map.has_key?(encoded, "access_token")
    assert {:ok, decoded} = Session.decode(encoded)
    assert decoded.principal.id == principal.id
    assert MapSet.equal?(decoded.capabilities, MapSet.new([:web_access, :query]))
  end

  test "rejects identities that cannot fit safely in the encrypted cookie" do
    {:ok, principal} =
      Principal.oidc("https://issuer.example/", String.duplicate("s", 513), :user)

    {:ok, context} =
      Context.single_tenant(principal, [:web_access],
        expires_at: System.system_time(:second) + 60
      )

    assert :error = Session.encode(context)
  end

  test "rejects expired, malformed, and unknown capability sessions" do
    {:ok, principal} = Principal.oidc("https://issuer.example/", "subject", :user)

    {:ok, context} =
      Context.single_tenant(principal, [:web_access], expires_at: System.system_time(:second) - 1)

    assert {:ok, encoded} = Session.encode(context)
    assert :error = Session.decode(encoded)
    assert :error = Session.decode(%{"v" => 1, "iss" => "https://issuer.example/"})

    valid = %{
      "v" => 1,
      "iss" => "https://issuer.example/",
      "sub" => "subject",
      "capabilities" => ["web_access", "not-a-capability"],
      "exp" => System.system_time(:second) + 60
    }

    assert :error = Session.decode(valid)
  end
end
