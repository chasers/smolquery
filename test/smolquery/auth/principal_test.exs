defmodule Smolquery.Auth.PrincipalTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.Principal

  describe "oidc/4" do
    test "derives a stable opaque identity from issuer and subject" do
      assert {:ok, first} = Principal.oidc("https://idp.example", "user-1", :user)
      assert {:ok, second} = Principal.oidc("https://idp.example", "user-1", :user)

      assert first.id == second.id
      assert first.id == "oidc:v1:D69mFP3Atlfs9LemldrEtUY3onuuPpAvhUyuPWsUYsw"
      assert first.id != "https://idp.example"
      assert first.id != "user-1"
      assert first.issuer == "https://idp.example"
      assert first.subject == "user-1"
      assert Principal.valid?(first)
    end

    test "distinguishes the same subject at different issuers" do
      assert {:ok, first} = Principal.oidc("https://one.example", "same", :user)
      assert {:ok, second} = Principal.oidc("https://two.example", "same", :user)

      refute first.id == second.id
    end

    test "keeps issuer and subject boundaries distinct" do
      assert {:ok, first} = Principal.oidc("a\0b", "c", :user)
      assert {:ok, second} = Principal.oidc("a", "b\0c", :user)

      refute first.id == second.id
    end

    test "does not let display metadata affect identity" do
      assert {:ok, without_metadata} = Principal.oidc("issuer", "subject", :user)

      assert {:ok, with_metadata} =
               Principal.oidc("issuer", "subject", :user,
                 display_name: "Alice",
                 client_id: "web",
                 expires_at: 42
               )

      assert with_metadata.id == without_metadata.id
      assert with_metadata.display_name == "Alice"
      assert with_metadata.client_id == "web"
      assert with_metadata.expires_at == 42
    end

    test "requires exact non-empty issuer and subject strings" do
      assert {:error, {:invalid, :issuer}} = Principal.oidc("", "subject", :user)
      assert {:error, {:invalid, :issuer}} = Principal.oidc(:issuer, "subject", :user)
      assert {:error, {:invalid, :subject}} = Principal.oidc("issuer", "", :user)
      assert {:error, {:invalid, :subject}} = Principal.oidc("issuer", :subject, :user)
    end
  end

  describe "local/4" do
    test "builds local API-key and Basic principals" do
      assert {:ok, api_key} = Principal.local("static:api", :api_key, :service)
      assert {:ok, basic} = Principal.local("static:web", :basic, :user)

      assert api_key.authn == :api_key
      assert basic.authn == :basic
      assert api_key.issuer == nil
      assert api_key.subject == nil
      assert Principal.valid?(api_key)
      assert Principal.valid?(basic)
    end

    test "does not allow local OIDC identities" do
      assert {:error, :oidc_requires_oidc_constructor} =
               Principal.local("id", :oidc, :user)
    end
  end

  describe "validation" do
    test "rejects invalid kinds, options, metadata, and expiry" do
      assert {:error, :invalid_kind} = Principal.oidc("issuer", "subject", :admin)

      assert {:error, {:unknown_option, :groups}} =
               Principal.oidc("issuer", "subject", :user, groups: ["admins"])

      assert {:error, {:invalid_option, :display_name}} =
               Principal.local("id", :api_key, :service, display_name: "")

      assert {:error, {:invalid_option, :expires_at}} =
               Principal.local("id", :api_key, :service, expires_at: -1)

      assert {:error, :invalid_options} =
               Principal.local("id", :api_key, :service, %{expires_at: 1})

      assert {:error, :invalid_options} =
               Principal.local("id", :api_key, :service, expires_at: 1, expires_at: 2)
    end

    test "rejects invalid authentication values and malformed principals" do
      assert {:error, :invalid_authn} = Principal.local("id", :password, :service)
      assert {:error, :invalid_kind} = Principal.local("id", :api_key, :admin)

      refute Principal.valid?(%Principal{
               id: "id",
               authn: :api_key,
               kind: :service,
               issuer: "unexpected"
             })

      assert {:ok, oidc} = Principal.oidc("issuer", "subject", :user)
      refute Principal.valid?(%{oidc | id: "oidc:v1:forged"})
    end
  end
end
