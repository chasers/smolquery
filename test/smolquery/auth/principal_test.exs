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
      assert Principal.well_formed?(first)
    end

    test "distinguishes exact issuer and subject pairs" do
      assert {:ok, first} = Principal.oidc("https://one.example", "same", :user)
      assert {:ok, second} = Principal.oidc("https://two.example", "same", :user)
      assert {:ok, pairwise} = Principal.oidc("https://one.example", "same-client-subject", :user)

      refute first.id == second.id
      refute first.id == pairwise.id
    end

    test "keeps issuer and subject boundaries distinct" do
      assert {:ok, first} = Principal.oidc("ab", "c", :user)
      assert {:ok, second} = Principal.oidc("a", "bc", :user)

      refute first.id == second.id
    end

    test "does not let display metadata affect identity" do
      assert {:ok, without_metadata} = Principal.oidc("issuer", "subject", :user)

      assert {:ok, with_metadata} =
               Principal.oidc("issuer", "subject", :user,
                 display_name: "Alice",
                 client_id: "web"
               )

      assert with_metadata.id == without_metadata.id
      assert with_metadata.display_name == "Alice"
      assert with_metadata.client_id == "web"
    end

    test "requires exact non-empty issuer and subject strings" do
      assert {:error, {:invalid, :issuer}} = Principal.oidc("", "subject", :user)
      assert {:error, {:invalid, :issuer}} = Principal.oidc(:issuer, "subject", :user)
      assert {:error, {:invalid, :subject}} = Principal.oidc("issuer", "", :user)
      assert {:error, {:invalid, :subject}} = Principal.oidc("issuer", :subject, :user)
    end
  end

  describe "local/4" do
    test "derives stable authn-specific opaque IDs without retaining the source key" do
      assert {:ok, first} = Principal.local("stable-source", :api_key, :service)
      assert {:ok, second} = Principal.local("stable-source", :api_key, :service)
      assert {:ok, basic} = Principal.local("stable-source", :basic, :service)

      assert first.id == second.id
      assert first.id == "api_key:v1:YYMKF704-BVc8TMLshzdSjww0wilh6UT20EHWm27Q20"
      assert basic.id == "basic:v1:WwzWc2-Vju_Dsy7TNFUrFyXBToNxLan-lXElQkwzypA"
      assert first.authn == :api_key
      assert first.issuer == nil
      assert first.subject == nil
      refute first.id =~ "stable-source"
      refute Map.has_key?(first, :source_key)
      assert Principal.well_formed?(first)
    end

    test "separates local authn namespaces and OIDC" do
      assert {:ok, api_key} = Principal.local("same-source", :api_key, :service)
      assert {:ok, basic} = Principal.local("same-source", :basic, :service)
      assert {:ok, oidc} = Principal.oidc("same-source", "same-source", :service)

      refute api_key.id == basic.id
      refute api_key.id == oidc.id
      refute basic.id == oidc.id
    end

    test "rejects non-canonical or repeated local ID encodings" do
      assert {:ok, principal} = Principal.local("stable-source", :api_key, :service)
      prefix = "api_key:v1:"
      body_size = byte_size(principal.id) - 1
      <<body::binary-size(^body_size), _last>> = principal.id

      refute Principal.well_formed?(%{principal | id: body <> "1"})
      refute Principal.well_formed?(%{principal | id: prefix <> principal.id})
    end

    test "does not allow local OIDC identities or empty source keys" do
      assert {:error, :oidc_requires_oidc_constructor} =
               Principal.local("id", :oidc, :user)

      assert {:error, {:invalid, :source_key}} = Principal.local("", :api_key, :service)
    end
  end

  describe "validation" do
    test "rejects invalid kinds, options, and metadata" do
      assert {:error, :invalid_kind} = Principal.oidc("issuer", "subject", :admin)

      assert {:error, {:unknown_option, :groups}} =
               Principal.oidc("issuer", "subject", :user, groups: ["admins"])

      assert {:error, {:invalid_option, :display_name}} =
               Principal.local("id", :api_key, :service, display_name: "")

      assert {:error, :invalid_options} =
               Principal.local("id", :api_key, :service, %{display_name: "name"})

      assert {:error, :invalid_options} =
               Principal.local("id", :api_key, :service, display_name: "one", display_name: "two")
    end

    test "rejects invalid authentication values and malformed principals" do
      assert {:error, :invalid_authn} = Principal.local("id", :password, :service)
      assert {:error, :invalid_kind} = Principal.local("id", :api_key, :admin)

      refute Principal.well_formed?(%Principal{
               id: "id",
               authn: :api_key,
               kind: :service,
               issuer: "unexpected"
             })

      refute Principal.well_formed?(%{__struct__: Principal})
      refute Principal.well_formed?(%{__struct__: Principal, id: "partial"})

      assert {:ok, oidc} = Principal.oidc("issuer", "subject", :user)
      refute Principal.well_formed?(%{oidc | id: "oidc:v1:forged"})
    end
  end
end
