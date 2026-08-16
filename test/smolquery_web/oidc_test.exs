defmodule SmolqueryWeb.OIDCTest do
  use ExUnit.Case, async: false

  alias Smolquery.Auth.OIDC.Provider
  alias Smolquery.Test.MapCatalog
  alias SmolqueryWeb.{OIDC, Runtime}

  @private_key JOSE.JWK.generate_key({:rsa, 2048})
  @public_key JOSE.JWK.to_map(JOSE.JWK.to_public(@private_key))
              |> elem(1)
              |> Map.put("kid", "web-key")
  @jwks %{"keys" => [@public_key]}
  @metadata %{
    "issuer" => "https://issuer.example",
    "authorization_endpoint" => "https://issuer.example/authorize",
    "token_endpoint" => "https://issuer.example/token",
    "jwks_uri" => "https://issuer.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }
  @capabilities [:web_access, :query, :ingest, :catalog_manage, :platform_operate]

  setup do
    instance = Module.concat(__MODULE__, "Instance#{System.unique_integer([:positive])}")

    runtime =
      Runtime.new(
        name: instance,
        catalog: MapCatalog.new(),
        auth_mode: :oidc,
        secret_key_base: String.duplicate("s", 64),
        oidc: [
          issuer: "https://issuer.example",
          web_client_id: "web-client",
          web_client_auth_method: :none,
          web_origin: "https://ui.example",
          web_redirect_uri: "https://ui.example/auth/callback",
          claim_capabilities: %{"roles" => %{"operator" => @capabilities}}
        ]
      )

    provider = Module.concat(instance, "OIDCProvider")

    start_supervised!(
      {Provider,
       name: provider,
       config: runtime.oidc,
       http_client: fn url, _options ->
         case url do
           "https://issuer.example/.well-known/openid-configuration" -> response(@metadata)
           "https://issuer.example/keys" -> response(@jwks)
         end
       end}
    )

    {:ok, token_response} = Agent.start_link(fn -> nil end)
    test = self()

    http_client = fn url, options ->
      send(test, {:token_request, url, options})
      Agent.get(token_response, & &1)
    end

    %{runtime: runtime, token_response: token_response, http_client: http_client}
  end

  test "builds an explicit URL-safe state, nonce, and S256 PKCE authorization request", %{
    runtime: runtime
  } do
    assert {:ok, url, transaction} = OIDC.begin(runtime)
    uri = URI.parse(url)
    params = URI.decode_query(uri.query)

    assert uri.scheme == "https"
    assert uri.host == "issuer.example"
    assert uri.path == "/authorize"
    assert params["client_id"] == "web-client"
    assert params["redirect_uri"] == "https://ui.example/auth/callback"
    assert params["response_type"] == "code"
    assert params["scope"] == "openid"
    assert params["state"] == transaction.state
    assert params["nonce"] == transaction.nonce
    assert params["code_challenge_method"] == "S256"
    assert transaction.state =~ ~r/\A[A-Za-z0-9_-]+\z/
    assert transaction.nonce =~ ~r/\A[A-Za-z0-9_-]+\z/

    expected_challenge =
      transaction.verifier
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    assert params["code_challenge"] == expected_challenge
  end

  test "rejects expired, future-dated, and malformed transactions before exchange", %{
    runtime: runtime
  } do
    assert {:ok, _url, transaction} = OIDC.begin(runtime, now: 1_000)

    assert {:error, :invalid_transaction} =
             OIDC.consume(transaction, transaction.state, now: 1_301)

    assert {:error, :invalid_transaction} =
             OIDC.consume(transaction, transaction.state, now: 999)

    assert {:error, :invalid_transaction} = OIDC.consume(%{}, transaction.state)
  end

  test "stores a bounded set and consumes only the matching transaction" do
    transactions =
      Enum.map(1..5, fn index ->
        %{
          state: "state-#{index}",
          nonce: "nonce-#{index}",
          verifier: String.duplicate(Integer.to_string(index), 43),
          created_at: 1_000
        }
      end)

    cookie =
      Enum.reduce(transactions, nil, fn transaction, cookie ->
        assert {:ok, next_cookie} = OIDC.add_transaction(cookie, transaction, now: 1_000)
        next_cookie
      end)

    assert {:error, :invalid_transaction, cookie} =
             OIDC.take_transaction(cookie, "state-1", now: 1_000)

    assert {:ok, second, cookie} = OIDC.take_transaction(cookie, "state-2", now: 1_000)
    assert second.nonce == "nonce-2"

    assert {:error, :invalid_transaction, ^cookie} =
             OIDC.take_transaction(cookie, "unknown", now: 1_000)

    remaining = Enum.drop(transactions, 2)

    assert nil ==
             Enum.reduce(remaining, cookie, fn transaction, cookie ->
               assert {:ok, ^transaction, remaining_cookie} =
                        OIDC.take_transaction(cookie, transaction.state, now: 1_000)

               remaining_cookie
             end)
  end

  test "accepts a legacy single transaction while adding a concurrent login" do
    first = %{
      state: "first",
      nonce: "first-nonce",
      verifier: String.duplicate("a", 43),
      created_at: 1_000
    }

    second = %{
      state: "second",
      nonce: "second-nonce",
      verifier: String.duplicate("b", 43),
      created_at: 1_000
    }

    legacy = OIDC.encode_transaction(first)
    assert {:ok, cookie} = OIDC.add_transaction(legacy, second, now: 1_000)
    assert {:ok, ^first, remaining} = OIDC.take_transaction(cookie, "first", now: 1_000)
    assert {:ok, ^second, nil} = OIDC.take_transaction(remaining, "second", now: 1_000)
  end

  test "form-encodes confidential client credentials before HTTP Basic", %{
    runtime: runtime
  } do
    config = %{
      runtime.oidc
      | web_client_id: "client id+value",
        web_client_secret: "secret+/= value",
        web_client_auth_method: :client_secret_basic
    }

    runtime = %{runtime | oidc: config}

    transaction = %{
      state: "state",
      nonce: "nonce",
      verifier: String.duplicate("v", 43),
      created_at: 1
    }

    test = self()

    client = fn url, options ->
      send(test, {:confidential_request, url, options})
      {:error, :stop}
    end

    assert {:error, :authentication_failed} =
             OIDC.authenticate(runtime, transaction, "code", http_client: client)

    assert_receive {:confidential_request, "https://issuer.example/token", options}

    assert {"authorization", "Basic " <> encoded} =
             Enum.find(options[:headers], &match?({"authorization", _value}, &1))

    expected =
      Base.encode64(
        URI.encode_www_form(config.web_client_id) <>
          ":" <> URI.encode_www_form(config.web_client_secret)
      )

    assert encoded == expected
    refute options[:body] =~ config.web_client_secret
  end

  test "validates state before exchanging the code and normalizes the ID token", %{
    runtime: runtime,
    token_response: token_response,
    http_client: http_client
  } do
    assert {:ok, _url, transaction} = OIDC.begin(runtime)
    id_token = id_token(transaction.nonce)

    Agent.update(token_response, fn _ ->
      response(%{"id_token" => id_token, "access_token" => "access-token"})
    end)

    assert {:error, :invalid_transaction} = OIDC.consume(transaction, "wrong-state")
    assert {:ok, consumed} = OIDC.consume(transaction, transaction.state)

    assert {:ok, context} =
             OIDC.authenticate(runtime, consumed, "authorization-code", http_client: http_client)

    assert MapSet.equal?(context.capabilities, MapSet.new(@capabilities))

    assert_receive {:token_request, "https://issuer.example/token", options}
    assert options[:method] == :post
    assert options[:redirect] == false
    assert options[:receive_timeout] > 0
    assert options[:request_timeout] > 0
    refute Enum.any?(options[:headers], &match?({"authorization", _value}, &1))

    body = URI.decode_query(options[:body])
    assert body["grant_type"] == "authorization_code"
    assert body["redirect_uri"] == "https://ui.example/auth/callback"
    assert body["client_id"] == "web-client"
    assert body["code"] == "authorization-code"
    assert body["code_verifier"] == transaction.verifier
  end

  test "rejects malformed or non-JSON token responses generically", %{
    runtime: runtime,
    token_response: token_response,
    http_client: http_client
  } do
    assert {:ok, _url, transaction} = OIDC.begin(runtime)
    assert {:ok, consumed} = OIDC.consume(transaction, transaction.state)

    Agent.update(token_response, fn _ ->
      {:ok,
       %Req.Response{
         status: 200,
         headers: %{"content-type" => ["text/html"]},
         body: JSON.encode!(%{"id_token" => id_token(transaction.nonce), "access_token" => "a"})
       }}
    end)

    assert {:error, :authentication_failed} =
             OIDC.authenticate(runtime, consumed, "authorization-code", http_client: http_client)

    assert {:error, :authentication_failed} =
             OIDC.authenticate(runtime, consumed, String.duplicate("x", 4_097),
               http_client: http_client
             )
  end

  defp id_token(nonce) do
    now = System.system_time(:second)

    claims = %{
      "iss" => "https://issuer.example",
      "aud" => "web-client",
      "sub" => "subject-1",
      "exp" => now + 300,
      "iat" => now,
      "nonce" => nonce,
      "roles" => ["operator"]
    }

    JOSE.JWT.sign(
      @private_key,
      JOSE.JWS.from_map(%{"alg" => "RS256", "kid" => "web-key"}),
      claims
    )
    |> JOSE.JWS.compact()
    |> elem(1)
  end

  defp response(body) do
    {:ok,
     %Req.Response{
       status: 200,
       headers: %{"content-type" => ["application/json"]},
       body: JSON.encode!(body)
     }}
  end
end
