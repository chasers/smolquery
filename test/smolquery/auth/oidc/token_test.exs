defmodule Smolquery.Auth.OIDC.TokenTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.OIDC.{Config, Provider, Token}

  @now 1_700_000_000
  @private_key JOSE.JWK.generate_key({:rsa, 2048})
  @public_key JOSE.JWK.to_map(JOSE.JWK.to_public(@private_key))
              |> elem(1)
              |> Map.put("kid", "one")
  @jwks %{"keys" => [@public_key]}
  @metadata %{
    "issuer" => "https://issuer.example",
    "authorization_endpoint" => "https://issuer.example/authorize",
    "token_endpoint" => "https://issuer.example/token",
    "jwks_uri" => "https://issuer.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }
  @base_config Config.new(
                 [
                   oidc: [
                     issuer: "https://issuer.example",
                     api_audience: "smolquery-api",
                     claim_capabilities: %{
                       "scope" => %{
                         "query" => [:query],
                         "writer" => [:ingest],
                         "admin" => [:catalog_manage]
                       }
                     }
                   ]
                 ],
                 :api
               )

  test "verifies and normalizes a signed token" do
    claims =
      valid_claims(%{"scope" => ["query", "writer", "admin"], "name" => "Ada", "azp" => "web"})

    assert {:ok, context} = Token.verify(token(claims), @base_config, @jwks, %{}, now: @now)
    assert context.principal.authn == :oidc
    assert context.principal.subject == "subject-1"
    assert context.principal.display_name == "Ada"
    assert context.principal.client_id == "web"
    assert MapSet.equal?(context.capabilities, MapSet.new([:query, :ingest, :catalog_manage]))
    assert context.expires_at == claims["exp"] + @base_config.clock_skew
    refute inspect(context) =~ token(claims)
  end

  test "requires exact issuer, audience, subject, and integer expiration" do
    for change <- [
          {"iss", "https://other.example"},
          {"aud", "other"},
          {"sub", ""},
          {"exp", "later"},
          {"exp", true},
          {"exp", 1.2}
        ] do
      assert :error =
               Token.verify(
                 token(valid_claims(%{elem(change, 0) => elem(change, 1)})),
                 @base_config,
                 @jwks,
                 %{},
                 now: @now
               )
    end

    assert :error =
             Token.verify(token(Map.delete(valid_claims(), "sub")), @base_config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(Map.delete(valid_claims(), "exp")), @base_config, @jwks, %{},
               now: @now
             )
  end

  test "applies expiration, not-before, and issued-at boundaries" do
    config = %{@base_config | clock_skew: 10, iat_future_seconds: 20}

    assert {:ok, _} =
             Token.verify(token(valid_claims(%{"exp" => @now - 1})), config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"exp" => @now - 10})), config, @jwks, %{},
               now: @now
             )

    assert {:ok, _} =
             Token.verify(token(valid_claims(%{"nbf" => @now + 10})), config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"nbf" => @now + 11})), config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"nbf" => -1})), config, @jwks, %{}, now: @now)

    assert {:ok, _} =
             Token.verify(token(valid_claims(%{"iat" => @now + 20})), config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"iat" => @now + 21})), config, @jwks, %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"iat" => "future"})), config, @jwks, %{},
               now: @now
             )
  end

  test "requires a bounded kid and rejects duplicate or incompatible keys" do
    no_kid = token(valid_claims(), %{"alg" => "RS256"})
    assert :error = Token.verify(no_kid, @base_config, @jwks, %{}, now: @now)

    duplicate = %{"keys" => [@public_key, @public_key]}
    assert :error = Token.verify(token(valid_claims()), @base_config, duplicate, %{}, now: @now)

    wrong_use = Map.put(@public_key, "use", "enc")

    assert :error =
             Token.verify(token(valid_claims()), @base_config, %{"keys" => [wrong_use]}, %{},
               now: @now
             )

    wrong_type = Map.put(@public_key, "kty", "EC")

    assert :error =
             Token.verify(token(valid_claims()), @base_config, %{"keys" => [wrong_type]}, %{},
               now: @now
             )
  end

  test "authenticate uses the supervised provider path to accept one real key rotation" do
    rotated_key = JOSE.JWK.generate_key({:rsa, 2048})

    rotated_public =
      JOSE.JWK.to_map(JOSE.JWK.to_public(rotated_key)) |> elem(1) |> Map.put("kid", "two")

    rotated_token = token(valid_claims(), %{"alg" => "RS256", "kid" => "two"}, rotated_key)
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://issuer.example/keys" ->
          count = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
          {:ok, response(if(count == 0, do: @jwks, else: %{"keys" => [rotated_public]}))}
      end
    end

    config = %{@base_config | forced_refresh_cooldown_ms: 100_000}
    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)
    assert {:ok, _context} = Token.authenticate(rotated_token, config, pid, now: @now)
    assert Agent.get(counter, & &1) == 2
    Process.exit(pid, :normal)
  end

  test "authenticate rejects stale unknown keys during refresh outage and cooldown" do
    rotated_key = JOSE.JWK.generate_key({:rsa, 2048})
    rotated_token = token(valid_claims(), %{"alg" => "RS256", "kid" => "two"}, rotated_key)
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://issuer.example/keys" ->
          count = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
          if count == 0, do: {:ok, response(@jwks)}, else: {:error, :timeout}
      end
    end

    config = %{@base_config | forced_refresh_cooldown_ms: 100_000}
    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)
    assert :error = Token.authenticate(rotated_token, config, pid, now: @now)
    assert :error = Token.authenticate(rotated_token, config, pid, now: @now)
    assert Agent.get(counter, & &1) == 2
    Process.exit(pid, :normal)
  end

  test "refreshes once for an unknown kid and fails when refresh has no key" do
    rotated_key = JOSE.JWK.generate_key({:rsa, 2048})

    rotated_public =
      JOSE.JWK.to_map(JOSE.JWK.to_public(rotated_key)) |> elem(1) |> Map.put("kid", "two")

    rotated_token = token(valid_claims(), %{"alg" => "RS256", "kid" => "two"}, rotated_key)

    assert {:ok, _context} =
             Token.verify(rotated_token, @base_config, @jwks, %{"keys" => [rotated_public]},
               now: @now
             )

    assert :error =
             Token.verify(rotated_token, @base_config, @jwks, %{"keys" => []}, now: @now)
  end

  test "rejects invalid signatures and algorithms" do
    other_key = JOSE.JWK.generate_key({:rsa, 2048})

    assert :error =
             Token.verify(
               token(valid_claims(), %{"alg" => "RS256", "kid" => "one"}, other_key),
               @base_config,
               @jwks,
               %{},
               now: @now
             )

    assert :error =
             Token.verify(
               replace_header(token(valid_claims()), %{"alg" => "HS256", "kid" => "one"}),
               @base_config,
               @jwks,
               %{},
               now: @now
             )

    rs384_config = %{@base_config | algorithms: ["RS256"]}

    assert :error =
             Token.verify(
               token(valid_claims(), %{"alg" => "RS384", "kid" => "one"}),
               rs384_config,
               @jwks,
               %{},
               now: @now
             )

    malformed = "not.a.jwt"
    assert :error = Token.verify(malformed, @base_config, @jwks, %{}, now: @now)
  end

  test "rejects prohibited protected headers and malformed optional typ" do
    for extra <- [
          %{"jku" => "https://attacker.example/key"},
          %{"jwk" => %{}},
          %{"x5u" => "https://attacker.example/cert"},
          %{"crit" => ["exp"]},
          %{"b64" => false},
          %{"typ" => ""},
          %{"typ" => 42}
        ] do
      header = Map.merge(%{"alg" => "RS256", "kid" => "one"}, extra)

      assert :error =
               Token.verify(token(valid_claims(), header), @base_config, @jwks, %{}, now: @now)
    end
  end

  test "rejects empty audience members and does not partially grant malformed list claims" do
    assert :error =
             Token.verify(
               token(valid_claims(%{"aud" => ["", "smolquery-api"]})),
               @base_config,
               @jwks,
               %{},
               now: @now
             )

    claims = valid_claims(%{"scope" => ["query", 42, "admin"]})
    assert {:ok, context} = Token.verify(token(claims), @base_config, @jwks, %{}, now: @now)
    assert MapSet.equal?(context.capabilities, MapSet.new())
  end

  test "rejects browser ID-token audiences at the API boundary" do
    config = %{@base_config | web_client_id: "smolquery-web"}

    browser_id_claims =
      valid_claims(%{
        "aud" => ["smolquery-web", "smolquery-api"],
        "azp" => "smolquery-web",
        "nonce" => "browser-nonce",
        "scope" => ["query", "admin"]
      })

    assert :error =
             Token.verify(token(browser_id_claims), config, @jwks, %{}, now: @now)

    access_claims =
      valid_claims(%{
        "aud" => ["other-resource", "smolquery-api"],
        "azp" => "smolquery-web",
        "scope" => ["query"]
      })

    assert {:ok, context} =
             Token.verify(token(access_claims), config, @jwks, %{}, now: @now)

    assert MapSet.equal?(context.capabilities, MapSet.new([:query]))
  end

  test "enforces optional type and required claim values" do
    config = %{
      @base_config
      | typ_allowlist: ["at+jwt"],
        required_claims: %{"token_use" => ["access"]}
    }

    assert {:ok, _} =
             Token.verify(
               token(valid_claims(%{"token_use" => "access"}), %{
                 "alg" => "RS256",
                 "kid" => "one",
                 "typ" => "at+jwt"
               }),
               config,
               @jwks,
               %{},
               now: @now
             )

    assert :error =
             Token.verify(
               token(valid_claims(%{"token_use" => "id"}), %{
                 "alg" => "RS256",
                 "kid" => "one",
                 "typ" => "at+jwt"
               }),
               config,
               @jwks,
               %{},
               now: @now
             )

    assert :error =
             Token.verify(token(valid_claims(%{"token_use" => "access"})), config, @jwks, %{},
               now: @now
             )
  end

  test "rejects oversized compact tokens before JOSE decoding" do
    config = %{@base_config | max_token_bytes: 100}

    assert :error =
             Token.verify(
               token(valid_claims(%{"padding" => String.duplicate("x", 200)})),
               config,
               @jwks,
               %{},
               now: @now
             )

    segment_config = %{@base_config | max_token_bytes: 10_000, max_segment_bytes: 100}

    assert :error =
             Token.verify(
               token(valid_claims(%{"padding" => String.duplicate("x", 200)})),
               segment_config,
               @jwks,
               %{},
               now: @now
             )
  end

  defp unique_name, do: {:global, {__MODULE__, System.unique_integer([:positive])}}

  defp response(body) do
    %Req.Response{
      status: 200,
      headers: %{"content-type" => ["application/json"]},
      body: JSON.encode!(body)
    }
  end

  defp valid_claims(overrides \\ %{}),
    do:
      Map.merge(
        %{
          "iss" => "https://issuer.example",
          "aud" => "smolquery-api",
          "sub" => "subject-1",
          "exp" => @now + 100
        },
        overrides
      )

  defp replace_header(token, header) do
    [_old_header, payload, signature] = String.split(token, ".", trim: false)
    encoded = Base.url_encode64(JSON.encode!(header), padding: false)
    Enum.join([encoded, payload, signature], ".")
  end

  defp token(claims, header \\ %{"alg" => "RS256", "kid" => "one"}, key \\ @private_key) do
    JOSE.JWT.sign(key, JOSE.JWS.from_map(header), claims) |> JOSE.JWS.compact() |> elem(1)
  end
end
