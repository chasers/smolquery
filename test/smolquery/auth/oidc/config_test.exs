defmodule Smolquery.Auth.OIDC.ConfigTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.OIDC.Config

  @base [
    issuer: "https://issuer.example/",
    api_audience: "smolquery-api",
    web_client_id: "smolquery-web",
    web_client_secret: "secret",
    web_host: "ui.example",
    web_origin: "https://ui.example",
    web_redirect_uri: "https://ui.example/auth/callback"
  ]

  test "retains the exact issuer and validates role-specific settings" do
    config = Config.new([oidc: @base], :api)
    assert config.issuer == "https://issuer.example/"
    assert config.api_audience == "smolquery-api"
    assert config.web_client_id == nil
    assert config.algorithms == ["RS256"]
    assert config.refresh_failure_backoff_ms == 1_000
  end

  test "requires API audience and web settings only for their roles" do
    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_API_AUDIENCE/, fn ->
      Config.new([oidc: Keyword.delete(@base, :api_audience)], :api)
    end

    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_WEB_CLIENT_ID/, fn ->
      Config.new([oidc: Keyword.delete(@base, :web_client_id)], :web)
    end

    assert Config.new([oidc: Keyword.delete(@base, :web_client_id)], :api)
  end

  test "requires the deployed host and exact callback route" do
    for {key, value, env} <- [
          {:web_host, "other.example", "SMOLQUERY_WEB_HOST"},
          {:web_redirect_uri, "https://ui.example/not-the-callback",
           "SMOLQUERY_OIDC_WEB_REDIRECT_URI"},
          {:web_redirect_uri, "https://ui.example/auth/callback?next=/",
           "SMOLQUERY_OIDC_WEB_REDIRECT_URI"}
        ] do
      assert_raise ArgumentError, ~r/#{env}/, fn ->
        Config.new([oidc: Keyword.put(@base, key, value)], :web)
      end
    end
  end

  test "validates bounded browser authorization scopes" do
    config =
      Config.new([oidc: Keyword.put(@base, :web_scopes, ["openid", "profile", "groups"])], :web)

    assert config.web_scopes == ["openid", "profile", "groups"]

    for scopes <- [
          [],
          ["profile"],
          ["openid", "openid"],
          ["openid", "not a scope"],
          ["openid" | Enum.map(1..32, &"scope#{&1}")]
        ] do
      assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_WEB_SCOPES/, fn ->
        Config.new([oidc: Keyword.put(@base, :web_scopes, scopes)], :web)
      end
    end
  end

  test "rejects insecure, malformed, and unbounded settings" do
    cases = [
      {:issuer, "http://issuer.example", "SMOLQUERY_OIDC_ISSUER"},
      {:issuer, "https://issuer.example?x=1", "SMOLQUERY_OIDC_ISSUER"},
      {:algorithms, [], "SMOLQUERY_OIDC_ALGORITHMS"},
      {:algorithms, ["none"], "SMOLQUERY_OIDC_ALGORITHMS"},
      {:algorithms, ["RS256", "RS256"], "SMOLQUERY_OIDC_ALGORITHMS"},
      {:clock_skew, 301, "SMOLQUERY_OIDC_CLOCK_SKEW"},
      {:refresh_failure_backoff_ms, 0, "SMOLQUERY_OIDC_REFRESH_FAILURE_BACKOFF_MS"},
      {:receive_timeout_ms, 0, "SMOLQUERY_OIDC_RECEIVE_TIMEOUT_MS"}
    ]

    for {key, value, message} <- cases do
      assert_raise ArgumentError, ~r/#{message}/, fn ->
        Config.new([oidc: Keyword.put(@base, key, value)], :api)
      end
    end

    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_WEB_ORIGIN/, fn ->
      Config.new([oidc: Keyword.put(@base, :web_origin, "http://ui.example")], :web)
    end

    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_WEB_REDIRECT_URI/, fn ->
      Config.new(
        [oidc: Keyword.put(@base, :web_redirect_uri, "https://other.example/callback")],
        :web
      )
    end
  end

  test "requires a secret for confidential clients but not public clients" do
    config = Keyword.delete(@base, :web_client_secret)

    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_WEB_CLIENT_SECRET/, fn ->
      Config.new([oidc: config], :web)
    end

    public = config |> Keyword.put(:web_client_auth_method, :none) |> Config.new(:web)
    assert public.web_client_secret == nil

    secret = "super-secret-client-value"

    error =
      try do
        Config.new(
          [
            oidc:
              Keyword.put(
                Keyword.put(@base, :web_client_auth_method, :none),
                :web_client_secret,
                secret
              )
          ],
          :web
        )
      rescue
        exception -> exception
      end

    refute Exception.message(error) =~ secret
    refute inspect(error) =~ secret
  end

  test "constrains exact claim values to known capabilities" do
    mapping = %{
      "roles" => %{"reader" => [:query], "operator" => [:web_access, :platform_operate]}
    }

    config = Config.new([oidc: Keyword.put(@base, :claim_capabilities, mapping)], :api)
    assert config.claim_capabilities == mapping

    for invalid <- [
          %{"roles" => %{"" => [:query]}},
          %{"roles" => %{"reader" => [:unknown]}},
          %{"roles" => %{}}
        ] do
      assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_CLAIM_CAPABILITIES/, fn ->
        Config.new([oidc: Keyword.put(@base, :claim_capabilities, invalid)], :api)
      end
    end
  end

  test "validates token type and required payload claim settings" do
    config =
      Config.new(
        [
          oidc:
            Keyword.merge(@base,
              typ_allowlist: ["at+jwt"],
              required_claims: %{"token_use" => ["access"]},
              max_token_bytes: 1024,
              max_segment_bytes: 512,
              iat_future_seconds: 60,
              forced_refresh_cooldown_ms: 250
            )
        ],
        :api
      )

    assert config.typ_allowlist == ["at+jwt"]
    assert config.required_claims == %{"token_use" => ["access"]}
    assert config.max_token_bytes == 1024
    assert config.max_segment_bytes == 512
    assert config.iat_future_seconds == 60
    assert config.forced_refresh_cooldown_ms == 250

    for {key, value, env} <- [
          {:typ_allowlist, ["at+jwt", "at+jwt"], "SMOLQUERY_OIDC_TOKEN_TYPES"},
          {:required_claims, %{"token_use" => []}, "SMOLQUERY_OIDC_REQUIRED_CLAIMS"},
          {:max_token_bytes, 0, "SMOLQUERY_OIDC_MAX_TOKEN_BYTES"},
          {:max_segment_bytes, 0, "SMOLQUERY_OIDC_MAX_TOKEN_SEGMENT_BYTES"},
          {:iat_future_seconds, -1, "SMOLQUERY_OIDC_IAT_FUTURE_SECONDS"},
          {:forced_refresh_cooldown_ms, 0, "SMOLQUERY_OIDC_FORCED_REFRESH_COOLDOWN_MS"}
        ] do
      assert_raise ArgumentError, ~r/#{env}/, fn ->
        Config.new([oidc: Keyword.put(@base, key, value)], :api)
      end
    end
  end

  test "resolves separate API and web token profiles with global fallbacks" do
    options =
      Keyword.merge(@base,
        typ_allowlist: ["legacy+jwt"],
        required_claims: %{"legacy" => ["true"]},
        api_typ_allowlist: ["at+jwt"],
        api_required_claims: %{"token_use" => ["access"]},
        web_typ_allowlist: ["JWT"],
        web_required_claims: %{"token_use" => ["id"]}
      )

    api = Config.new([oidc: options], :api)
    web = Config.new([oidc: options], :web)

    assert api.typ_allowlist == ["at+jwt"]
    assert api.required_claims == %{"token_use" => ["access"]}
    assert web.typ_allowlist == ["JWT"]
    assert web.required_claims == %{"token_use" => ["id"]}

    fallback = Config.new([oidc: @base ++ [typ_allowlist: ["legacy+jwt"]]], :api)
    assert fallback.typ_allowlist == ["legacy+jwt"]

    assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_API_TOKEN_TYPES/, fn ->
      invalid = Keyword.put(options, :api_typ_allowlist, ["at+jwt", "at+jwt"])
      Config.new([oidc: invalid], :api)
    end
  end

  test "rejects an API audience that aliases the browser client id" do
    options = Keyword.put(@base, :api_audience, "smolquery-web")

    for role <- [:api, :web] do
      assert_raise ArgumentError, ~r/SMOLQUERY_OIDC_API_AUDIENCE.*different/, fn ->
        Config.new([oidc: options], role)
      end
    end
  end

  test "redacts the client secret" do
    config = Config.new([oidc: @base], :web)
    refute inspect(config) =~ "\"secret\""
  end
end
