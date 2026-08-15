defmodule Smolquery.Auth.OIDC.Config do
  @moduledoc """
  Validated OIDC deployment configuration.

  Issuer and provider endpoints must use HTTPS. Discovery endpoints may be
  hosted separately from the issuer, but are never followed across redirects.
  This module validates configuration before a role listener starts; it does
  not authenticate requests or infer tenant identity from provider claims.
  """

  alias Smolquery.Auth.Context

  @algorithms ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"]
  @derive {Inspect, except: [:web_client_secret]}
  defstruct [
    :issuer,
    :api_audience,
    :web_client_id,
    :web_client_secret,
    :web_client_auth_method,
    :web_origin,
    :web_redirect_uri,
    :algorithms,
    :clock_skew,
    :claim_capabilities,
    :typ_allowlist,
    :required_claims,
    :max_token_bytes,
    :max_segment_bytes,
    :iat_future_seconds,
    :discovery_max_age_ms,
    :jwks_max_age_ms,
    :forced_refresh_cooldown_ms,
    :connect_timeout_ms,
    :receive_timeout_ms,
    :request_timeout_ms,
    :max_body_bytes
  ]

  @type claim_capabilities :: %{
          optional(String.t()) => %{optional(String.t()) => [Context.capability()]}
        }
  @type required_claims :: %{optional(String.t()) => [String.t()]}
  @type t :: %__MODULE__{
          issuer: String.t(),
          api_audience: String.t() | nil,
          web_client_id: String.t() | nil,
          web_client_secret: String.t() | nil,
          web_client_auth_method: :none | :client_secret_basic,
          web_origin: String.t() | nil,
          web_redirect_uri: String.t() | nil,
          algorithms: [String.t()],
          clock_skew: non_neg_integer(),
          claim_capabilities: claim_capabilities(),
          typ_allowlist: [String.t()],
          required_claims: required_claims(),
          max_token_bytes: pos_integer(),
          max_segment_bytes: pos_integer(),
          iat_future_seconds: non_neg_integer(),
          discovery_max_age_ms: non_neg_integer(),
          jwks_max_age_ms: non_neg_integer(),
          forced_refresh_cooldown_ms: non_neg_integer(),
          connect_timeout_ms: pos_integer(),
          receive_timeout_ms: pos_integer(),
          request_timeout_ms: pos_integer(),
          max_body_bytes: pos_integer()
        }

  @defaults [
    algorithms: ["RS256"],
    clock_skew: 30,
    discovery_max_age_ms: 3_600_000,
    jwks_max_age_ms: 3_600_000,
    forced_refresh_cooldown_ms: 1_000,
    connect_timeout_ms: 2_000,
    receive_timeout_ms: 5_000,
    request_timeout_ms: 10_000,
    max_body_bytes: 1_048_576,
    typ_allowlist: [],
    required_claims: %{},
    max_token_bytes: 65_536,
    max_segment_bytes: 32_768,
    iat_future_seconds: 300,
    web_client_auth_method: :client_secret_basic,
    claim_capabilities: %{}
  ]

  @doc "Validates OIDC settings for a role before its listener starts."
  @spec new(keyword(), :api | :web) :: t()
  def new(opts, role) when role in [:api, :web] and is_list(opts) do
    global = Application.get_env(:smolquery, __MODULE__, [])
    config = Keyword.merge(@defaults, Keyword.merge(global, Keyword.get(opts, :oidc, opts)))

    issuer = https_uri!(config, :issuer, "SMOLQUERY_OIDC_ISSUER")

    if URI.parse(issuer).query != nil,
      do: invalid!("SMOLQUERY_OIDC_ISSUER", issuer, "an HTTPS issuer without a query")

    api_audience = required_for_role(config, :api_audience, role, "SMOLQUERY_OIDC_API_AUDIENCE")

    web_client_id =
      required_for_role(config, :web_client_id, role, "SMOLQUERY_OIDC_WEB_CLIENT_ID")

    {web_origin, web_redirect_uri} = web_urls(config, role)
    auth_method = config |> Keyword.fetch!(:web_client_auth_method) |> auth_method!()
    web_client_secret = client_secret(config, auth_method, role)
    algorithms = algorithms!(Keyword.fetch!(config, :algorithms))
    claim_capabilities = claim_capabilities!(Keyword.fetch!(config, :claim_capabilities))

    typ_allowlist =
      string_allowlist!(Keyword.fetch!(config, :typ_allowlist), "SMOLQUERY_OIDC_TOKEN_TYPES")

    required_claims = required_claims!(Keyword.fetch!(config, :required_claims))

    %__MODULE__{
      issuer: issuer,
      api_audience: api_audience,
      web_client_id: web_client_id,
      web_client_secret: web_client_secret,
      web_client_auth_method: auth_method,
      web_origin: web_origin,
      web_redirect_uri: web_redirect_uri,
      algorithms: algorithms,
      clock_skew: bounded_integer!(config, :clock_skew, "SMOLQUERY_OIDC_CLOCK_SKEW", 300),
      claim_capabilities: claim_capabilities,
      typ_allowlist: typ_allowlist,
      required_claims: required_claims,
      max_token_bytes:
        positive_bounded_integer!(
          config,
          :max_token_bytes,
          "SMOLQUERY_OIDC_MAX_TOKEN_BYTES",
          1_048_576
        ),
      max_segment_bytes:
        positive_bounded_integer!(
          config,
          :max_segment_bytes,
          "SMOLQUERY_OIDC_MAX_TOKEN_SEGMENT_BYTES",
          524_288
        ),
      iat_future_seconds:
        bounded_integer!(
          config,
          :iat_future_seconds,
          "SMOLQUERY_OIDC_IAT_FUTURE_SECONDS",
          86_400
        ),
      discovery_max_age_ms:
        bounded_integer!(
          config,
          :discovery_max_age_ms,
          "SMOLQUERY_OIDC_DISCOVERY_MAX_AGE_MS",
          86_400_000
        ),
      jwks_max_age_ms:
        bounded_integer!(config, :jwks_max_age_ms, "SMOLQUERY_OIDC_JWKS_MAX_AGE_MS", 86_400_000),
      forced_refresh_cooldown_ms:
        bounded_integer!(
          config,
          :forced_refresh_cooldown_ms,
          "SMOLQUERY_OIDC_FORCED_REFRESH_COOLDOWN_MS",
          86_400_000
        ),
      connect_timeout_ms:
        positive_bounded_integer!(
          config,
          :connect_timeout_ms,
          "SMOLQUERY_OIDC_CONNECT_TIMEOUT_MS",
          30_000
        ),
      receive_timeout_ms:
        positive_bounded_integer!(
          config,
          :receive_timeout_ms,
          "SMOLQUERY_OIDC_RECEIVE_TIMEOUT_MS",
          60_000
        ),
      request_timeout_ms:
        positive_bounded_integer!(
          config,
          :request_timeout_ms,
          "SMOLQUERY_OIDC_REQUEST_TIMEOUT_MS",
          120_000
        ),
      max_body_bytes:
        positive_bounded_integer!(
          config,
          :max_body_bytes,
          "SMOLQUERY_OIDC_MAX_BODY_BYTES",
          10_485_760
        )
    }
  end

  @doc "Validates an exact claim-value-to-capabilities mapping."
  @spec claim_capabilities!(term()) :: claim_capabilities()
  def claim_capabilities!(mapping) when is_map(mapping) do
    Enum.reduce(mapping, %{}, &parse_claim_mapping(&1, &2, mapping))
  end

  def claim_capabilities!(value),
    do: invalid!("SMOLQUERY_OIDC_CLAIM_CAPABILITIES", value, "a JSON claim-value capability map")

  defp parse_claim_mapping({claim, values}, acc, original) do
    if nonempty_string?(claim) and is_map(values) do
      Map.put(acc, claim, parse_claim_values(values, original))
    else
      raise_invalid_claim_mapping(original)
    end
  end

  defp parse_claim_values(values, original) do
    parsed = Enum.reduce(values, %{}, &parse_claim_value(&1, &2, original))
    if map_size(parsed) == 0, do: raise_invalid_claim_mapping(original), else: parsed
  end

  defp parse_claim_value({value, capabilities}, acc, original) do
    if nonempty_string?(value) and is_list(capabilities) and capabilities != [] and
         Enum.all?(capabilities, &Context.capability?/1) do
      Map.put(acc, value, Enum.uniq(capabilities))
    else
      raise_invalid_claim_mapping(original)
    end
  end

  def algorithms, do: @algorithms

  @doc "Returns the closed set of configured protected-header token types."
  @spec string_allowlist!(term(), String.t()) :: [String.t()]
  def string_allowlist!([], _env), do: []

  def string_allowlist!(values, env) when is_list(values) do
    if Enum.all?(values, &nonempty_string?/1) and length(values) == length(Enum.uniq(values)),
      do: values,
      else: invalid!(env, values, "a unique non-empty string list")
  end

  def string_allowlist!(value, env), do: invalid!(env, value, "a string list")

  @doc "Validates exact required payload claim values."
  @spec required_claims!(term()) :: required_claims()
  def required_claims!(claims) when is_map(claims) do
    Enum.reduce(claims, %{}, fn {claim, values}, acc ->
      if nonempty_string?(claim) and is_list(values) and values != [] and
           Enum.all?(values, &nonempty_string?/1) and
           length(values) == length(Enum.uniq(values)) do
        Map.put(acc, claim, values)
      else
        invalid!("SMOLQUERY_OIDC_REQUIRED_CLAIMS", claims, "a map of claim names to string lists")
      end
    end)
  end

  def required_claims!(value),
    do: invalid!("SMOLQUERY_OIDC_REQUIRED_CLAIMS", value, "a map of claim names to string lists")

  @spec raise_invalid_claim_mapping(term()) :: no_return()
  defp raise_invalid_claim_mapping(mapping),
    do:
      invalid!("SMOLQUERY_OIDC_CLAIM_CAPABILITIES", mapping, "a JSON claim-value capability map")

  defp required_for_role(config, :api_audience, :api, env),
    do: nonempty!(Keyword.get(config, :api_audience), env)

  defp required_for_role(config, :web_client_id, :web, env),
    do: nonempty!(Keyword.get(config, :web_client_id), env)

  defp required_for_role(_config, _key, _role, _env), do: nil

  defp web_urls(_config, :api), do: {nil, nil}

  defp web_urls(config, :web) do
    origin = https_uri!(config, :web_origin, "SMOLQUERY_OIDC_WEB_ORIGIN")
    redirect = https_uri!(config, :web_redirect_uri, "SMOLQUERY_OIDC_WEB_REDIRECT_URI")
    origin_uri = URI.parse(origin)
    redirect_uri = URI.parse(redirect)

    if origin_uri.path not in [nil, "", "/"] or origin_uri.query != nil or
         origin_uri.fragment != nil do
      invalid!("SMOLQUERY_OIDC_WEB_ORIGIN", origin, "an HTTPS origin without a path")
    end

    if origin_tuple(origin_uri) != origin_tuple(redirect_uri) do
      invalid!(
        "SMOLQUERY_OIDC_WEB_REDIRECT_URI",
        redirect,
        "a URI with the exact configured web origin"
      )
    end

    {origin, redirect}
  end

  defp origin_tuple(uri), do: {uri.scheme, uri.host, uri.port || default_port(uri.scheme)}
  defp default_port("https"), do: 443

  defp client_secret(config, :client_secret_basic, :web),
    do: nonempty!(Keyword.get(config, :web_client_secret), "SMOLQUERY_OIDC_WEB_CLIENT_SECRET")

  defp client_secret(config, :none, :web) do
    case Keyword.get(config, :web_client_secret) do
      nil ->
        nil

      _value ->
        raise ArgumentError, "SMOLQUERY_OIDC_WEB_CLIENT_SECRET must be unset for a public client"
    end
  end

  defp client_secret(_config, _method, _role), do: nil
  defp auth_method!(:none), do: :none
  defp auth_method!(:client_secret_basic), do: :client_secret_basic

  defp auth_method!(value),
    do: invalid!("SMOLQUERY_OIDC_WEB_CLIENT_AUTH_METHOD", value, "none or client_secret_basic")

  defp algorithms!(algorithms) when is_list(algorithms) do
    cond do
      algorithms == [] ->
        invalid!(
          "SMOLQUERY_OIDC_ALGORITHMS",
          algorithms,
          "a non-empty asymmetric algorithm allowlist"
        )

      Enum.any?(algorithms, &(not is_binary(&1) or &1 == "")) ->
        invalid!("SMOLQUERY_OIDC_ALGORITHMS", algorithms, "known asymmetric algorithms")

      length(algorithms) != length(Enum.uniq(algorithms)) ->
        invalid!("SMOLQUERY_OIDC_ALGORITHMS", algorithms, "unique algorithms")

      Enum.any?(algorithms, &(&1 not in @algorithms)) ->
        invalid!("SMOLQUERY_OIDC_ALGORITHMS", algorithms, "RS/PS/ES asymmetric algorithms")

      true ->
        algorithms
    end
  end

  defp algorithms!(value),
    do: invalid!("SMOLQUERY_OIDC_ALGORITHMS", value, "a non-empty asymmetric algorithm allowlist")

  defp https_uri!(config, key, env) do
    value = nonempty!(Keyword.get(config, key), env)
    uri = URI.parse(value)

    if uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and is_nil(uri.userinfo) and
         is_nil(uri.fragment) do
      value
    else
      invalid!(env, value, "an HTTPS URI with a host and no userinfo or fragment")
    end
  end

  defp bounded_integer!(config, key, env, maximum) do
    value = Keyword.get(config, key)

    if is_integer(value) and value >= 0 and value <= maximum,
      do: value,
      else: invalid!(env, value, "a bounded non-negative integer at most #{maximum}")
  end

  defp positive_bounded_integer!(config, key, env, maximum) do
    value = Keyword.get(config, key)

    if is_integer(value) and value > 0 and value <= maximum,
      do: value,
      else: invalid!(env, value, "a bounded positive integer at most #{maximum}")
  end

  defp nonempty!(value, _env) when is_binary(value) and byte_size(value) > 0, do: value
  defp nonempty!(value, env), do: invalid!(env, value, "a non-empty value")
  defp nonempty_string?(value), do: is_binary(value) and byte_size(value) > 0

  defp invalid!(env, value, expected),
    do: raise(ArgumentError, "#{env} has invalid value #{inspect(value)}; expected #{expected}")
end
