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
    :web_scopes,
    :algorithms,
    :clock_skew,
    :claim_capabilities,
    :discovery_max_age_ms,
    :jwks_max_age_ms,
    :refresh_failure_backoff_ms,
    :connect_timeout_ms,
    :receive_timeout_ms,
    :request_timeout_ms,
    :max_body_bytes
  ]

  @type claim_capabilities :: %{
          optional(String.t()) => %{optional(String.t()) => [Context.capability()]}
        }
  @type t :: %__MODULE__{
          issuer: String.t(),
          api_audience: String.t() | nil,
          web_client_id: String.t() | nil,
          web_client_secret: String.t() | nil,
          web_client_auth_method: :none | :client_secret_basic,
          web_origin: String.t() | nil,
          web_redirect_uri: String.t() | nil,
          web_scopes: [String.t()] | nil,
          algorithms: [String.t()],
          clock_skew: non_neg_integer(),
          claim_capabilities: claim_capabilities(),
          discovery_max_age_ms: non_neg_integer(),
          jwks_max_age_ms: non_neg_integer(),
          refresh_failure_backoff_ms: pos_integer(),
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
    refresh_failure_backoff_ms: 1_000,
    connect_timeout_ms: 2_000,
    receive_timeout_ms: 5_000,
    request_timeout_ms: 10_000,
    max_body_bytes: 1_048_576,
    web_client_auth_method: :client_secret_basic,
    web_scopes: ["openid"],
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
    web_scopes = web_scopes(config, role)
    auth_method = config |> Keyword.fetch!(:web_client_auth_method) |> auth_method!()
    web_client_secret = client_secret(config, auth_method, role)
    algorithms = algorithms!(Keyword.fetch!(config, :algorithms))
    claim_capabilities = claim_capabilities!(Keyword.fetch!(config, :claim_capabilities))

    %__MODULE__{
      issuer: issuer,
      api_audience: api_audience,
      web_client_id: web_client_id,
      web_client_secret: web_client_secret,
      web_client_auth_method: auth_method,
      web_origin: web_origin,
      web_redirect_uri: web_redirect_uri,
      web_scopes: web_scopes,
      algorithms: algorithms,
      clock_skew: bounded_integer!(config, :clock_skew, "SMOLQUERY_OIDC_CLOCK_SKEW", 300),
      claim_capabilities: claim_capabilities,
      discovery_max_age_ms:
        bounded_integer!(
          config,
          :discovery_max_age_ms,
          "SMOLQUERY_OIDC_DISCOVERY_MAX_AGE_MS",
          86_400_000
        ),
      jwks_max_age_ms:
        bounded_integer!(config, :jwks_max_age_ms, "SMOLQUERY_OIDC_JWKS_MAX_AGE_MS", 86_400_000),
      refresh_failure_backoff_ms:
        positive_bounded_integer!(
          config,
          :refresh_failure_backoff_ms,
          "SMOLQUERY_OIDC_REFRESH_FAILURE_BACKOFF_MS",
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
    web_host = nonempty!(Keyword.get(config, :web_host), "SMOLQUERY_WEB_HOST")
    origin_uri = URI.parse(origin)
    redirect_uri = URI.parse(redirect)

    if origin_uri.path not in [nil, "", "/"] or origin_uri.query != nil or
         origin_uri.fragment != nil do
      invalid!("SMOLQUERY_OIDC_WEB_ORIGIN", origin, "an HTTPS origin without a path")
    end

    if origin_tuple(origin_uri) != origin_tuple(redirect_uri) or
         redirect_uri.path != "/auth/callback" or redirect_uri.query != nil do
      invalid!(
        "SMOLQUERY_OIDC_WEB_REDIRECT_URI",
        redirect,
        "the exact configured web origin with path /auth/callback and no query"
      )
    end

    if not same_host?(origin_uri.host, web_host) do
      invalid!(
        "SMOLQUERY_WEB_HOST",
        web_host,
        "the host from SMOLQUERY_OIDC_WEB_ORIGIN"
      )
    end

    {origin, redirect}
  end

  defp web_scopes(_config, :api), do: nil

  defp web_scopes(config, :web) do
    scopes = Keyword.fetch!(config, :web_scopes)

    valid =
      if is_list(scopes) do
        count = Enum.count_until(scopes, 33)

        count in 1..32 and MapSet.size(MapSet.new(scopes)) == count and "openid" in scopes and
          Enum.all?(scopes, &valid_scope?/1) and byte_size(Enum.join(scopes, " ")) <= 1_024
      else
        false
      end

    if valid,
      do: scopes,
      else:
        invalid!(
          "SMOLQUERY_OIDC_WEB_SCOPES",
          scopes,
          "a unique list of at most 32 OAuth scope tokens including openid"
        )
  end

  defp valid_scope?(scope) when is_binary(scope) and byte_size(scope) in 1..128 do
    Enum.all?(:binary.bin_to_list(scope), fn byte ->
      byte == 0x21 or byte in 0x23..0x5B or byte in 0x5D..0x7E
    end)
  end

  defp valid_scope?(_scope), do: false

  defp same_host?(left, right), do: String.downcase(left) == String.downcase(right)
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
