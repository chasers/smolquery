defmodule Smolquery.Auth.OIDC.Token do
  @moduledoc """
  Verifies OIDC access tokens for the API resource server.

  Header metadata is bounded and used only to select a key from the supervised
  provider cache. Signature verification is strict and all claims are checked
  before a normalized context is built. Verification failures are deliberately
  collapsed to `:error` at the adapter boundary.
  """

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.OIDC.{Config, Discovery, Provider}
  alias Smolquery.Auth.Principal

  @max_subject_bytes 4_096
  @display_claims ["name", "preferred_username"]
  @client_claims ["client_id", "azp"]

  @type result :: {:ok, Context.t()} | :error

  @doc "Verifies an access token through a supervised provider cache."
  @spec authenticate(String.t(), Config.t(), pid() | atom(), keyword()) :: result()
  def authenticate(token, config, provider, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:second))

    with {:ok, header} <- parse_header(token, config),
         :ok <- validate_header(header, config),
         {:ok, jwks} <- provider_jwks(provider),
         {:ok, jwk} <- select_or_refresh(header, jwks, provider, config),
         {:ok, claims} <- verify_claims(token, jwk, config, now),
         {:ok, context} <- normalize(claims, config) do
      {:ok, context}
    else
      _failure -> :error
    end
  end

  @doc "Verifies a browser ID token against the web client and one-time nonce."
  @spec authenticate_web(String.t(), Config.t(), pid() | atom(), String.t(), keyword()) ::
          result()
  def authenticate_web(token, config, provider, nonce, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:second))
    access_token = Keyword.get(opts, :access_token)
    code = Keyword.get(opts, :code)

    with {:ok, header} <- parse_header(token, config),
         :ok <- validate_header(header, config),
         {:ok, jwks} <- provider_jwks(provider),
         {:ok, jwk} <- select_or_refresh(header, jwks, provider, config),
         {:ok, claims} <- verify_signature(token, jwk, config),
         {:ok, claims} <- validate_web_claims(claims, config, nonce, now),
         :ok <- validate_hash_claims(claims, header, access_token, code),
         {:ok, context} <- normalize(claims, config) do
      {:ok, context}
    else
      _failure -> :error
    end
  end

  @doc "Verifies a token against supplied JWKS, primarily for deterministic tests."
  @spec verify(String.t(), Config.t(), map(), map(), keyword()) :: result()
  def verify(token, config, jwks, refreshed_jwks, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:second))

    with {:ok, header} <- parse_header(token, config),
         :ok <- validate_header(header, config),
         {:ok, jwk} <- select_or_refresh_with(header, jwks, refreshed_jwks, config),
         {:ok, claims} <- verify_claims(token, jwk, config, now),
         {:ok, context} <- normalize(claims, config) do
      {:ok, context}
    else
      _failure -> :error
    end
  end

  defp parse_header(token, %Config{} = config) when is_binary(token) do
    if byte_size(token) <= config.max_token_bytes do
      parse_segments(String.split(token, ".", trim: false), config.max_segment_bytes)
    else
      :error
    end
  end

  defp parse_header(_token, _config), do: :error

  defp parse_segments([header, payload, signature], max_segment_bytes)
       when byte_size(header) <= max_segment_bytes and
              byte_size(payload) <= max_segment_bytes and
              byte_size(signature) <= max_segment_bytes do
    case decode_segment(header) do
      {:ok, fields} when is_map(fields) -> {:ok, fields}
      _failure -> :error
    end
  end

  defp parse_segments(_segments, _max_segment_bytes), do: :error

  defp decode_segment(segment) do
    case Base.url_decode64(segment, padding: false) do
      {:ok, decoded} -> JSON.decode(decoded)
      :error -> :error
    end
  end

  defp validate_header(header, config) do
    with alg when is_binary(alg) <- Map.get(header, "alg"),
         true <- alg in config.algorithms,
         kid when is_binary(kid) <- Map.get(header, "kid"),
         true <- kid != "" and byte_size(kid) <= @max_subject_bytes,
         false <- Enum.any?(["jku", "jwk", "x5u", "crit", "b64"], &Map.has_key?(header, &1)),
         :ok <- validate_type(header, config) do
      :ok
    else
      _failure -> :error
    end
  end

  defp validate_type(header, %Config{typ_allowlist: []}) do
    case Map.fetch(header, "typ") do
      :error -> :ok
      {:ok, typ} when is_binary(typ) and typ != "" -> :ok
      _present -> :error
    end
  end

  defp validate_type(header, %Config{typ_allowlist: allowlist}),
    do: if(Map.get(header, "typ") in allowlist, do: :ok, else: :error)

  defp select_or_refresh(header, jwks, provider, config) do
    case select_key(header, jwks, config) do
      {:error, :unknown_kid} ->
        case provider_refresh_jwks(provider) do
          {:ok, refreshed} -> select_key(header, refreshed, config)
          _failure -> :error
        end

      result ->
        result
    end
  end

  defp provider_jwks(provider), do: provider_call(fn -> Provider.jwks(provider) end)

  defp provider_refresh_jwks(provider),
    do: provider_call(fn -> Provider.refresh_jwks(provider) end)

  defp provider_call(fun) do
    fun.()
  catch
    :exit, _reason -> :error
  end

  defp select_or_refresh_with(header, jwks, refreshed_jwks, config) do
    case select_key(header, jwks, config) do
      {:error, :unknown_kid} -> select_key(header, refreshed_jwks, config)
      result -> result
    end
  end

  defp select_key(%{"kid" => kid, "alg" => alg}, %{"keys" => keys}, config)
       when is_list(keys) do
    matching = Enum.filter(keys, &(is_map(&1) and Map.get(&1, "kid") == kid))

    case matching do
      [] -> {:error, :unknown_kid}
      [_key, _another | _rest] -> :error
      [key] -> compatible_key(key, alg, config)
    end
  end

  defp select_key(_header, _jwks, _config), do: :error

  defp compatible_key(key, alg, config) when is_map(key) do
    if valid_key_metadata?(key, alg, config) do
      jwk_from_map(key)
    else
      :error
    end
  end

  defp compatible_key(_key, _alg, _config), do: :error

  defp valid_key_metadata?(key, alg, config) do
    key_use = Map.get(key, "use")
    key_alg = Map.get(key, "alg")
    key_ops = Map.get(key, "key_ops")

    (is_nil(key_use) or key_use == "sig") and
      (is_nil(key_alg) or key_alg == alg) and
      (is_nil(key_ops) or (is_list(key_ops) and "verify" in key_ops)) and
      public_key_compatible?(key, alg) and alg in config.algorithms
  end

  defp public_key_compatible?(%{"kty" => "RSA"} = key, alg) do
    (String.starts_with?(alg, "RS") or String.starts_with?(alg, "PS")) and
      Discovery.validate_jwks(%{"keys" => [key]}) == :ok
  end

  defp public_key_compatible?(%{"kty" => "EC", "crv" => curve} = key, alg) do
    expected_curve = %{"ES256" => "P-256", "ES384" => "P-384", "ES512" => "P-521"}
    Map.get(expected_curve, alg) == curve and Discovery.validate_jwks(%{"keys" => [key]}) == :ok
  end

  defp public_key_compatible?(_key, _alg), do: false

  defp jwk_from_map(key) do
    {:ok, JOSE.JWK.from_map(key)}
  rescue
    ArgumentError -> :error
    FunctionClauseError -> :error
    BadMapError -> :error
    ErlangError -> :error
  end

  defp verify_claims(token, jwk, config, now) do
    case verify_signature(token, jwk, config) do
      {:ok, claims} -> validate_claims(claims, config, now)
      :error -> :error
    end
  end

  defp verify_signature(token, jwk, config) do
    case JOSE.JWT.verify_strict(jwk, config.algorithms, token) do
      {true, %JOSE.JWT{fields: claims}, _jws} when is_map(claims) -> {:ok, claims}
      _failure -> :error
    end
  end

  defp validate_web_claims(claims, config, nonce, now) do
    with :ok <-
           required_claims(claims, %{"iss" => [config.issuer], "sub" => [Map.get(claims, "sub")]}),
         :ok <- required_claims(claims, config.required_claims),
         :ok <- exact_issuer(claims, config.issuer),
         :ok <- valid_web_audience(claims, config.web_client_id),
         {:ok, _subject} <- required_string(claims, "sub"),
         {:ok, _expires_at} <- valid_expiry(claims, config.clock_skew, now),
         :ok <- valid_not_before(claims, config.clock_skew, now),
         :ok <- valid_web_issued_at(claims, config.iat_future_seconds, now),
         :ok <- valid_nonce(claims, nonce) do
      {:ok,
       Map.put(claims, "__expires_at", Map.fetch!(claims, "exp"))
       |> Map.put("__subject", Map.fetch!(claims, "sub"))}
    else
      _failure -> :error
    end
  end

  defp valid_web_audience(%{"aud" => audience} = claims, expected) when is_binary(audience) do
    if audience == expected and azp_valid?(claims, expected), do: :ok, else: :error
  end

  defp valid_web_audience(%{"aud" => audience} = claims, expected) when is_list(audience) do
    valid =
      audience != [] and Enum.all?(audience, &(is_binary(&1) and &1 != "")) and
        expected in audience

    authorized_party_valid? =
      case audience do
        [_single] -> azp_valid?(claims, expected)
        [_first, _second | _rest] -> Map.get(claims, "azp") == expected
      end

    if valid and authorized_party_valid?, do: :ok, else: :error
  end

  defp valid_web_audience(_claims, _expected), do: :error

  defp azp_valid?(claims, expected),
    do: is_nil(Map.get(claims, "azp")) or Map.get(claims, "azp") == expected

  defp valid_web_issued_at(%{"iat" => iat}, future, now)
       when is_integer(iat) and iat >= 0,
       do: if(iat <= now + future, do: :ok, else: :error)

  defp valid_web_issued_at(_claims, _future, _now), do: :error

  defp valid_nonce(%{"nonce" => nonce}, expected) when is_binary(nonce) and is_binary(expected) do
    if byte_size(nonce) == byte_size(expected) and :crypto.hash_equals(nonce, expected),
      do: :ok,
      else: :error
  end

  defp valid_nonce(_claims, _expected), do: :error

  defp validate_hash_claims(claims, header, access_token, code) do
    case validate_one_hash(claims, "at_hash", access_token, header) do
      :ok -> validate_one_hash(claims, "c_hash", code, header)
      :error -> :error
    end
  end

  defp validate_one_hash(claims, claim, value, header) do
    case Map.fetch(claims, claim) do
      :error ->
        :ok

      {:ok, expected} when is_binary(expected) and is_binary(value) ->
        verify_hash(expected, value, header["alg"])

      _ ->
        :error
    end
  end

  defp verify_hash(expected, value, alg) do
    with {:ok, algorithm} <- hash_algorithm(alg),
         digest <- :crypto.hash(algorithm, value),
         half <- binary_part(digest, 0, div(byte_size(digest), 2)),
         actual <- Base.url_encode64(half, padding: false),
         true <- byte_size(actual) == byte_size(expected),
         true <- :crypto.hash_equals(actual, expected) do
      :ok
    else
      _ -> :error
    end
  end

  defp hash_algorithm(alg) when alg in ["RS256", "PS256", "ES256"], do: {:ok, :sha256}
  defp hash_algorithm(alg) when alg in ["RS384", "PS384", "ES384"], do: {:ok, :sha384}
  defp hash_algorithm(alg) when alg in ["RS512", "PS512", "ES512"], do: {:ok, :sha512}
  defp hash_algorithm(_alg), do: :error

  defp validate_claims(claims, config, now) do
    with :ok <- required_claims(claims, config.required_claims),
         :ok <- exact_issuer(claims, config.issuer),
         :ok <- valid_audience(claims, config.api_audience),
         {:ok, subject} <- required_string(claims, "sub"),
         {:ok, expires_at} <- valid_expiry(claims, config.clock_skew, now),
         :ok <- valid_not_before(claims, config.clock_skew, now),
         :ok <- valid_issued_at(claims, config.iat_future_seconds, now) do
      {:ok, Map.put(claims, "__expires_at", expires_at) |> Map.put("__subject", subject)}
    else
      _failure -> :error
    end
  end

  defp required_claims(claims, required) do
    if Enum.all?(required, fn {claim, values} ->
         claim_value_matches?(Map.get(claims, claim), values)
       end) do
      :ok
    else
      :error
    end
  end

  defp claim_value_matches?(value, allowed) when is_binary(value), do: value in allowed

  defp claim_value_matches?(values, allowed) when is_list(values) do
    {all_strings, matched} =
      Enum.reduce(values, {values != [], false}, fn value, {all_strings, matched} ->
        {all_strings and is_binary(value) and value != "", matched or value in allowed}
      end)

    all_strings and matched
  end

  defp claim_value_matches?(_value, _allowed), do: false

  defp exact_issuer(%{"iss" => issuer}, expected) when issuer == expected, do: :ok
  defp exact_issuer(_claims, _expected), do: :error

  defp valid_audience(%{"aud" => audience}, expected) when is_binary(audience),
    do: if(audience == expected, do: :ok, else: :error)

  defp valid_audience(%{"aud" => audience}, expected) when is_list(audience) do
    {all_strings, matched} =
      Enum.reduce(audience, {audience != [], false}, fn value, {all_strings, matched} ->
        {all_strings and is_binary(value) and value != "", matched or value == expected}
      end)

    if all_strings and matched, do: :ok, else: :error
  end

  defp valid_audience(_claims, _expected), do: :error

  defp required_string(claims, key) do
    case Map.get(claims, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= @max_subject_bytes ->
        {:ok, value}

      _value ->
        :error
    end
  end

  defp valid_expiry(%{"exp" => exp}, skew, now) when is_integer(exp) and exp >= 0 do
    if now < exp + skew, do: {:ok, exp + skew}, else: :error
  end

  defp valid_expiry(_claims, _skew, _now), do: :error

  defp valid_not_before(%{"nbf" => nbf}, skew, now) when is_integer(nbf) and nbf >= 0 do
    if nbf <= now + skew, do: :ok, else: :error
  end

  defp valid_not_before(%{"nbf" => _nbf}, _skew, _now), do: :error
  defp valid_not_before(_claims, _skew, _now), do: :ok

  defp valid_issued_at(%{"iat" => iat}, future, now) when is_integer(iat) and iat >= 0 do
    if iat <= now + future, do: :ok, else: :error
  end

  defp valid_issued_at(%{"iat" => _iat}, _future, _now), do: :error
  defp valid_issued_at(_claims, _future, _now), do: :ok

  defp normalize(claims, config) do
    capabilities = mapped_capabilities(claims, config.claim_capabilities)
    subject = Map.fetch!(claims, "__subject")
    expires_at = Map.fetch!(claims, "__expires_at")

    with {:ok, principal} <-
           Principal.oidc(config.issuer, subject, :user, principal_options(claims)),
         {:ok, context} <-
           Context.single_tenant(principal, MapSet.to_list(capabilities), expires_at: expires_at) do
      {:ok, context}
    else
      _failure -> :error
    end
  end

  defp mapped_capabilities(claims, mappings) do
    Enum.reduce(mappings, MapSet.new(), fn {claim, values}, capabilities ->
      map_claim_values(Map.get(claims, claim), values, capabilities)
    end)
  end

  defp map_claim_values(value, values, capabilities) when is_binary(value),
    do: union_capabilities(capabilities, Map.get(values, value, []))

  defp map_claim_values(values, mapping, capabilities) when is_list(values) do
    if values != [] and Enum.all?(values, &(is_binary(&1) and &1 != "")) do
      Enum.reduce(values, capabilities, fn value, acc ->
        union_capabilities(acc, Map.get(mapping, value, []))
      end)
    else
      capabilities
    end
  end

  defp map_claim_values(_value, _mapping, capabilities), do: capabilities

  defp union_capabilities(capabilities, values),
    do: Enum.reduce(values, capabilities, &MapSet.put(&2, &1))

  defp principal_options(claims) do
    display_name = Enum.find_value(@display_claims, &string_claim(claims, &1))
    client_id = Enum.find_value(@client_claims, &string_claim(claims, &1))
    [display_name: display_name, client_id: client_id]
  end

  defp string_claim(claims, key) do
    case Map.get(claims, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= @max_subject_bytes ->
        value

      _value ->
        nil
    end
  end
end
