defmodule Smolquery.Auth.OIDC.Discovery do
  @moduledoc """
  Fetches and validates issuer discovery metadata and JWKS documents.

  HTTP failures, malformed responses, insecure endpoints, redirects, and
  oversized bodies are errors. Callers must never treat stale or missing
  metadata as an authorization success.
  """

  alias Smolquery.Auth.OIDC.Config

  @type http_client :: (String.t(), keyword() -> {:ok, Req.Response.t()} | {:error, term()})

  @doc "Fetches issuer discovery metadata through Req or an injected client."
  @spec fetch(Config.t(), http_client()) :: {:ok, map()} | {:error, term()}
  def fetch(%Config{} = config, client \\ &fetch_request/2) do
    url = discovery_url(config.issuer)

    with {:ok, body} <- get_json(url, config, client),
         :ok <- validate_metadata(body, config) do
      {:ok, body}
    end
  end

  @doc "Fetches and validates the configured provider JWKS document."
  @spec fetch_jwks(Config.t(), map(), http_client()) :: {:ok, map()} | {:error, term()}
  def fetch_jwks(%Config{} = config, metadata, client \\ &fetch_request/2)
      when is_map(metadata) do
    with {:ok, url} <- fetch_https_endpoint(metadata, "jwks_uri"),
         {:ok, body} <- get_json(url, config, client),
         :ok <- validate_jwks(body, config) do
      {:ok, body}
    end
  end

  @doc "Validates discovery metadata against the configured issuer and algorithms."
  @spec validate_metadata(map(), Config.t()) :: :ok | {:error, term()}
  def validate_metadata(metadata, %Config{} = config) when is_map(metadata) do
    with {:ok, issuer} <- string_field(metadata, "issuer"),
         :ok <- same_issuer?(issuer, config.issuer),
         {:ok, _authorization} <- fetch_https_endpoint(metadata, "authorization_endpoint"),
         {:ok, _token} <- fetch_https_endpoint(metadata, "token_endpoint"),
         {:ok, _jwks} <- fetch_https_endpoint(metadata, "jwks_uri") do
      algorithms_supported?(metadata, config.algorithms)
    end
  end

  def validate_metadata(_metadata, _config), do: {:error, :discovery_not_object}

  @doc "Validates the supported public JWKS key shapes."
  @spec validate_jwks(map()) :: :ok | {:error, term()}
  def validate_jwks(%{"keys" => keys}) when is_list(keys) and keys != [] do
    if Enum.all?(keys, &public_jwk_shape?/1), do: :ok, else: {:error, :jwks_malformed}
  end

  def validate_jwks(_jwks), do: {:error, :jwks_malformed}

  @doc "Validates that a JWKS contains a key usable by the local algorithm policy."
  @spec validate_jwks(map(), Config.t()) :: :ok | {:error, term()}
  def validate_jwks(%{"keys" => keys} = jwks, %Config{} = config) do
    with :ok <- validate_jwks(jwks),
         :ok <- unique_key_ids(keys),
         true <- Enum.any?(keys, &usable_signing_key?(&1, config.algorithms)) do
      :ok
    else
      false -> {:error, :jwks_no_compatible_signing_key}
      error -> error
    end
  end

  def validate_jwks(_jwks, %Config{}), do: {:error, :jwks_malformed}

  defp discovery_url(issuer),
    do:
      issuer <>
        if(String.ends_with?(issuer, "/"), do: "", else: "/") <>
        ".well-known/openid-configuration"

  defp get_json(url, config, client) do
    options = [
      connect_options: [timeout: config.connect_timeout_ms],
      receive_timeout: config.receive_timeout_ms,
      request_timeout: config.request_timeout_ms,
      max_body_bytes: config.max_body_bytes,
      redirect: false
    ]

    case client.(url, options) do
      {:ok, %Req.Response{status: status} = response} when status in 200..299 ->
        validate_response(response, config.max_body_bytes)

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_status, status}}

      {:error, reason} ->
        {:error, {:http_error, reason}}

      _other ->
        {:error, :invalid_http_response}
    end
  end

  @doc false
  @spec fetch_request(String.t(), keyword()) :: {:ok, Req.Response.t()} | {:error, term()}
  def fetch_request(url, options) do
    key = make_ref()
    max_body_bytes = Keyword.fetch!(options, :max_body_bytes)
    Process.put({__MODULE__, key}, {0, []})

    callback = fn {:data, data}, {req, response} ->
      with :ok <- content_length_status(response, max_body_bytes),
           :ok <- accumulate_chunk(key, data, max_body_bytes) do
        {:cont, {req, response}}
      else
        {:error, reason} ->
          Process.put({__MODULE__, {key, :error}}, reason)
          {:halt, {req, response}}
      end
    end

    result =
      try do
        request_result =
          Req.get(url,
            retry: false,
            raw: true,
            decode_body: false,
            redirect: false,
            connect_options: Keyword.fetch!(options, :connect_options),
            receive_timeout: Keyword.fetch!(options, :receive_timeout),
            request_timeout: Keyword.fetch!(options, :request_timeout),
            into: callback,
            headers: [{"accept", "application/json, application/jwk-set+json"}]
          )

        {request_result, Process.get({__MODULE__, key}, {0, []}),
         Process.get({__MODULE__, {key, :error}})}
      after
        Process.delete({__MODULE__, key})
        Process.delete({__MODULE__, {key, :error}})
      end

    case result do
      {{:ok, response}, {_size, chunks}, error} when is_struct(response, Req.Response) ->
        {:ok, %{response | body: error || chunks |> Enum.reverse() |> IO.iodata_to_binary()}}

      {{:error, reason}, _body, _error} ->
        {:error, reason}
    end
  end

  defp accumulate_chunk(key, data, max_body_bytes) when is_binary(data) do
    {size, chunks} = Process.get({__MODULE__, key}, {0, []})

    if size + byte_size(data) <= max_body_bytes do
      Process.put({__MODULE__, key}, {size + byte_size(data), [data | chunks]})
      :ok
    else
      {:error, :response_too_large}
    end
  end

  defp accumulate_chunk(_key, _data, _max_body_bytes), do: {:error, :response_too_large}

  defp validate_response(response, max_body_bytes) do
    content_type = Req.Response.get_header(response, "content-type")

    cond do
      not json_content_type?(content_type) ->
        {:error, :content_type_invalid}

      response.body == :response_too_large ->
        {:error, :response_too_large}

      response.body in [:content_length_invalid, :content_length_conflict] ->
        {:error, response.body}

      not is_binary(response.body) ->
        {:error, :response_body_not_binary}

      byte_size(response.body) > max_body_bytes ->
        {:error, :response_too_large}

      true ->
        decode_json(response.body)
    end
  end

  defp content_length_status(response, max_body_bytes) do
    values = Req.Response.get_header(response, "content-length")

    parsed =
      values
      |> Enum.flat_map(&String.split(&1, ","))
      |> Enum.map(&parse_content_length/1)

    cond do
      parsed == [] -> :ok
      Enum.any?(parsed, &is_nil/1) -> {:error, :content_length_invalid}
      Enum.uniq(parsed) |> length() > 1 -> {:error, :content_length_conflict}
      hd(parsed) > max_body_bytes -> {:error, :response_too_large}
      true -> :ok
    end
  end

  defp parse_content_length(value) do
    value = String.trim(value)

    case Integer.parse(value) do
      {length, ""} when length >= 0 -> length
      _ -> nil
    end
  end

  defp decode_json(body) do
    case JSON.decode(body) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, _decoded} -> {:error, :response_not_object}
      {:error, _reason} -> {:error, :response_invalid_json}
    end
  end

  defp json_content_type?(values) do
    Enum.any?(values, fn value ->
      value =
        value
        |> String.split(";", parts: 2)
        |> hd()
        |> String.trim()
        |> String.downcase()

      value in ["application/json", "application/jwk-set+json"]
    end)
  end

  defp string_field(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:metadata_field_invalid, key}}
    end
  end

  defp same_issuer?(issuer, configured),
    do: if(issuer == configured, do: :ok, else: {:error, :issuer_mismatch})

  defp fetch_https_endpoint(metadata, key) do
    with {:ok, value} <- string_field(metadata, key),
         uri = URI.parse(value),
         true <-
           uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
             is_nil(uri.userinfo) and is_nil(uri.fragment) do
      {:ok, value}
    else
      _ -> {:error, {:endpoint_invalid, key}}
    end
  end

  defp algorithms_supported?(metadata, allowed) do
    case Map.get(metadata, "id_token_signing_alg_values_supported") do
      values when is_list(values) ->
        if Enum.any?(allowed, &(&1 in values)), do: :ok, else: {:error, :algorithm_unsupported}

      _ ->
        {:error, :algorithms_missing}
    end
  end

  defp public_jwk_shape?(%{"kid" => kid, "kty" => "RSA", "n" => n, "e" => e} = key)
       when is_binary(kid) and kid != "" and is_binary(n) and is_binary(e) do
    no_private_fields?(key) and strong_rsa_public_key?(n, e)
  end

  defp public_jwk_shape?(%{"kid" => kid, "kty" => "EC", "crv" => curve, "x" => x, "y" => y} = key)
       when is_binary(kid) and kid != "" and curve in ["P-256", "P-384", "P-521"] and is_binary(x) and
              is_binary(y) do
    no_private_fields?(key) and valid_ec_coordinate?(curve, x) and valid_ec_coordinate?(curve, y)
  end

  defp public_jwk_shape?(%{"kid" => kid, "kty" => "OKP", "crv" => curve, "x" => x} = key)
       when is_binary(kid) and kid != "" and curve in ["Ed25519", "Ed448"] and is_binary(x) do
    no_private_fields?(key) and valid_b64url?(x)
  end

  defp public_jwk_shape?(_key), do: false

  defp usable_signing_key?(key, algorithms) when is_map(key) do
    key_use = Map.get(key, "use")
    key_alg = Map.get(key, "alg")
    key_ops = Map.get(key, "key_ops")

    (is_nil(key_use) or key_use == "sig") and
      (is_nil(key_ops) or (is_list(key_ops) and "verify" in key_ops)) and
      key_algorithm_compatible?(key, key_alg, algorithms)
  end

  defp usable_signing_key?(_key, _algorithms), do: false

  defp unique_key_ids(keys) do
    key_ids = Enum.map(keys, &Map.fetch!(&1, "kid"))
    if length(key_ids) == length(Enum.uniq(key_ids)), do: :ok, else: {:error, :jwks_duplicate_kid}
  end

  defp key_algorithm_compatible?(key, nil, algorithms),
    do: Enum.any?(algorithms, &key_supports_algorithm?(key, &1))

  defp key_algorithm_compatible?(key, algorithm, algorithms),
    do: algorithm in algorithms and key_supports_algorithm?(key, algorithm)

  defp key_supports_algorithm?(%{"kty" => "RSA"}, algorithm),
    do: String.starts_with?(algorithm, "RS") or String.starts_with?(algorithm, "PS")

  defp key_supports_algorithm?(%{"kty" => "EC", "crv" => curve}, algorithm) do
    Map.get(%{"ES256" => "P-256", "ES384" => "P-384", "ES512" => "P-521"}, algorithm) ==
      curve
  end

  defp key_supports_algorithm?(_key, _algorithm), do: false

  defp no_private_fields?(key),
    do: Enum.all?(~w(d p q dp dq qi oth k), &(not Map.has_key?(key, &1)))

  defp strong_rsa_public_key?(modulus, exponent) do
    with {:ok, modulus_bytes} <- canonical_b64url(modulus),
         {:ok, exponent_bytes} <- canonical_b64url(exponent),
         true <- rsa_modulus_at_least_2048_bits?(modulus_bytes),
         exponent <- :binary.decode_unsigned(exponent_bytes),
         true <- exponent >= 3 and rem(exponent, 2) == 1 do
      true
    else
      _invalid -> false
    end
  end

  defp rsa_modulus_at_least_2048_bits?(<<0, _rest::binary>>), do: false

  defp rsa_modulus_at_least_2048_bits?(<<first, _rest::binary>> = modulus) do
    byte_size(modulus) > 256 or (byte_size(modulus) == 256 and first >= 128)
  end

  defp rsa_modulus_at_least_2048_bits?(_modulus), do: false

  defp valid_ec_coordinate?(curve, coordinate) do
    expected_bytes = %{"P-256" => 32, "P-384" => 48, "P-521" => 66}

    case canonical_b64url(coordinate) do
      {:ok, decoded} -> byte_size(decoded) == Map.fetch!(expected_bytes, curve)
      :error -> false
    end
  end

  defp valid_b64url?(value), do: match?({:ok, _decoded}, canonical_b64url(value))

  defp canonical_b64url(value) when is_binary(value) and value != "" do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} ->
        if decoded != "" and Base.url_encode64(decoded, padding: false) == value,
          do: {:ok, decoded},
          else: :error

      :error ->
        :error
    end
  end

  defp canonical_b64url(_value), do: :error
end
