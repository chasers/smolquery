defmodule Smolquery.Auth.OIDC.DiscoveryTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.OIDC.{Config, Discovery}

  @config Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)
  @rsa_public JOSE.JWK.generate_key({:rsa, 2048})
              |> JOSE.JWK.to_public()
              |> JOSE.JWK.to_map()
              |> elem(1)
              |> Map.put("kid", "one")
  @ec_public JOSE.JWK.generate_key({:ec, "P-256"})
             |> JOSE.JWK.to_public()
             |> JOSE.JWK.to_map()
             |> elem(1)
             |> Map.put("kid", "ec-one")
  @metadata %{
    "issuer" => "https://issuer.example/",
    "authorization_endpoint" => "https://login.example/authorize",
    "token_endpoint" => "https://login.example/token",
    "jwks_uri" => "https://keys.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }

  test "fetches and validates exact issuer and HTTPS metadata" do
    client = fn url, _options ->
      assert url == "https://issuer.example/.well-known/openid-configuration"
      {:ok, response(@metadata)}
    end

    assert {:ok, @metadata} = Discovery.fetch(@config, client)
  end

  test "rejects issuer mismatch, insecure endpoints, and unsupported algorithms" do
    for change <- [
          {:issuer, "https://issuer.example"},
          {:authorization_endpoint, "http://login.example/authorize"},
          {:id_token_signing_alg_values_supported, ["HS256"]}
        ] do
      metadata = Map.put(@metadata, Atom.to_string(elem(change, 0)), elem(change, 1))
      assert {:error, _reason} = Discovery.validate_metadata(metadata, @config)
    end
  end

  test "accepts JSON media type variants and rejects oversized injected responses" do
    assert {:ok, _} =
             Discovery.fetch(@config, fn _, _ ->
               {:ok, response(@metadata, 200, "Application/JSON; charset=utf-8")}
             end)

    jwks = %{"keys" => [@rsa_public]}

    assert {:ok, _} =
             Discovery.fetch_jwks(@config, @metadata, fn _, _ ->
               {:ok, response(jwks, 200, "application/jwk-set+json")}
             end)

    small =
      Config.new(
        [oidc: [issuer: "https://issuer.example/", api_audience: "api", max_body_bytes: 3]],
        :api
      )

    assert {:error, :response_too_large} =
             Discovery.fetch(small, fn _, _ -> {:ok, response(@metadata)} end)
  end

  test "real Req transport refuses redirects and halts oversized streams" do
    redirect_port =
      local_server(
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
      )

    assert {:ok, %Req.Response{status: 302}} =
             Discovery.fetch_request("http://127.0.0.1:#{redirect_port}/", request_options(1024))

    oversize_port =
      local_server(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10\r\nConnection: close\r\n\r\n1234567890"
      )

    assert {:ok, %Req.Response{body: :response_too_large}} =
             Discovery.fetch_request("http://127.0.0.1:#{oversize_port}/", request_options(4))

    chunked_port =
      local_server(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n{\"a\"\r\n3\r\n:1}\r\n0\r\n\r\n"
      )

    assert {:ok, %Req.Response{body: "{\"a\":1}"}} =
             Discovery.fetch_request("http://127.0.0.1:#{chunked_port}/", request_options(32))
  end

  test "real Req transport enforces the total request timeout" do
    port =
      local_server(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
        200
      )

    assert {:error, %Req.TransportError{reason: :timeout}} =
             Discovery.fetch_request("http://127.0.0.1:#{port}/", request_options(1024, 20))
  end

  test "requires a signing key compatible with the configured algorithms" do
    assert :ok = Discovery.validate_jwks(%{"keys" => [@rsa_public]}, @config)

    assert {:error, :jwks_no_compatible_signing_key} =
             Discovery.validate_jwks(%{"keys" => [@ec_public]}, @config)

    assert {:error, :jwks_no_compatible_signing_key} =
             Discovery.validate_jwks(%{"keys" => [Map.put(@rsa_public, "use", "enc")]}, @config)

    mixed_config = %{@config | algorithms: ["RS256", "ES256"]}

    assert {:error, :jwks_no_compatible_signing_key} =
             Discovery.validate_jwks(
               %{"keys" => [Map.put(@rsa_public, "alg", "ES256")]},
               mixed_config
             )

    assert {:error, :jwks_duplicate_kid} =
             Discovery.validate_jwks(%{"keys" => [@rsa_public, @rsa_public]}, @config)
  end

  test "rejects malformed and private JWKS keys" do
    assert :ok == Discovery.validate_jwks(%{"keys" => [@rsa_public]})
    assert :ok == Discovery.validate_jwks(%{"keys" => [@ec_public]})

    assert {:error, :jwks_malformed} =
             Discovery.validate_jwks(%{
               "keys" => [%{"kid" => "weak", "kty" => "RSA", "n" => "AQ", "e" => "AQAB"}]
             })

    padded_weak_modulus = Base.url_encode64(<<0, 1::size(2048)>>, padding: false)

    assert {:error, :jwks_malformed} =
             Discovery.validate_jwks(%{
               "keys" => [
                 %{
                   "kid" => "padded-weak",
                   "kty" => "RSA",
                   "n" => padded_weak_modulus,
                   "e" => "AQAB"
                 }
               ]
             })

    assert {:error, :jwks_malformed} =
             Discovery.validate_jwks(%{
               "keys" => [%{"kid" => "one", "kty" => "oct", "k" => "AQ"}]
             })

    assert {:error, :jwks_malformed} =
             Discovery.validate_jwks(%{"keys" => [Map.put(@rsa_public, "d", "private")]})

    assert {:error, :jwks_malformed} =
             Discovery.validate_jwks(%{"keys" => [Map.put(@rsa_public, "oth", [])]})
  end

  defp response(body, status \\ 200, content_type \\ "application/json") do
    %Req.Response{
      status: status,
      headers: %{"content-type" => [content_type]},
      body: JSON.encode!(body)
    }
  end

  defp request_options(max_body_bytes, request_timeout \\ 500) do
    [
      max_body_bytes: max_body_bytes,
      connect_options: [timeout: 500],
      receive_timeout: 500,
      request_timeout: request_timeout,
      redirect: false
    ]
  end

  defp local_server(response, delay \\ 0) do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(socket)

    spawn(fn ->
      {:ok, client} = :gen_tcp.accept(socket)
      :gen_tcp.recv(client, 0, 1_000)
      if delay > 0, do: Process.sleep(delay)
      :gen_tcp.send(client, response)
      :gen_tcp.close(client)
      :gen_tcp.close(socket)
    end)

    port
  end
end
