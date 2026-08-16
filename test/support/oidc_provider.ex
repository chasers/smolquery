defmodule Smolquery.Test.OIDCProvider do
  @moduledoc false

  @private_key JOSE.JWK.generate_key({:rsa, 2048})
  @public_key @private_key
              |> JOSE.JWK.to_public()
              |> JOSE.JWK.to_map()
              |> elem(1)
              |> Map.put("kid", "supervisor-test")
  @metadata %{
    "issuer" => "https://issuer.example",
    "authorization_endpoint" => "https://issuer.example/authorize",
    "token_endpoint" => "https://issuer.example/token",
    "jwks_uri" => "https://issuer.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }
  @jwks %{"keys" => [@public_key]}

  def client do
    fn
      "https://issuer.example/.well-known/openid-configuration", _options ->
        {:ok, response(@metadata)}

      "https://issuer.example/keys", _options ->
        {:ok, response(@jwks)}
    end
  end

  def failing_client, do: fn _url, _options -> {:error, :provider_unavailable} end

  defp response(body) do
    %Req.Response{
      status: 200,
      headers: %{"content-type" => ["application/json"]},
      body: JSON.encode!(body)
    }
  end
end
