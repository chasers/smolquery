defmodule SmolqueryApi.OIDCAuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Auth
  alias Smolquery.Auth.OIDC.Provider
  alias Smolquery.Test.ApiEndpoint
  alias SmolqueryApi.Runtime

  @private_key JOSE.JWK.generate_key({:rsa, 2048})
  @public_key JOSE.JWK.to_map(JOSE.JWK.to_public(@private_key))
              |> elem(1)
              |> Map.put("kid", "one")

  test "authenticates OIDC requests before parsing and assigns a normalized context" do
    name = start_api()
    token = token(%{"scope" => ["query", "ingest", "catalog"]})

    response = request(name, conn(:get, "/v1/no/such/route") |> bearer(token))

    assert response.status == 404
    assert {:ok, context} = Auth.fetch_context(response)
    assert context.principal.authn == :oidc
    assert MapSet.equal?(context.capabilities, MapSet.new([:query, :ingest, :catalog_manage]))
    refute inspect(context) =~ token
  end

  test "rejects query-only OIDC tokens and obscures route existence without parsing bodies" do
    name = start_api()
    token = token(%{"scope" => "query"})

    real = request(name, conn(:post, "/v1/datasets", "not-json") |> bearer(token))
    absent = request(name, conn(:post, "/v1/no/such/route", "not-json") |> bearer(token))

    assert real.status == 401
    assert real.resp_body == absent.resp_body
    assert %Plug.Conn.Unfetched{aspect: :body_params} = real.body_params
    assert %Plug.Conn.Unfetched{aspect: :body_params} = absent.body_params
  end

  defp start_api do
    name = :"api_oidc_#{System.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        name: name,
        auth_mode: :oidc,
        oidc: [
          issuer: "https://issuer.example",
          api_audience: "smolquery-api",
          claim_capabilities: %{
            "scope" => %{
              "query" => [:query],
              "ingest" => [:ingest],
              "catalog" => [:catalog_manage]
            }
          }
        ]
      )

    client = fn url, _options -> {:ok, response_for(url)} end
    provider_name = Module.concat(name, "OIDCProvider")

    {:ok, provider} =
      Provider.start_link(name: provider_name, config: runtime.oidc, http_client: client)

    Runtime.put(runtime)

    on_exit(fn ->
      Runtime.delete(name)
      if Process.alive?(provider), do: GenServer.stop(provider)
    end)

    name
  end

  defp request(name, conn), do: ApiEndpoint.request(name, conn)
  defp bearer(conn, token), do: put_req_header(conn, "authorization", "Bearer #{token}")

  defp token(extra_claims) do
    claims =
      Map.merge(
        %{
          "iss" => "https://issuer.example",
          "aud" => "smolquery-api",
          "sub" => "subject-1",
          "exp" => System.system_time(:second) + 100
        },
        extra_claims
      )

    jws = JOSE.JWS.from_map(%{"alg" => "RS256", "kid" => "one"})
    JOSE.JWT.sign(@private_key, jws, claims) |> JOSE.JWS.compact() |> elem(1)
  end

  defp response_for("https://issuer.example/.well-known/openid-configuration") do
    response(%{
      "issuer" => "https://issuer.example",
      "authorization_endpoint" => "https://issuer.example/authorize",
      "token_endpoint" => "https://issuer.example/token",
      "jwks_uri" => "https://issuer.example/keys",
      "id_token_signing_alg_values_supported" => ["RS256"]
    })
  end

  defp response_for("https://issuer.example/keys"),
    do: response(%{"keys" => [@public_key]})

  defp response(body),
    do: %Req.Response{
      status: 200,
      headers: %{"content-type" => ["application/json"]},
      body: JSON.encode!(body)
    }
end
