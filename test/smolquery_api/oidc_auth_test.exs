defmodule SmolqueryApi.OIDCAuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Auth
  alias Smolquery.Auth.OIDC.Provider
  alias Smolquery.QueryService
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
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

  test "rejects query-only OIDC tokens before parsing catalog writes" do
    name = start_api()
    token = token(%{"scope" => "query"})

    response =
      request(
        name,
        conn(:post, "/v1/datasets", "not-json")
        |> put_req_header("content-type", "application/json")
        |> bearer(token)
      )

    assert response.status == 403
    assert JSON.decode!(response.resp_body)["error"]["status"] == "PERMISSION_DENIED"
    assert %Plug.Conn.Unfetched{aspect: :body_params} = response.body_params

    oversized =
      request(
        name,
        conn(:post, "/v1/datasets", String.duplicate("x", 2_000_000))
        |> put_req_header("content-type", "application/json")
        |> bearer(token)
      )

    assert oversized.status == 403
    assert %Plug.Conn.Unfetched{aspect: :body_params} = oversized.body_params
  end

  test "applies the closed capability matrix before parsers" do
    routes = %{
      query: [
        {:get, "/v1/datasets"},
        {:get, "/v1/datasets/missing/tables"},
        {:get, "/v1/datasets/missing/tables/missing"},
        {:post, "/v1/queries"},
        {:post, "/v1/jobs"},
        {:get, "/v1/jobs/missing"},
        {:get, "/v1/jobs/missing/results"},
        {:delete, "/v1/jobs/missing"}
      ],
      ingest: [
        {:post, "/v1/datasets/missing/tables/missing/insert"},
        {:post, "/v1/datasets/missing/tables/missing/load"}
      ],
      catalog_manage: [
        {:post, "/v1/datasets"},
        {:post, "/v1/datasets/missing/tables"},
        {:patch, "/v1/datasets/missing/tables/missing"}
      ]
    }

    matrix =
      for {route_capability, route_list} <- routes,
          {method, path} <- route_list,
          do: {route_capability, method, path}

    for {capability, _routes} <- routes do
      name = start_api()

      for {route_capability, method, path} <- matrix do
        response =
          request(
            name,
            route_request(method, path, token(%{"scope" => scope_value(capability)}))
          )

        if capability == route_capability do
          assert response.status not in [401, 403],
                 "#{capability} token denied #{method} #{path}: #{response.status}"
        else
          assert response.status == 403
          assert %Plug.Conn.Unfetched{aspect: :body_params} = response.body_params
        end
      end
    end
  end

  defp scope_value(:catalog_manage), do: "catalog"
  defp scope_value(capability), do: Atom.to_string(capability)

  defp route_request(method, path, token) do
    body = if method == :get or method == :delete, do: "", else: "not-json"

    conn(method, path, body)
    |> put_req_header("content-type", "application/json")
    |> bearer(token)
  end

  defp start_api do
    name = :"api_oidc_#{System.unique_integer([:positive])}"
    query_name = :"api_oidc_query_#{System.unique_integer([:positive])}"

    {:ok, _query} =
      QueryService.Supervisor.start_link(
        name: query_name,
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})
      )

    runtime =
      Runtime.new(
        name: name,
        auth_mode: :oidc,
        catalog: MapCatalog.new(),
        query_name: query_name,
        oidc: [
          issuer: "https://issuer.example",
          api_audience: "smolquery-api",
          web_client_id: "smolquery-web",
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
      QueryService.Runtime.delete(query_name)
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
