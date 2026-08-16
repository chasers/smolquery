defmodule SmolqueryApi.RouterTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Auth
  alias Smolquery.Test.ApiEndpoint
  alias SmolqueryApi.Runtime

  @key "test-api-key"

  defp start_api(opts \\ []) do
    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(Keyword.merge([name: name, api_key: @key], opts))
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp request(name, conn), do: ApiEndpoint.request(name, conn)

  defp authorized(conn, key \\ @key), do: put_req_header(conn, "authorization", "Bearer #{key}")

  describe "healthz" do
    test "answers without credentials" do
      name = start_api()

      response = request(name, conn(:get, "/healthz"))

      assert response.status == 200
      assert JSON.decode!(response.resp_body) == %{"status" => "ok"}
    end

    test "POST healthz requires auth and does not parse valid or malformed bodies" do
      name = start_api()
      valid = request(name, conn(:post, "/healthz", "{}"))
      malformed = request(name, conn(:post, "/healthz", "not-json"))

      assert valid.status == 401
      assert malformed.status == 401
      assert valid.resp_body == malformed.resp_body
      assert %Plug.Conn.Unfetched{aspect: :body_params} = valid.body_params
      assert %Plug.Conn.Unfetched{aspect: :body_params} = malformed.body_params
    end
  end

  describe "metrics" do
    test "answers Prometheus text to the internal secret" do
      name = start_api()

      response =
        request(
          name,
          conn(:get, "/metrics")
          |> put_req_header(
            Smolquery.InternalSecret.header(),
            Smolquery.InternalSecret.value()
          )
        )

      assert response.status == 200

      assert {"content-type", "text/plain" <> _charset} =
               List.keyfind(response.resp_headers, "content-type", 0)
    end

    test "refuses a scrape without the secret, and one holding only the API key" do
      name = start_api()

      assert request(name, conn(:get, "/metrics")).status == 401
      assert request(name, authorized(conn(:get, "/metrics"))).status == 401

      wrong =
        conn(:get, "/metrics")
        |> put_req_header(Smolquery.InternalSecret.header(), "not-the-secret")

      assert request(name, wrong).status == 401
    end
  end

  describe "auth" do
    test "no authorization header is a 401 in the error envelope" do
      name = start_api()

      response = request(name, conn(:get, "/v1/datasets"))

      assert response.status == 401
      assert response.halted

      assert %{"error" => %{"code" => 401, "status" => "UNAUTHENTICATED", "message" => message}} =
               JSON.decode!(response.resp_body)

      assert message == "missing or invalid API credential"
    end

    test "a correct key attaches the normalized service context" do
      name = start_api()

      response = request(name, conn(:get, "/v1/no/such/route") |> authorized())

      assert response.status == 404
      assert {:ok, context} = Auth.fetch_context(response)
      assert context.principal.authn == :api_key
      assert context.principal.kind == :service
      assert context.principal.id == Runtime.new(name: name, api_key: @key).context.principal.id
      assert MapSet.equal?(context.capabilities, MapSet.new([:query, :ingest, :catalog_manage]))
      refute inspect(context) =~ @key
    end

    test "a wrong key is a 401" do
      name = start_api()

      response = request(name, conn(:get, "/v1/datasets") |> authorized("wrong"))

      assert response.status == 401
    end

    test "rotating the API key preserves the principal identity" do
      name = start_api()
      first = request(name, authorized(conn(:get, "/v1/no/such/route")))

      first_id =
        first |> Auth.fetch_context() |> elem(1) |> Map.fetch!(:principal) |> Map.fetch!(:id)

      Runtime.put(Runtime.new(name: name, api_key: "rotated-api-key"))
      second = request(name, authorized(conn(:get, "/v1/no/such/route"), "rotated-api-key"))

      second_id =
        second |> Auth.fetch_context() |> elem(1) |> Map.fetch!(:principal) |> Map.fetch!(:id)

      assert first_id == second_id
      {:ok, context} = Auth.fetch_context(second)
      refute inspect(context) =~ "rotated-api-key"
    end

    test "a non-bearer scheme is a 401" do
      name = start_api()

      conn = conn(:get, "/v1/datasets") |> put_req_header("authorization", "Basic #{@key}")

      assert request(name, conn).status == 401
    end

    test "an instance with no published runtime answers 401, never an open door" do
      name = :"api_never_started_#{:erlang.unique_integer([:positive])}"

      response = request(name, conn(:get, "/v1/datasets") |> authorized())

      assert response.status == 401
    end

    test "an unauthenticated request cannot distinguish real routes from absent ones" do
      name = start_api()

      real = request(name, conn(:get, "/v1/datasets"))
      absent = request(name, conn(:get, "/v1/no/such/route"))

      assert real.status == absent.status
      assert real.resp_body == absent.resp_body
    end
  end

  describe "unknown routes" do
    test "an authenticated request for an absent route is a 404 in the envelope" do
      name = start_api()

      response = request(name, conn(:get, "/v1/no/such/route") |> authorized())

      assert response.status == 404

      assert %{"error" => %{"code" => 404, "status" => "NOT_FOUND"}} =
               JSON.decode!(response.resp_body)
    end
  end
end
