defmodule SmolqueryApi.AuthorizationTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Smolquery.Auth
  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Policy
  alias Smolquery.Auth.Principal
  alias SmolqueryApi.Authorization

  test "rejects unknown capabilities while initializing the route plug" do
    assert_raise ArgumentError, "unsupported API capability", fn ->
      Authorization.init(capability: :platform_operate)
    end
  end

  test "returns the generic 401 envelope for missing and expired contexts" do
    assert authorization(conn(:get, "/v1/datasets"), :query).status == 401

    expired = context([:query], expires_at: 10)

    response =
      conn(:get, "/v1/datasets")
      |> Auth.assign_context(expired)
      |> Authorization.call(Authorization.init(capability: :query))

    assert response.status == 401

    assert JSON.decode!(response.resp_body)["error"] == %{
             "code" => 401,
             "status" => "UNAUTHENTICATED",
             "message" => "missing or invalid API credential"
           }
  end

  test "returns the generic 403 envelope without exposing identity details" do
    response = authorization(conn(:get, "/v1/datasets"), :query, context([:ingest]))

    assert response.status == 403

    assert JSON.decode!(response.resp_body)["error"] == %{
             "code" => 403,
             "status" => "PERMISSION_DENIED",
             "message" => "insufficient API capability"
           }

    refute response.resp_body =~ "subject"
    refute response.resp_body =~ "ingest"
  end

  test "allows a granted capability" do
    response = authorization(conn(:get, "/v1/datasets"), :query, context([:query]))
    refute response.halted
  end

  test "policy treats malformed assigned contexts as unauthenticated" do
    conn = Plug.Conn.assign(conn(:get, "/v1/datasets"), Auth.assign_key(), :not_a_context)

    assert Policy.authorize(:not_a_context, :query, 1) == {:error, :unauthenticated}
    assert authorization(conn, :query).status == 401
  end

  defp authorization(conn, capability, context \\ nil) do
    conn = if context, do: Auth.assign_context(conn, context), else: conn
    Authorization.call(conn, Authorization.init(capability: capability))
  end

  defp context(capabilities, opts \\ []) do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject-1", :user)
    opts = Keyword.put_new(opts, :expires_at, System.system_time(:second) + 100)
    {:ok, context} = Context.single_tenant(principal, capabilities, opts)
    context
  end
end
