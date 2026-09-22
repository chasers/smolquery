defmodule SmolqueryVictoriaMetrics.AuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias SmolqueryVictoriaMetrics.Auth

  @password "auth-test-password"

  defp check(conn), do: Auth.authenticated?(conn, @password)

  defp authorization(value),
    do: put_req_header(conn(:post, "/api/v1/write"), "authorization", value)

  test "takes a Bearer token, as vmagent's -remoteWrite.bearerToken sends it" do
    assert check(authorization("Bearer " <> @password))
  end

  test "takes basic auth, whatever the user" do
    for user <- ["vmagent", "grafana", ""] do
      assert check(authorization(Plug.BasicAuth.encode_basic_auth(user, @password))), user
    end
  end

  test "refuses a wrong password in either form" do
    refute check(authorization("Bearer wrong"))
    refute check(authorization(Plug.BasicAuth.encode_basic_auth("vmagent", "wrong")))
  end

  test "refuses a request with no credential, or one in another scheme" do
    refute check(conn(:post, "/api/v1/write"))
    refute check(authorization("Token " <> @password))
    refute check(authorization(@password))
  end
end
