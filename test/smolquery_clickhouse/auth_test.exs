defmodule SmolqueryClickHouse.AuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3, fetch_query_params: 1]
  import Plug.Test

  alias SmolqueryClickHouse.Auth

  @password "auth-test-password"

  defp check(conn), do: conn |> fetch_query_params() |> Auth.authenticated?(@password)

  test "takes the password from X-ClickHouse-Key, whatever the user" do
    assert conn(:post, "/")
           |> put_req_header("x-clickhouse-user", "logflare")
           |> put_req_header("x-clickhouse-key", @password)
           |> check()
  end

  test "takes the password from basic auth" do
    assert conn(:post, "/")
           |> put_req_header(
             "authorization",
             Plug.BasicAuth.encode_basic_auth("default", @password)
           )
           |> check()
  end

  test "takes the password parameter" do
    assert check(conn(:post, "/?user=default&password=#{@password}"))
  end

  test "takes a bearer token, as the API's insert does" do
    assert conn(:post, "/") |> put_req_header("authorization", "Bearer #{@password}") |> check()
  end

  test "refuses a wrong password and a missing one" do
    refute conn(:post, "/") |> put_req_header("x-clickhouse-key", "wrong") |> check()
    refute check(conn(:post, "/"))
  end

  test "the header wins over the parameter" do
    refute conn(:post, "/?password=#{@password}")
           |> put_req_header("x-clickhouse-key", "wrong")
           |> check()
  end
end
