defmodule SmolqueryClickHouse.RouterTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias SmolqueryApi.Admission
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime

  @password "router-test-password"
  @insert "/?query=INSERT%20INTO%20logs.events%20FORMAT%20RowBinary"

  setup do
    name = :"ch_router_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, password: @password, max_ndjson_bytes: 1_000))
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  defp request(conn, name), do: Router.call(conn, Router.init(name))

  defp authed(conn), do: put_req_header(conn, "x-clickhouse-key", @password)

  test "GET / and GET /ping answer Ok. without a password", %{name: name} do
    for path <- ["/", "/ping"] do
      response = request(conn(:get, path), name)

      assert response.status == 200
      assert response.resp_body == "Ok.\n"
    end
  end

  test "a missing or wrong password is AUTHENTICATION_FAILED on every path", %{name: name} do
    conns = [
      conn(:post, @insert, ""),
      conn(:get, "/replicas_status"),
      conn(:post, @insert, "") |> put_req_header("x-clickhouse-key", "wrong")
    ]

    for conn <- conns do
      response = request(conn, name)

      assert response.status == 401
      assert get_resp_header(response, "x-clickhouse-exception-code") == ["516"]
      assert response.resp_body =~ "Code: 516. DB::Exception: Authentication failed"
    end
  end

  test "an instance with no published runtime refuses as a wrong password does" do
    response = conn(:post, @insert, "") |> authed() |> request(:ch_router_never_started)

    assert response.status == 401
  end

  test "a query reaches the query service, which is not running here", %{name: name} do
    for conn <- [conn(:get, "/?query=SELECT%201"), conn(:post, "/", "SELECT 1")] do
      response = conn |> authed() |> request(name)

      assert response.status == 503
      assert get_resp_header(response, "x-clickhouse-exception-code") == ["1002"]
      assert get_resp_header(response, "retry-after") == ["5"]
    end
  end

  test "an INSERT in the body alone is NOT_IMPLEMENTED", %{name: name} do
    response =
      conn(:post, "/", "INSERT INTO logs.events VALUES (1)") |> authed() |> request(name)

    assert response.status == 501
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["48"]
  end

  test "a statement over max_query_size is refused before it runs", %{name: name} do
    response =
      conn(:post, "/", "SELECT '" <> String.duplicate("x", 262_145) <> "'")
      |> authed()
      |> request(name)

    assert response.status == 400
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["62"]
  end

  test "max_query_size counts the query parameter and the body together", %{name: name} do
    half = "SELECT '" <> String.duplicate("x", 140_000) <> "'"

    for conn <- [
          conn(:post, "/?" <> URI.encode_query(%{"query" => half}), half),
          conn(:get, "/?" <> URI.encode_query(%{"query" => half <> half}))
        ] do
      response = conn |> authed() |> request(name)

      assert response.status == 400
      assert get_resp_header(response, "x-clickhouse-exception-code") == ["62"]
    end
  end

  test "an unknown path with the password is a 404", %{name: name} do
    response = conn(:get, "/replicas_status") |> authed() |> request(name)

    assert response.status == 404
    assert response.resp_body =~ "There is no handle /replicas_status"
  end

  test "a full admission counter refuses an insert before its body, with retry-after", %{
    name: name
  } do
    start_supervised!({Admission, name: name, limit: 100})
    {:ok, _reservation} = GenServer.call(Admission.server(name), {:admit, 95, self()})

    response =
      conn(:post, @insert, String.duplicate("x", 10))
      |> put_req_header("content-length", "10")
      |> authed()
      |> request(name)

    assert response.status == 429
    assert get_resp_header(response, "retry-after") == ["1"]
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["202"]
  end

  test "a URL parameter that parses to a list or a map is BAD_ARGUMENTS, not a crash", %{
    name: name
  } do
    for path <- [@insert <> "&database[x]=1", @insert <> "&insert_deduplication_token[]=a"] do
      response = conn(:post, path, "") |> authed() |> request(name)

      assert response.status == 400
      assert get_resp_header(response, "x-clickhouse-exception-code") == ["36"]
    end
  end

  test "requests are counted by status class", %{name: name} do
    request(conn(:get, "/ping"), name)

    assert Smolquery.Telemetry.render() =~ ~s(smolquery_clickhouse_requests_total{class="2xx"})
  end
end
