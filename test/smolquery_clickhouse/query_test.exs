defmodule SmolqueryClickHouse.QueryTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime

  @password "query-test-password"

  setup do
    query = :"ch_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    name = :"ch_query_edge_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, password: @password, query_name: query))
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  defp request(conn, name) do
    conn
    |> put_req_header("x-clickhouse-key", @password)
    |> Router.call(name)
  end

  defp post(name, sql, headers \\ []) do
    headers
    |> Enum.reduce(conn(:post, "/", sql), fn {key, value}, conn ->
      put_req_header(conn, key, value)
    end)
    |> request(name)
  end

  defp exception_code(response), do: get_resp_header(response, "x-clickhouse-exception-code")

  test "a statement in the body answers TabSeparated by default", %{name: name} do
    response = post(name, "SELECT 1 + 1 AS n, 'a\tb' AS s")

    assert response.status == 200
    assert response.resp_body == "2\ta\\tb\n"
    assert get_resp_header(response, "x-clickhouse-format") == ["TabSeparated"]
    assert [_id] = get_resp_header(response, "x-clickhouse-query-id")
    assert [summary] = get_resp_header(response, "x-clickhouse-summary")
    assert %{"result_rows" => "1", "read_bytes" => _bytes} = JSON.decode!(summary)
  end

  test "a GET reads the query parameter, and its FORMAT clause names the format", %{name: name} do
    response =
      conn(:get, "/?" <> URI.encode_query(%{"query" => "SELECT 7 AS n FORMAT JSONEachRow;"}))
      |> request(name)

    assert response.status == 200
    assert response.resp_body == ~s({"n":7}\n)
    assert get_resp_header(response, "x-clickhouse-format") == ["JSONEachRow"]
  end

  test "X-ClickHouse-Format names the format, and a FORMAT clause wins over it", %{name: name} do
    assert %{"data" => [%{"n" => 3}], "rows" => 1} =
             name
             |> post("SELECT 3 AS n", [{"x-clickhouse-format", "JSON"}])
             |> Map.fetch!(:resp_body)
             |> JSON.decode!()

    response = post(name, "SELECT 3 AS n FORMAT TSVWithNames", [{"x-clickhouse-format", "JSON"}])

    assert response.resp_body == "n\n3\n"
  end

  test "the handshake ch sends on connect answers a version, in RowBinaryWithNamesAndTypes", %{
    name: name
  } do
    response =
      post(name, "select 1, version()", [{"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}])

    assert response.status == 200
    assert get_resp_header(response, "x-clickhouse-format") == ["RowBinaryWithNamesAndTypes"]
    assert <<2, _names_and_types::binary>> = response.resp_body
    assert String.ends_with?(response.resp_body, <<0, 8, "24.8.1.1">>)
  end

  test "timezone() answers UTC", %{name: name} do
    assert post(name, "SELECT timezone() AS tz").resp_body == "UTC\n"
  end

  test "an unknown format is UNKNOWN_FORMAT", %{name: name} do
    response = post(name, "SELECT 1 FORMAT Native")

    assert response.status == 404
    assert exception_code(response) == ["73"]
  end

  test "a statement the engine cannot parse is SYNTAX_ERROR", %{name: name} do
    response = post(name, "SELEC 1")

    assert response.status == 400
    assert exception_code(response) == ["62"]
    assert response.resp_body =~ "Code: 62. DB::Exception: "
  end

  test "a table the catalog does not hold is UNKNOWN_TABLE", %{name: name} do
    response = post(name, "SELECT * FROM logs.nope")

    assert {response.status, exception_code(response)} == {404, ["60"]}, response.resp_body
  end

  test "an empty statement is SYNTAX_ERROR", %{name: name} do
    assert name |> post("  ") |> exception_code() == ["62"]
  end

  test "a GET that could change a table is READONLY", %{name: name} do
    response =
      conn(:get, "/?" <> URI.encode_query(%{"query" => "ALTER TABLE a.b ADD COLUMN c STRING"}))
      |> request(name)

    assert response.status == 400
    assert exception_code(response) == ["164"]
  end

  test "an invalid max_execution_time is BAD_ARGUMENTS", %{name: name} do
    response =
      conn(:post, "/?max_execution_time=soon", "SELECT 1")
      |> request(name)

    assert response.status == 400
    assert exception_code(response) == ["36"]
  end
end
