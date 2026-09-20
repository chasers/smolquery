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

  test "version() inside a string literal or a comment is left as written", %{name: name} do
    response = post(name, "SELECT 'call version()' AS s, version() AS v /* version() */")

    assert response.status == 200
    assert response.resp_body == "call version()\t24.8.1.1\n"
  end

  test "a FORMAT clause after a string literal names the format", %{name: name} do
    response = post(name, "SELECT 1 AS n WHERE 'x' = 'x' FORMAT JSONEachRow")

    assert response.resp_body == ~s({"n":1}\n)
  end

  test "a max_execution_time past what a timer takes is held to the longest one", %{name: name} do
    for seconds <- ["5000000", "1e300"] do
      response = conn(:post, "/?max_execution_time=#{seconds}", "SELECT 1") |> request(name)

      assert response.status == 200, seconds
    end
  end

  test "a value that is not UTF-8 answers in every format, replaced in JSON", %{name: name} do
    sql = "SELECT CAST(unhex('FF41') AS BLOB) AS b"

    assert post(name, sql <> " FORMAT JSONEachRow").resp_body == ~s({"b":"�A"}\n)
    assert post(name, sql <> " FORMAT TabSeparated").status == 200
    assert post(name, sql <> " FORMAT RowBinaryWithNamesAndTypes").status == 200
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

  describe "a statement written for ClickHouse (T-481)" do
    test "parameters, backticks, escapes, SETTINGS and FORMAT, as HyperDX sends them", %{
      name: name
    } do
      sql =
        "SELECT {n:Int32} AS `__hdx_n`, {s:String} AS s, 'it\\'s' AS q " <>
          "SETTINGS short_circuit_function_evaluation = 'force_enable' \nFORMAT JSONCompact"

      response =
        conn(:post, "/?" <> URI.encode_query(%{"param_n" => "7", "param_s" => "a'b"}), sql)
        |> request(name)

      assert response.status == 200
      assert %{"meta" => meta, "data" => [[7, "a'b", "it's"]]} = JSON.decode!(response.resp_body)
      assert Enum.map(meta, & &1["name"]) == ["__hdx_n", "s", "q"]
    end

    test "the SETTINGS clause bounds the query, over the URL's setting", %{name: name} do
      response =
        conn(:post, "/?max_execution_time=abc", "SELECT 1 SETTINGS max_execution_time = 5")
        |> request(name)

      assert response.status == 200
      assert response.resp_body == "1\n"
    end

    test "FORMAT before SETTINGS is read too", %{name: name} do
      response = post(name, "SELECT 1 AS n FORMAT JSONEachRow SETTINGS max_threads = 1")

      assert response.status == 200
      assert response.resp_body == ~s|{"n":1}\n|
    end

    test "a placeholder with no value is code 456", %{name: name} do
      response = post(name, "SELECT {missing:String}")

      assert response.status == 400
      assert exception_code(response) == ["456"]
    end
  end

  describe "a parameter's value is a value, whatever it holds (review of T-481)" do
    defp with_params(name, sql, params) do
      query = Map.new(params, fn {key, value} -> {"param_" <> key, value} end)

      conn(:post, "/?" <> URI.encode_query(query), sql) |> request(name)
    end

    test "a trailing backslash cannot open the literal to the next parameter's text", %{
      name: name
    } do
      sql = "SELECT {p:String} AS p, {q:String} AS q FORMAT JSONEachRow"

      response = with_params(name, sql, %{"p" => "x\\\\", "q" => " OR 1=1 --"})

      assert response.status == 200
      assert JSON.decode!(response.resp_body) == %{"p" => "x\\", "q" => " OR 1=1 --"}
    end

    test "a value is unescaped once: an escaped backslash before n is not a newline", %{
      name: name
    } do
      response =
        with_params(name, "SELECT {p:String} AS p FORMAT JSONEachRow", %{"p" => "C:\\\\new"})

      assert JSON.decode!(response.resp_body) == %{"p" => "C:\\new"}
    end

    test "an Identifier holding a backslash and a quote names one column", %{name: name} do
      response =
        with_params(name, "SELECT 1 AS {c:Identifier} FORMAT JSONEachRow", %{
          "c" => "a\\\" , 2 AS \"b"
        })

      assert JSON.decode!(response.resp_body) == %{"a\\\" , 2 AS \"b" => 1}
    end

    test "a negative number after a minus sign is not a comment", %{name: name} do
      response =
        with_params(name, "SELECT 10 -{n:Int32} AS v, 1 -{f:Float64} AS w", %{
          "n" => "-5",
          "f" => "-0.5"
        })

      assert response.resp_body == "15\t1.5\n"
    end

    test "a type with a quoted argument is still a placeholder", %{name: name} do
      response =
        with_params(name, "SELECT {t:DateTime64(3, 'UTC')} AS t FORMAT JSONEachRow", %{
          "t" => "2026-09-19 10:11:12.5"
        })

      assert response.status == 200, response.resp_body
      assert response.resp_body =~ "2026-09-19 10:11:12.5"
    end
  end

  describe "what HyperDX reads in an answer (T-493)" do
    test "count() is named count(), and the results table's format answers", %{name: name} do
      response = post(name, "SELECT count(), count(*) AS n FORMAT JSON")

      assert %{"meta" => [%{"name" => "count()"}, %{"name" => "n"}], "data" => [row]} =
               JSON.decode!(response.resp_body)

      assert row == %{"count()" => "1", "n" => "1"}

      response = post(name, "SELECT 1 AS a FORMAT JSONCompactEachRowWithNamesAndTypes")

      assert get_resp_header(response, "x-clickhouse-format") == [
               "JSONCompactEachRowWithNamesAndTypes"
             ]

      assert response.resp_body == ~s|["a"]\n["Nullable(Int32)"]\n[1]\n|
    end

    test "date_time_output_format=iso writes a timestamp as ISO 8601", %{name: name} do
      response =
        conn(
          :post,
          "/?date_time_output_format=iso",
          "SELECT TIMESTAMP '2026-09-19 10:11:29.5' AS ts FORMAT JSONEachRow"
        )
        |> request(name)

      assert response.resp_body == ~s|{"ts":"2026-09-19T10:11:29.500000Z"}\n|
    end
  end
end
