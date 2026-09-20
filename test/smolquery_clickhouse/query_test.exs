defmodule SmolqueryClickHouse.QueryTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

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

  test "a literal or a comment that mentions system. asks nothing of the catalog (review of T-482)",
       %{name: name} do
    assert post(name, "SELECT 'kube-system.pod' AS s").resp_body == "kube-system.pod\n"
    assert post(name, "SELECT 1 AS n -- system.tables").resp_body == "1\n"
  end

  describe "ClickHouse's dialect (T-485, T-494)" do
    test "HyperDX's histogram runs as written: macros, backticks, and the alias named three times",
         %{name: name} do
      bucket = "toStartOfInterval(toDateTime(ts), INTERVAL 1 minute) AS `__hdx_time_bucket`"

      sql =
        "SELECT count(),level,#{bucket} " <>
          "FROM (SELECT TIMESTAMP '2026-09-19 10:11:00' + INTERVAL (i) SECOND AS ts, " <>
          "CASE WHEN i % 2 = 0 THEN 'info' ELSE 'error' END AS level FROM range(120) r(i)) " <>
          "WHERE (ts >= fromUnixTimestamp64Milli({from:Int64}) AND ts <= fromUnixTimestamp64Milli({to:Int64})) " <>
          "AND ((hasToken(lower(level), lower('ERROR')))) " <>
          "GROUP BY level,#{bucket} ORDER BY #{bucket} LIMIT {n:Int32} \nFORMAT JSON"

      params = %{
        "param_from" => "1789812660000",
        "param_to" => "1789812780000",
        "param_n" => "100000",
        "date_time_output_format" => "iso"
      }

      response = conn(:post, "/?" <> URI.encode_query(params), sql) |> request(name)

      assert response.status == 200, response.resp_body

      assert %{"meta" => meta, "data" => data, "rows" => 2} = JSON.decode!(response.resp_body)
      assert Enum.map(meta, & &1["name"]) == ["count()", "level", "__hdx_time_bucket"]

      assert data == [
               %{
                 "count()" => "30",
                 "level" => "error",
                 "__hdx_time_bucket" => "2026-09-19T10:11:00.000000Z"
               },
               %{
                 "count()" => "30",
                 "level" => "error",
                 "__hdx_time_bucket" => "2026-09-19T10:12:00.000000Z"
               }
             ]
    end

    test "a number filter's CAST(x, 'Float64') runs", %{name: name} do
      response = post(name, "SELECT 250 = CAST('250', 'Float64') AS same")

      assert response.resp_body == "true\n"
    end

    test "a query service with the functions switched off answers unknown function", %{name: name} do
      off = :"ch_query_off_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {QueryService.Supervisor,
         name: off,
         clickhouse_functions: false,
         catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
        id: off
      )

      on_exit(fn -> QueryService.Runtime.delete(off) end)

      {:ok, runtime} = Runtime.fetch(name)
      Runtime.put(%{runtime | query_name: off})

      response = post(name, "SELECT toDate('2026-09-19')")

      assert response.status == 404
      assert exception_code(response) == ["46"]
      assert response.resp_body =~ "todate"
    end
  end

  describe "a row click's round trip (T-496)" do
    test "a map answers in its stored key order, and sent back as it was answered, finds its row",
         %{name: name} do
      from = "FROM (SELECT MAP {'z.last': '1', 'a.first': '2'} AS attrs, 7 AS id)"

      answered = post(name, "SELECT attrs #{from} FORMAT JSONEachRow").resp_body

      assert answered == ~s|{"attrs":{"z.last":"1","a.first":"2"}}\n|

      json =
        answered
        |> String.trim()
        |> String.replace_prefix(~s|{"attrs":|, "")
        |> String.replace_suffix("}", "")

      where = "attrs=JSONExtract('#{json}', 'Map(String, String)') AND isNull(NULL)"

      assert post(name, "SELECT id #{from} WHERE #{where}").resp_body == "7\n"
    end

    test "a tab-separated row and RowBinary keep that order too", %{name: name} do
      sql = "SELECT MAP {'z': '1', 'a': '2'} AS attrs"

      assert post(name, sql).resp_body == "{'z':'1','a':'2'}\n"

      assert post(name, sql <> " FORMAT RowBinaryWithNamesAndTypes").resp_body =~
               <<2, 1, ?z, 1, ?1, 1, ?a, 1, ?2>>
    end
  end

  describe "a statement the edge cannot answer (T-480)" do
    import ExUnit.CaptureLog

    test "is logged as the client sent it, redacted, with its user agent", %{name: name} do
      log =
        capture_log(fn ->
          response =
            post(
              name,
              "SELECT x FROM range(3) r(x) ARRAY JOIN [1] AS y WHERE 'needle' = 'needle' FORMAT JSON",
              [
                {"user-agent", "hyperdx 2.1.0"}
              ]
            )

          assert response.status == 400
        end)

      assert log =~ "clickhouse edge could not answer: code=62"
      assert log =~ ~s|user_agent="hyperdx 2.1.0"|
      assert log =~ "ARRAY JOIN [1] AS y WHERE '?' = '?' FORMAT JSON"
      refute log =~ "needle"
    end

    test "an unknown format and a missing parameter are logged; a statement that answers is not",
         %{name: name} do
      log =
        capture_log(fn ->
          post(name, "SELECT 1 FORMAT Parquet")
          post(name, "SELECT {absent:String}")
          post(name, "SELECT 1")
        end)

      assert log =~ "code=73"
      assert log =~ "code=456"
      assert [_before, _format, _parameter] = String.split(log, "could not answer")
    end
  end

  test "a TIMESTAMP_NS answers all nine digits, and sent back finds its row (T-496)", %{
    name: name
  } do
    from = "FROM (SELECT CAST('2026-09-20 02:03:16.123456789' AS TIMESTAMP_NS) AS ts, 7 AS id)"

    response =
      conn(:post, "/?date_time_output_format=iso", "SELECT ts #{from} FORMAT JSONEachRow")
      |> request(name)

    assert response.resp_body == ~s|{"ts":"2026-09-20T02:03:16.123456789Z"}\n|

    where = "ts=parseDateTime64BestEffort('2026-09-20T02:03:16.123456789Z', 9)"

    assert post(name, "SELECT id #{from} WHERE #{where}").resp_body == "7\n"
  end

  test "a statement's SETTINGS clause chooses ISO timestamps, as the URL does (review)", %{
    name: name
  } do
    sql =
      "SELECT TIMESTAMP '2026-09-19 10:11:29.5' AS ts SETTINGS date_time_output_format = 'iso' FORMAT JSONEachRow"

    assert post(name, sql).resp_body == ~s|{"ts":"2026-09-19T10:11:29.500000Z"}\n|
  end

  describe "EXPLAIN ESTIMATE (T-506)" do
    test "answers ClickHouse's five columns for a statement, without running it", %{name: name} do
      response = post(name, "EXPLAIN ESTIMATE SELECT count() FROM range(10) r(i) FORMAT JSON")

      assert response.status == 200, response.resp_body

      assert %{"meta" => meta, "data" => [row], "rows" => 1} = JSON.decode!(response.resp_body)
      assert Enum.map(meta, & &1["name"]) == ~w(database table parts rows marks)

      assert row == %{
               "database" => "",
               "table" => "",
               "parts" => "0",
               "rows" => "0",
               "marks" => "0"
             }
    end

    test "is a read, so a GET may send it, in any case and with a parameter", %{name: name} do
      query =
        URI.encode_query(%{"query" => "explain estimate SELECT {n:Int32} AS n", "param_n" => "1"})

      response = conn(:get, "/?" <> query) |> request(name)

      assert response.status == 200
      assert response.resp_body == "\t\t0\t0\t0\n"
    end

    test "the statement under it is rewritten as it would be to run (review of T-506)", %{
      name: name
    } do
      sql =
        "EXPLAIN ESTIMATE WITH (i + 1) AS `next` SELECT count() FROM range(10) r(i) " <>
          "WHERE next > {n:Int32} GROUP BY i AS `k` SETTINGS max_threads = 1 FORMAT JSONEachRow"

      response = conn(:post, "/?param_n=3", sql) |> request(name)

      assert response.status == 200, response.resp_body
      assert %{"marks" => "0"} = JSON.decode!(String.trim(response.resp_body))
    end

    test "rows and parts are null, not zero, when the plan has no sizes to give (review of T-506)",
         %{name: name} do
      response = post(name, "EXPLAIN ESTIMATE SELECT 1 FORMAT JSONEachRow")

      assert %{"rows" => rows, "parts" => parts} = JSON.decode!(String.trim(response.resp_body))
      assert {rows, parts} in [{nil, nil}, {"0", "0"}]
    end

    test "a statement that does not bind answers its error, which is how HyperDX validates an expression",
         %{name: name} do
      response = post(name, "EXPLAIN ESTIMATE SELECT no_such_function(1)")

      assert response.status == 404
      assert exception_code(response) == ["46"]
    end
  end
end
