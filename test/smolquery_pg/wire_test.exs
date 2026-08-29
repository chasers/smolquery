defmodule SmolqueryPg.WireTest do
  @moduledoc """
  The Postgres wire edge over a real listener, driven by a simple-protocol
  client (PL-58 layer 1).
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime

  @password "wire-test-password"

  setup do
    unique = :erlang.unique_integer([:positive])
    query = :"pg_wire_query_#{unique}"
    pg = :"pg_wire_edge_#{unique}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg,
       auth: :cleartext,
       password: @password,
       query_name: query,
       port: 0,
       catalog: MapCatalog.new()},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)

    %{port: port}
  end

  defp connect(port) do
    {:ok, socket, params} = PgClient.connect(port, password: @password)

    {socket, params}
  end

  describe "startup" do
    test "authenticates with the password and reports the session parameters", %{port: port} do
      {_socket, params} = connect(port)

      assert params["server_version"] == "14.10"
      assert params["client_encoding"] == "UTF8"
      assert params["integer_datetimes"] == "on"
      assert params["standard_conforming_strings"] == "on"
    end

    test "refuses a wrong password with 28P01 and closes", %{port: port} do
      assert {:error, %{"C" => "28P01", "M" => message}} =
               PgClient.connect(port, password: "nope")

      assert message =~ ~s|user "smolquery"|
    end

    test "declines SSL, then takes the real startup", %{port: port} do
      {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(socket, <<8::32, 80_877_103::32>>)

      assert {:ok, "N"} = :gen_tcp.recv(socket, 1, 5_000)

      :ok = :gen_tcp.send(socket, PgClient.startup([{"user", "u"}]))
      assert {:ok, {?R, <<3::32>>}} = PgClient.recv(socket)
    end

    test "refuses an unknown protocol version", %{port: port} do
      {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(socket, <<8::32, 131_072::32>>)

      assert {:ok, {?E, body}} = PgClient.recv(socket)
      assert %{"C" => "0A000"} = PgClient.fields(body)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 5_000)
    end
  end

  describe "simple query" do
    test "answers a SELECT with typed columns and text values", %{port: port} do
      {socket, _params} = connect(port)

      answer =
        PgClient.query(socket, """
        SELECT 1::BIGINT AS i, 1.5::DOUBLE AS f, 'x' AS s, true AS b,
               TIMESTAMP '2026-08-01 10:00:00' AS ts, DATE '2026-08-01' AS d,
               12.50::DECIMAL(38,2) AS n, NULL::VARCHAR AS z
        """)

      assert answer.errors == []
      assert answer.status == ?I
      assert [%{columns: columns, rows: [row], tag: "SELECT 1"}] = answer.results

      assert Enum.map(columns, & &1.name) == ~w(i f s b ts d n z)
      assert Enum.map(columns, & &1.oid) == [20, 701, 25, 16, 1114, 1082, 1700, 25]

      assert row == [
               "1",
               "1.5",
               "x",
               "t",
               "2026-08-01 10:00:00.000000",
               "2026-08-01",
               "12.50",
               nil
             ]
    end

    test "answers several statements in one message, in order", %{port: port} do
      {socket, _params} = connect(port)

      answer = PgClient.query(socket, "SELECT 1 AS a; SELECT 2 AS b")

      assert [%{rows: [["1"]]}, %{rows: [["2"]]}] = answer.results
    end

    test "an empty query answers EmptyQueryResponse", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: ""}], status: ?I} = PgClient.query(socket, "  ")
    end

    test "a bad query answers a syntax error and the session stays usable", %{port: port} do
      {socket, _params} = connect(port)

      assert %{errors: [%{"C" => "42601"}], status: ?I} =
               PgClient.query(socket, "SELECT FROM WHERE")

      assert %{results: [%{rows: [["1"]]}]} = PgClient.query(socket, "SELECT 1")
    end

    test "an unknown table answers 42P01", %{port: port} do
      {socket, _params} = connect(port)

      assert %{errors: [%{"C" => "42P01", "M" => message}]} =
               PgClient.query(socket, "SELECT * FROM nowhere.nothing")

      assert message =~ "nowhere.nothing"
    end

    test "a write statement answers 0A000", %{port: port} do
      {socket, _params} = connect(port)

      assert %{errors: [%{"C" => "0A000", "M" => message}]} =
               PgClient.query(socket, "INSERT INTO analytics.events VALUES (1)")

      assert message =~ "INSERT is not supported"
    end
  end

  describe "session state" do
    test "SET is remembered, SHOW reads it back, reported parameters are announced", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}], params: %{"application_name" => "psql"}} =
               PgClient.query(socket, "SET application_name = 'psql'")

      assert %{
               results: [%{columns: [%{name: "application_name"}], rows: [["psql"]], tag: "SHOW"}]
             } =
               PgClient.query(socket, "SHOW application_name")

      assert %{results: [%{tag: "SET"}], params: %{"TimeZone" => "UTC"}} =
               PgClient.query(socket, "SET TIME ZONE 'UTC'")

      assert %{results: [%{rows: [["UTC"]]}]} = PgClient.query(socket, "SHOW TIME ZONE")
      assert %{results: [%{tag: "SET"}]} = PgClient.query(socket, "SET extra_float_digits TO 3")
      assert %{results: [%{rows: [["3"]]}]} = PgClient.query(socket, "SHOW extra_float_digits")
      assert %{results: [%{tag: "RESET"}]} = PgClient.query(socket, "RESET application_name")
      assert %{errors: [%{"C" => "42704"}]} = PgClient.query(socket, "SHOW no_such_setting")
    end

    test "SHOW ALL lists every setting", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{columns: columns, rows: rows}]} = PgClient.query(socket, "SHOW ALL")
      assert Enum.map(columns, & &1.name) == ~w(name setting description)
      assert Enum.any?(rows, &match?(["server_version", "14.10", ""], &1))
    end

    test "a transaction block tracks its status, and a failure aborts it", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")
      assert %{status: ?T} = PgClient.query(socket, "SELECT 1")
      assert %{errors: [%{"C" => "42601"}], status: ?E} = PgClient.query(socket, "SELECT FROM")

      assert %{errors: [%{"C" => "25P02"}], status: ?E} = PgClient.query(socket, "SELECT 1")
      assert %{results: [%{tag: "ROLLBACK"}], status: ?I} = PgClient.query(socket, "COMMIT")

      assert %{results: [%{tag: "BEGIN"}], status: ?T} =
               PgClient.query(socket, "START TRANSACTION")

      assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")

      assert %{notices: [%{"C" => "25P01"}], results: [%{tag: "ROLLBACK"}]} =
               PgClient.query(socket, "ROLLBACK")
    end

    test "SET statement_timeout takes Postgres units and refuses what it cannot parse", %{
      port: port
    } do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}]} = PgClient.query(socket, "SET statement_timeout = '30s'")
      assert %{results: [%{rows: [["30000"]]}]} = PgClient.query(socket, "SHOW statement_timeout")

      assert %{results: [%{tag: "SET"}]} =
               PgClient.query(socket, "SET statement_timeout = '2min'")

      assert %{results: [%{rows: [["120000"]]}]} =
               PgClient.query(socket, "SHOW statement_timeout")

      assert %{results: [%{tag: "SET"}]} = PgClient.query(socket, "SET statement_timeout = 0")
      assert %{results: [%{rows: [["0"]]}]} = PgClient.query(socket, "SHOW statement_timeout")

      assert %{errors: [%{"C" => "22023"}]} =
               PgClient.query(socket, "SET statement_timeout = 'abc'")

      assert %{errors: [%{"C" => "22023"}]} = PgClient.query(socket, "SET statement_timeout = -1")
      assert %{results: [%{rows: [["0"]]}]} = PgClient.query(socket, "SHOW statement_timeout")
    end

    test "a statement_timeout cancels a slow query with 57014", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}]} = PgClient.query(socket, "SET statement_timeout = 50")

      assert %{errors: [%{"C" => "57014"}]} =
               PgClient.query(
                 socket,
                 "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"
               )
    end
  end

  describe "the extended protocol" do
    test "parse, bind, describe, execute answer typed rows", %{port: port} do
      {socket, _params} = connect(port)

      answer = PgClient.extended(socket, "SELECT 1::BIGINT AS i, 'x' AS s")

      assert answer.errors == []
      assert answer.status == ?I

      assert [
               %{
                 columns: [%{name: "i", oid: 20}, %{name: "s", oid: 25}],
                 rows: [["1", "x"]],
                 tag: "SELECT 1"
               }
             ] =
               answer.results
    end

    test "binary results carry the Postgres binary forms", %{port: port} do
      {socket, _params} = connect(port)

      answer =
        PgClient.extended(
          socket,
          "SELECT 7::BIGINT AS i, 1.5::DOUBLE AS f, true AS b, DATE '2000-01-02' AS d, 12.50::DECIMAL(38,2) AS n",
          [],
          result_formats: [1]
        )

      assert [%{rows: [[<<7::64-signed>>, <<1.5::float-64>>, <<1>>, <<1::32-signed>>, numeric]]}] =
               answer.results

      assert <<2::16, 0::16-signed, 0::16, 2::16, 12::16, 5000::16>> = numeric
    end

    test "parameters bind by declared OID, in text and binary", %{port: port} do
      {socket, _params} = connect(port)

      answer =
        PgClient.extended(socket, "SELECT $1 + $2 AS sum, $3 AS s, $4 AS z", [
          {20, 0, "40"},
          {20, 1, <<2::64-signed>>},
          {25, 0, "it's"},
          {25, 0, nil}
        ])

      assert answer.errors == []
      assert [%{rows: [["42", "it's", nil]]}] = answer.results
    end

    test "describing a statement answers its parameters and columns before a bind", %{port: port} do
      {socket, _params} = connect(port)

      answer =
        PgClient.extended(socket, "SELECT $1::bigint AS n, 'x' AS s", [{20, 0, "5"}],
          declared: [0],
          describe_statement: true
        )

      assert answer.parameters == [20]
      last = List.last(answer.results)
      assert Enum.map(last.columns, & &1.oid) == [20, 25]
      assert last.rows == [["5", "x"]]
    end

    test "a no-parameter statement describes its columns and runs", %{port: port} do
      {socket, _params} = connect(port)

      answer = PgClient.extended(socket, "SELECT 1 AS a, 'x' AS b", [], describe_statement: true)

      assert answer.parameters == []
      last = List.last(answer.results)
      assert Enum.map(last.columns, & &1.name) == ["a", "b"]
      assert last.rows == [["1", "x"]]
    end

    test "a row limit suspends the portal, and later executes drain it", %{port: port} do
      {socket, _params} = connect(port)

      first = PgClient.extended(socket, "SELECT range AS n FROM range(5)", [], max_rows: 2)
      assert first.suspended
      assert [%{rows: [_, _] = page1}] = first.results

      :ok = PgClient.send_raw(socket, [PgClient.execute("", 2), PgClient.frame(?S, [])])
      second = PgClient.query_answer(socket)
      assert second.suspended
      assert [%{rows: [_, _] = page2}] = second.results

      :ok = PgClient.send_raw(socket, [PgClient.execute("", 0), PgClient.frame(?S, [])])
      third = PgClient.query_answer(socket)
      refute Map.get(third, :suspended, false)
      assert [%{rows: [_] = page3, tag: "SELECT 1"}] = third.results

      values = (page1 ++ page2 ++ page3) |> List.flatten() |> Enum.sort()
      assert values == ["0", "1", "2", "3", "4"]
    end

    test "session statements run through the extended protocol too", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}], params: %{"application_name" => "pgx"}} =
               PgClient.extended(socket, "SET application_name = 'pgx'")

      assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.extended(socket, "BEGIN")
      assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.extended(socket, "COMMIT")
      assert %{results: [%{tag: ""}]} = PgClient.extended(socket, "")
    end

    test "an error discards the pipeline until Sync, then the session is usable", %{port: port} do
      {socket, _params} = connect(port)

      answer = PgClient.extended(socket, "SELECT FROM WHERE")

      assert [%{"C" => "42601"}] = answer.errors
      assert answer.results == []
      assert answer.status == ?I

      assert %{errors: [%{"C" => "26000"}]} =
               (:ok =
                  PgClient.send_raw(socket, [
                    PgClient.bind("", "missing", [], []),
                    PgClient.frame(?S, [])
                  ])) &&
                 PgClient.query_answer(socket)

      assert %{results: [%{rows: [["1"]]}]} = PgClient.extended(socket, "SELECT 1")
    end

    test "a wrong parameter count is a protocol error", %{port: port} do
      {socket, _params} = connect(port)

      assert %{errors: [%{"C" => "08P01"}]} =
               PgClient.extended(socket, "SELECT $1, $2", [{25, 0, "a"}])
    end

    test "an unknown message type answers 0A000 and resynchronises on Sync", %{port: port} do
      {socket, _params} = connect(port)

      :ok = PgClient.send_raw(socket, [PgClient.frame(?F, "body"), PgClient.frame(?S, [])])

      assert {:ok, {?E, body}} = PgClient.recv(socket)
      assert %{"C" => "0A000"} = PgClient.fields(body)
      assert {:ok, {?Z, "I"}} = PgClient.recv(socket)
    end
  end

  describe "cancellation" do
    test "a CancelRequest with the session's key cancels its running query", %{port: port} do
      {socket, _params} = connect(port)
      %{backend: {pid, key}} = PgClient.backend_key(socket)

      :ok =
        PgClient.send_raw(
          socket,
          PgClient.frame(?Q, [
            "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b",
            0
          ])
        )

      Process.sleep(100)
      {:ok, canceller} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(canceller, <<16::32, 80_877_102::32, pid::32, key::32>>)
      assert {:error, :closed} = :gen_tcp.recv(canceller, 0, 5_000)

      assert %{errors: [%{"C" => "57014"}], status: ?I} = PgClient.query_answer(socket)
    end
  end

  describe "idle-in-transaction timeout" do
    test "terminates an idle block with FATAL 25P03, and SET tunes it", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}]} =
               PgClient.query(socket, "SET idle_in_transaction_session_timeout = 150")

      assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")

      assert {:ok, {?E, body}} = PgClient.recv(socket)
      assert %{"S" => "FATAL", "C" => "25P03"} = PgClient.fields(body)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 5_000)
    end

    test "an idle session outside a block is never terminated", %{port: port} do
      {socket, _params} = connect(port)

      assert %{results: [%{tag: "SET"}]} =
               PgClient.query(socket, "SET idle_in_transaction_session_timeout = 100")

      Process.sleep(300)

      assert %{results: [%{rows: [["1"]]}]} = PgClient.query(socket, "SELECT 1")
    end

    test "the timeout cannot be raised past the server's bound or disabled", %{port: port} do
      {socket, _params} = connect(port)

      assert %{notices: [%{"C" => "01000"}]} =
               PgClient.query(socket, "SET idle_in_transaction_session_timeout = 99999999")

      assert %{results: [%{rows: [["300000"]]}]} =
               PgClient.query(socket, "SHOW idle_in_transaction_session_timeout")

      assert %{notices: [%{"C" => "01000"}]} =
               PgClient.query(socket, "SET idle_in_transaction_session_timeout = 0")

      assert %{results: [%{rows: [["300000"]]}]} =
               PgClient.query(socket, "SHOW idle_in_transaction_session_timeout")

      assert %{notices: []} =
               PgClient.query(socket, "SET idle_in_transaction_session_timeout = 250")

      assert %{results: [%{rows: [["250"]]}]} =
               PgClient.query(socket, "SHOW idle_in_transaction_session_timeout")
    end

    test "activity inside the block re-arms the clock", %{port: port} do
      {socket, _params} = connect(port)

      PgClient.query(socket, "SET idle_in_transaction_session_timeout = 400")
      PgClient.query(socket, "BEGIN")

      for _keepalive <- 1..3 do
        Process.sleep(150)
        assert %{results: [%{rows: [["1"]]}], status: ?T} = PgClient.query(socket, "SELECT 1")
      end

      assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")
    end
  end

  test "refuses to boot without a password" do
    Application.put_env(:smolquery, SmolqueryApi, [])

    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryPg.Supervisor.start_link(name: :pg_no_password, port: 0, catalog: MapCatalog.new())
    end
  end
end

defmodule SmolqueryPg.FdwStatementsTest do
  @moduledoc """
  The statements `postgres_fdw` drives a scan with (PL-58 layer 4):
  cursors over a transaction block, savepoints, `DEALLOCATE ALL`, and
  `EXPLAIN`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime

  @password "fdw-statements-password"

  setup do
    unique = :erlang.unique_integer([:positive])
    query = :"pg_fdw_query_#{unique}"
    pg = :"pg_fdw_edge_#{unique}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg,
       auth: :cleartext,
       password: @password,
       query_name: query,
       port: 0,
       catalog: MapCatalog.new()},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)
    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    %{socket: socket}
  end

  test "the exact fdw scan: transaction, declare, fetch pages, close, commit", %{socket: socket} do
    assert %{results: [%{tag: "BEGIN"}], status: ?T} =
             PgClient.query(socket, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    assert %{errors: [], results: [%{tag: "DECLARE CURSOR"}]} =
             PgClient.query(socket, "DECLARE c1 CURSOR FOR SELECT range AS n FROM range(5)")

    assert %{results: [%{columns: [%{name: "n", oid: 20}], rows: [_, _] = page1, tag: "FETCH 2"}]} =
             PgClient.query(socket, "FETCH 2 FROM c1")

    assert %{results: [%{rows: [_, _] = page2, tag: "FETCH 2"}]} =
             PgClient.query(socket, "FETCH 2 FROM c1")

    assert %{results: [%{rows: [_] = page3, tag: "FETCH 1"}]} =
             PgClient.query(socket, "FETCH ALL FROM c1")

    assert %{results: [%{rows: [], tag: "FETCH 0"}]} = PgClient.query(socket, "FETCH 100 FROM c1")

    values = (page1 ++ page2 ++ page3) |> List.flatten() |> Enum.sort()
    assert values == ["0", "1", "2", "3", "4"]

    assert %{results: [%{tag: "CLOSE CURSOR"}]} = PgClient.query(socket, "CLOSE c1")
    assert %{errors: [%{"C" => "34000"}]} = PgClient.query(socket, "FETCH 1 FROM c1")

    assert %{results: [%{tag: "ROLLBACK"}], status: ?I} =
             PgClient.query(socket, "COMMIT TRANSACTION")
  end

  test "MOVE advances without rows, and savepoints are quiet no-ops", %{socket: socket} do
    PgClient.query(socket, "BEGIN")
    PgClient.query(socket, "DECLARE c2 CURSOR FOR SELECT range AS n FROM range(4)")

    assert %{results: [%{tag: "MOVE 3", rows: []}]} = PgClient.query(socket, "MOVE 3 FROM c2")

    assert %{results: [%{tag: "FETCH 1", rows: [[_last]]}]} =
             PgClient.query(socket, "FETCH ALL FROM c2")

    assert %{results: [%{tag: "SAVEPOINT"}], status: ?T} = PgClient.query(socket, "SAVEPOINT s1")

    assert %{results: [%{tag: "ROLLBACK"}], status: ?T} =
             PgClient.query(socket, "ROLLBACK TO SAVEPOINT s1")

    assert %{results: [%{tag: "RELEASE"}], status: ?T} =
             PgClient.query(socket, "RELEASE SAVEPOINT s1")

    assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")
  end

  test "DISCARD ALL cannot run inside a transaction block", %{socket: socket} do
    assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")
    assert %{errors: [%{"C" => "25001"}], status: ?E} = PgClient.query(socket, "DISCARD ALL")
    assert %{results: [%{tag: "ROLLBACK"}], status: ?I} = PgClient.query(socket, "ROLLBACK")
  end

  test "Describe of an empty prepared statement answers NoData without running anything", %{
    socket: socket
  } do
    :ok =
      PgClient.send_raw(socket, [
        PgClient.parse("empty", "", []),
        PgClient.describe(?S, "empty"),
        PgClient.frame(?S, [])
      ])

    assert {:ok, {?1, _}} = PgClient.recv(socket)
    assert {:ok, {?t, <<0::16>>}} = PgClient.recv(socket)
    assert {:ok, {?n, _}} = PgClient.recv(socket)
    assert {:ok, {?Z, "I"}} = PgClient.recv(socket)
  end

  test "DEALLOCATE ALL and DISCARD ALL clear the session's state", %{socket: socket} do
    assert %{results: [%{tag: "DEALLOCATE ALL"}]} = PgClient.query(socket, "DEALLOCATE ALL")

    PgClient.query(socket, "SET application_name = 'fdw'")
    PgClient.query(socket, "DECLARE c3 CURSOR FOR SELECT 1 AS one")

    assert %{results: [%{tag: "DISCARD ALL"}]} = PgClient.query(socket, "DISCARD ALL")
    assert %{errors: [%{"C" => "34000"}]} = PgClient.query(socket, "FETCH 1 FROM c3")
    assert %{results: [%{rows: [["fdw"]]}]} = PgClient.query(socket, "SHOW application_name")
  end

  test "EXPLAIN answers one Foreign Scan cost line postgres_fdw can parse", %{socket: socket} do
    assert %{
             errors: [],
             results: [%{columns: [%{name: "QUERY PLAN"}], rows: [[plan]], tag: "EXPLAIN"}]
           } =
             PgClient.query(socket, "EXPLAIN SELECT 1 AS n")

    assert [_all, _start, _total, rows, _width] =
             Regex.run(~r/\(cost=(\d+\.\d+)\.\.(\d+\.\d+) rows=(\d+) width=(\d+)\)/, plan)

    assert String.to_integer(rows) >= 1
  end

  test "a failed statement aborts the block, and ROLLBACK TO a real savepoint recovers it", %{
    socket: socket
  } do
    PgClient.query(socket, "BEGIN")
    assert %{results: [%{tag: "SAVEPOINT"}]} = PgClient.query(socket, "SAVEPOINT s")
    assert %{errors: [%{"C" => "42601"}], status: ?E} = PgClient.query(socket, "SELECT FROM")

    assert %{errors: [%{"C" => "25P02"}]} =
             PgClient.query(socket, "DECLARE c4 CURSOR FOR SELECT 1")

    assert %{errors: [%{"C" => "25P02"}]} = PgClient.query(socket, "SAVEPOINT s2")

    assert %{results: [%{tag: "ROLLBACK"}], status: ?T} =
             PgClient.query(socket, "ROLLBACK TO SAVEPOINT s")

    assert %{results: [%{rows: [["1"]]}], status: ?T} = PgClient.query(socket, "SELECT 1")
    assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")
  end

  test "unsupported transaction shapes error instead of quietly succeeding", %{socket: socket} do
    assert %{errors: [%{"C" => "25006"}], status: ?I} = PgClient.query(socket, "BEGIN READ WRITE")

    assert %{errors: [%{"C" => "0A000"}]} = PgClient.query(socket, "COMMIT PREPARED 'gid'")
    assert %{errors: [%{"C" => "25P01"}]} = PgClient.query(socket, "SAVEPOINT lonely")

    assert %{errors: [%{"C" => "25P01"}]} =
             PgClient.query(socket, "ROLLBACK TO SAVEPOINT lonely")

    PgClient.query(socket, "BEGIN")
    assert %{errors: [%{"C" => "3B001"}]} = PgClient.query(socket, "RELEASE SAVEPOINT ghost")
    PgClient.query(socket, "ROLLBACK")

    PgClient.query(socket, "BEGIN")

    assert %{errors: [%{"C" => "25006"}], status: ?E} =
             PgClient.query(socket, "SET TRANSACTION READ WRITE")

    PgClient.query(socket, "ROLLBACK")
  end

  test "SET TRANSACTION ISOLATION LEVEL works before the first query, errors after", %{
    socket: socket
  } do
    PgClient.query(socket, "BEGIN")

    assert %{results: [%{tag: "SET"}]} =
             PgClient.query(socket, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{rows: [["repeatable read"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")

    assert %{results: [%{rows: [["1"]]}]} = PgClient.query(socket, "SELECT 1")

    assert %{errors: [%{"C" => "25001"}]} =
             PgClient.query(socket, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

    PgClient.query(socket, "ROLLBACK")

    assert %{results: [%{rows: [["read committed"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")
  end

  test "SET LOCAL reverts at block end, and warns outside one", %{socket: socket} do
    assert %{results: [%{tag: "SET"}], notices: [%{"C" => "25P01"}]} =
             PgClient.query(socket, "SET LOCAL statement_timeout = 123")

    assert %{results: [%{rows: [["0"]]}]} = PgClient.query(socket, "SHOW statement_timeout")

    PgClient.query(socket, "BEGIN")

    assert %{results: [%{tag: "SET"}]} =
             PgClient.query(socket, "SET LOCAL statement_timeout = 456")

    assert %{results: [%{rows: [["456"]]}]} = PgClient.query(socket, "SHOW statement_timeout")
    PgClient.query(socket, "COMMIT")

    assert %{results: [%{rows: [["0"]]}]} = PgClient.query(socket, "SHOW statement_timeout")
  end
end
