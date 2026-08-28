defmodule SmolqueryPg.WireTest do
  @moduledoc """
  The Postgres wire edge over a real listener, driven by a simple-protocol
  client (PL-58 layer 1).
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
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
      {SmolqueryPg.Supervisor, name: pg, password: @password, query_name: query, port: 0},
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

      assert params["server_version"] == "16.0"
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
      assert Enum.any?(rows, &match?(["server_version", "16.0", ""], &1))
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

  test "refuses to boot without a password" do
    Application.put_env(:smolquery, SmolqueryApi, [])

    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryPg.Supervisor.start_link(name: :pg_no_password, port: 0)
    end
  end
end
