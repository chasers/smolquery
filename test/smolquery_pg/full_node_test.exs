defmodule SmolqueryPg.FullNodeTest do
  @moduledoc """
  A `SELECT` over a real table through the Postgres wire: rows written into
  the buffer, read back through the planner and the wire (PL-58).
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.Test.FullNode
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @password "full-node-password"

  setup context do
    node = FullNode.start(context, seal_max_files: 1_000, seal_max_age_ms: 600_000)
    pg = :"pg_full_node_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg,
       auth: :cleartext,
       password: @password,
       query_name: node.query,
       port: 0,
       catalog: node.catalog},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)

    %{node: node, port: port}
  end

  test "reads the hot tier through the wire", %{node: node, port: port} do
    batch = %{schema: FullNode.schema(), rows: Enum.map(1..5, &%{"id" => &1})}
    {:ok, _ack} = BufferService.Client.write_batch(node.buffer, FullNode.table(), batch)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    answer = PgClient.query(socket, "SELECT id FROM analytics.events ORDER BY id")

    assert answer.errors == []
    assert [%{columns: [%{name: "id", oid: 20}], rows: rows, tag: "SELECT 5"}] = answer.results
    assert rows == [["1"], ["2"], ["3"], ["4"], ["5"]]

    assert %{results: [%{rows: [["5"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")
  end

  test "a transaction block pins its read: mid-block inserts appear only after COMMIT", %{
    node: node,
    port: port
  } do
    write = fn range ->
      batch = %{schema: FullNode.schema(), rows: Enum.map(range, &%{"id" => &1})}
      {:ok, _ack} = BufferService.Client.write_batch(node.buffer, FullNode.table(), batch)
    end

    write.(1..4)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}], status: ?T} =
             PgClient.query(socket, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{rows: [["repeatable read"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    write.(5..9)
    Process.sleep(50)

    assert %{results: [%{rows: [["4"]]}], status: ?T} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events WHERE id < 100")

    assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")

    assert %{results: [%{rows: [["9"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")
  end

  test "a plain BEGIN is READ COMMITTED: each statement reads fresh", %{node: node, port: port} do
    write = fn range ->
      batch = %{schema: FullNode.schema(), rows: Enum.map(range, &%{"id" => &1})}
      {:ok, _ack} = BufferService.Client.write_batch(node.buffer, FullNode.table(), batch)
    end

    write.(1..3)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")

    assert %{results: [%{rows: [["read committed"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")

    assert %{results: [%{rows: [["3"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    write.(4..5)
    Process.sleep(50)

    assert %{results: [%{rows: [["5"]]}], status: ?T} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    PgClient.query(socket, "COMMIT")
  end
end
