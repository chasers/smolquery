defmodule SmolqueryPg.FullNodeTest do
  @moduledoc """
  A `SELECT` over a real table through the Postgres wire: rows written into
  the buffer, read back through the planner and the wire (PL-58).
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode
  alias Smolquery.Test.PgClient
  alias Smolquery.Test.SegmentFixture
  alias SmolqueryPg.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @password "full-node-password"

  setup context do
    grace = context |> Map.take([:retire_grace_ms]) |> Map.to_list()
    node = FullNode.start(context, [seal_max_files: 1_000, seal_max_age_ms: 600_000] ++ grace)
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
    write(node, 1..4)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}], status: ?T} =
             PgClient.query(socket, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{rows: [["repeatable read"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    write(node, 5..9)

    assert %{results: [%{rows: [["4"]]}], status: ?T} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events WHERE id < 100")

    assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")

    assert %{results: [%{rows: [["9"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")
  end

  test "a plain BEGIN is READ COMMITTED: each statement reads fresh", %{node: node, port: port} do
    write(node, 1..3)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")

    assert %{results: [%{rows: [["read committed"]]}]} =
             PgClient.query(socket, "SHOW transaction_isolation")

    assert %{results: [%{rows: [["3"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    write(node, 4..5)

    assert %{results: [%{rows: [["5"]]}], status: ?T} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    PgClient.query(socket, "COMMIT")
  end

  test "a block reads a table by the ids its first touch saw: a segment stamped before the bound that lands later stays out (T-418)",
       %{node: node, port: port} do
    write(node, 1..4)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}], status: ?T} =
             PgClient.query(socket, "BEGIN ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    land_back_dated(node, [%{"id" => 100}], System.system_time(:millisecond) - 5_000)
    write(node, 5..9)

    assert %{results: [%{rows: [["4"]]}], status: ?T} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    assert %{results: [%{rows: [["4"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events WHERE id < 1000")

    assert %{results: [%{tag: "COMMIT"}], status: ?I} = PgClient.query(socket, "COMMIT")

    assert %{results: [%{rows: [["10"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")
  end

  @tag retire_grace_ms: 0
  test "a block whose pinned segment was retired past the grace answers 72000, never other rows (T-418)",
       %{node: node, port: port} do
    {:ok, ack} = write(node, 1..3)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)
    count = "SELECT count(*) AS n FROM analytics.events"

    assert %{results: [%{tag: "BEGIN"}]} =
             PgClient.query(socket, "BEGIN ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{rows: [["3"]]}]} = PgClient.query(socket, count)

    :ok = BufferService.Client.retire(node.buffer, FullNode.table(), [ack.segment_id], 1)

    assert Eventually.until(fn ->
             BufferService.Client.hot_manifest(node.buffer, FullNode.table()) == {:ok, []}
           end)

    assert %{errors: [%{"C" => "72000", "M" => message}], status: ?E} =
             PgClient.query(socket, count)

    assert message =~ "analytics.events"
    assert %{results: [%{tag: "ROLLBACK"}], status: ?I} = PgClient.query(socket, "ROLLBACK")
  end

  test "cursors in a block agree with each other and with a rescan, as postgres_fdw needs (T-418)",
       %{node: node, port: port} do
    write(node, 1..3)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)
    scan = "SELECT id FROM analytics.events ORDER BY id"

    assert %{results: [%{tag: "BEGIN"}]} =
             PgClient.query(socket, "BEGIN ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{tag: "DECLARE CURSOR"}]} =
             PgClient.query(socket, "DECLARE c1 CURSOR FOR #{scan}")

    assert %{results: [%{rows: first}]} = PgClient.query(socket, "FETCH ALL FROM c1")
    assert first == [["1"], ["2"], ["3"]]

    write(node, 4..6)

    assert %{results: [%{tag: "DECLARE CURSOR"}]} =
             PgClient.query(socket, "DECLARE c2 CURSOR FOR #{scan}")

    assert %{results: [%{rows: ^first}]} = PgClient.query(socket, "FETCH ALL FROM c2")

    assert %{results: [%{tag: "CLOSE CURSOR"}]} = PgClient.query(socket, "CLOSE c1")

    assert %{results: [%{tag: "DECLARE CURSOR"}]} =
             PgClient.query(socket, "DECLARE c1 CURSOR FOR #{scan}")

    assert %{results: [%{rows: ^first}]} = PgClient.query(socket, "FETCH ALL FROM c1")

    assert %{results: [%{tag: "COMMIT"}]} = PgClient.query(socket, "COMMIT")

    assert %{results: [%{rows: rows}]} = PgClient.query(socket, scan)
    assert rows == Enum.map(1..6, &[Integer.to_string(&1)])
  end

  test "EXPLAIN in a block forms and reads the pin, as postgres_fdw estimates before it scans (T-418)",
       %{node: node, port: port} do
    {:ok, _ack} = write(node, 1..3)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    assert %{results: [%{tag: "BEGIN"}]} =
             PgClient.query(socket, "BEGIN ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{tag: "EXPLAIN"}]} =
             PgClient.query(socket, "EXPLAIN SELECT id FROM analytics.events")

    {:ok, _ack} = write(node, 4..6)

    assert %{results: [%{rows: [["3"]]}]} =
             PgClient.query(socket, "SELECT count(*) AS n FROM analytics.events")

    assert %{results: [%{rows: [[plan]]}]} =
             PgClient.query(socket, "EXPLAIN SELECT id FROM analytics.events")

    assert plan =~ "rows=3"
    assert %{results: [%{tag: "COMMIT"}]} = PgClient.query(socket, "COMMIT")
  end

  test "a statement that fails before the pin has a snapshot drops the half pin: the block re-pins whole (T-418)",
       %{node: node, port: port} do
    {:ok, _ack} = write(node, 1..2)

    {:ok, socket, _params} = PgClient.connect(port, password: @password)
    count = "SELECT count(*) AS n FROM analytics.events"

    assert %{results: [%{tag: "BEGIN"}]} =
             PgClient.query(socket, "BEGIN ISOLATION LEVEL REPEATABLE READ")

    assert %{results: [%{tag: "SAVEPOINT"}]} = PgClient.query(socket, "SAVEPOINT s")

    assert %{errors: [%{"C" => "42P01"}], status: ?E} =
             PgClient.query(socket, "SELECT count(*) FROM analytics.missing")

    {:ok, _ack} = write(node, 3..4)

    assert %{results: [%{tag: "ROLLBACK"}], status: ?T} =
             PgClient.query(socket, "ROLLBACK TO SAVEPOINT s")

    assert %{results: [%{rows: [["4"]]}]} = PgClient.query(socket, count)

    {:ok, _ack} = write(node, 5..6)

    assert %{results: [%{rows: [["4"]]}]} = PgClient.query(socket, count)
    assert %{results: [%{tag: "COMMIT"}]} = PgClient.query(socket, "COMMIT")
  end

  defp write(node, range) do
    batch = %{schema: FullNode.schema(), rows: Enum.map(range, &%{"id" => &1})}
    BufferService.Client.write_batch(node.buffer, FullNode.table(), batch)
  end

  defp land_back_dated(node, rows, stamped_ms) do
    {:ok, runtime} = BufferService.Runtime.fetch(node.buffer)
    {:ok, prefix} = Store.prefix(FullNode.table())

    {:ok, segment} =
      SegmentFixture.write(rows, FullNode.schema(),
        store: runtime.store,
        prefix: prefix,
        id: Id.generate(stamped_ms)
      )

    {:ok, _entry} = HotManifest.add(runtime.manifest, FullNode.table(), segment)
  end
end
