defmodule SmolqueryPg.FdwIntegrationTest do
  @moduledoc """
  A real Postgres attaching smolquery through `postgres_fdw` (PL-58
  layer 4): `CREATE SERVER`, `IMPORT FOREIGN SCHEMA`, scans, and an
  aggregate through the foreign table.

  Tagged `:fdw` and excluded by default: the Postgres server itself must
  be able to open a TCP connection back to this test's listener, which
  holds for a host-native Postgres (`TEST_POSTGRES_HOST=localhost`) but
  not for CI's service container, whose loopback is its own namespace.
  Run it locally with `mix test --only fdw`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.Test.FullNode
  alias Smolquery.Test.Postgres
  alias SmolqueryPg.Runtime

  @moduletag :fdw
  @moduletag :tmp_dir

  @password "fdw-integration-password"

  setup context do
    node = FullNode.start(context, seal_max_files: 1_000, seal_max_age_ms: 600_000)
    pg = :"pg_fdw_int_#{:erlang.unique_integer([:positive])}"

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

    batch = %{schema: FullNode.schema(), rows: Enum.map(1..7, &%{"id" => &1})}
    {:ok, _ack} = BufferService.Client.write_batch(node.buffer, FullNode.table(), batch)

    options = Postgres.ensure_database!()
    {:ok, conn} = Postgrex.start_link(options)

    server = "smol_fdw_#{:erlang.unique_integer([:positive])}"
    schema = "#{server}_analytics"
    edge_host = System.get_env("TEST_PG_EDGE_HOST", "127.0.0.1")

    Postgrex.query!(conn, "CREATE EXTENSION IF NOT EXISTS postgres_fdw", [])

    Postgrex.query!(
      conn,
      "CREATE SERVER #{server} FOREIGN DATA WRAPPER postgres_fdw " <>
        "OPTIONS (host '#{edge_host}', port '#{port}', dbname 'smolquery')",
      []
    )

    Postgrex.query!(
      conn,
      "CREATE USER MAPPING FOR CURRENT_USER SERVER #{server} " <>
        "OPTIONS (user 'smolquery', password '#{@password}')",
      []
    )

    Postgrex.query!(conn, "CREATE SCHEMA #{schema}", [])

    on_exit(fn ->
      {:ok, cleanup} = Postgrex.start_link(options)
      Postgrex.query(cleanup, "DROP SERVER IF EXISTS #{server} CASCADE", [])
      Postgrex.query(cleanup, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
      GenServer.stop(cleanup)
    end)

    %{conn: conn, server: server, schema: schema}
  end

  test "imports the schema and scans through the foreign table", %{
    conn: conn,
    server: server,
    schema: schema
  } do
    Postgrex.query!(
      conn,
      "IMPORT FOREIGN SCHEMA analytics FROM SERVER #{server} INTO #{schema}",
      []
    )

    tables =
      Postgrex.query!(
        conn,
        "SELECT foreign_table_name FROM information_schema.foreign_tables " <>
          "WHERE foreign_table_schema = $1",
        [schema]
      )

    assert tables.rows == [["events"]]

    assert %{rows: [[7]]} = Postgrex.query!(conn, "SELECT count(*) FROM #{schema}.events", [])

    assert %{rows: [[1], [2], [3]]} =
             Postgrex.query!(conn, "SELECT id FROM #{schema}.events ORDER BY id LIMIT 3", [])

    assert %{rows: [[28]]} =
             Postgrex.query!(conn, "SELECT sum(id)::bigint FROM #{schema}.events", [])
  end
end
