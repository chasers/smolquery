defmodule SmolqueryPg.DdlWireTest do
  @moduledoc """
  `ALTER TABLE` over the Postgres wire (PL-61 L3): the `ALTER TABLE` command
  tag, Postgres's notice for a guarded no-op, Postgres's codes for the
  refusals, and a refusal inside a transaction block.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime

  @password "ddl-wire-password"
  @table {"analytics", "events"}

  setup do
    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
      )

    unique = :erlang.unique_integer([:positive])
    query = :"pg_ddl_query_#{unique}"
    pg = :"pg_ddl_edge_#{unique}"

    start_supervised!({QueryService.Supervisor, name: query, catalog: catalog}, id: query)
    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg,
       auth: :cleartext,
       password: @password,
       query_name: query,
       port: 0,
       catalog: catalog},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)
    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    %{socket: socket, catalog: catalog}
  end

  test "adds and drops with the ALTER TABLE tag; a guarded no-op notices", %{
    socket: socket,
    catalog: catalog
  } do
    assert %{errors: [], results: [%{tag: "ALTER TABLE", rows: []}], status: ?I} =
             PgClient.query(socket, "ALTER TABLE analytics.events ADD COLUMN label VARCHAR")

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert Schema.names(schema) == ["id", "ts", "label"]

    assert %{errors: [], results: [%{tag: "ALTER TABLE"}], notices: [notice]} =
             PgClient.query(
               socket,
               "ALTER TABLE analytics.events ADD COLUMN IF NOT EXISTS label TEXT"
             )

    assert notice["S"] == "NOTICE"

    assert notice["M"] =~
             ~s|column "label" of relation "analytics.events" already exists, skipping|

    assert %{errors: [], results: [%{tag: "ALTER TABLE"}], notices: []} =
             PgClient.query(socket, "ALTER TABLE analytics.events DROP COLUMN label;")

    assert %{errors: [], results: [%{tag: "ALTER TABLE"}], notices: [%{"M" => skipped}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events DROP COLUMN IF EXISTS label")

    assert skipped =~ "does not exist, skipping"
  end

  test "the refusals answer Postgres's codes", %{socket: socket} do
    assert %{errors: [%{"C" => "42701", "M" => "column id already exists"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events ADD COLUMN id BIGINT")

    assert %{errors: [%{"C" => "42703", "M" => "column nope does not exist"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events DROP COLUMN nope")

    assert %{errors: [%{"C" => "42P01"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.nope DROP COLUMN id")

    assert %{errors: [%{"C" => "0A000", "M" => "NOT NULL is not supported in ALTER TABLE"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events ADD COLUMN n BIGINT NOT NULL")

    assert %{errors: [%{"C" => "0A000", "M" => "RENAME is not supported in ALTER TABLE"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events RENAME TO other")

    assert %{errors: [%{"C" => "42601", "M" => "events: a table is named dataset.table"}]} =
             PgClient.query(socket, "ALTER TABLE events DROP COLUMN ts")

    assert %{errors: [%{"C" => "0A000", "M" => "bind parameters are supported in SELECT" <> _}]} =
             PgClient.extended(
               socket,
               "ALTER TABLE analytics.events ADD COLUMN label VARCHAR",
               [{25, 0, "x"}]
             )
  end

  test "inside a transaction block it is refused, and the block is failed", %{
    socket: socket,
    catalog: catalog
  } do
    assert %{results: [%{tag: "BEGIN"}], status: ?T} = PgClient.query(socket, "BEGIN")

    assert %{errors: [%{"C" => "25001", "M" => message}], status: ?E} =
             PgClient.query(socket, "ALTER TABLE analytics.events ADD COLUMN label VARCHAR")

    assert message =~ "cannot run inside a transaction block"

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert Schema.names(schema) == ["id", "ts"]

    assert %{results: [%{tag: "ROLLBACK"}], status: ?I} = PgClient.query(socket, "ROLLBACK")

    assert %{errors: [], results: [%{tag: "ALTER TABLE"}]} =
             PgClient.query(socket, "ALTER TABLE analytics.events ADD COLUMN label VARCHAR")
  end
end
