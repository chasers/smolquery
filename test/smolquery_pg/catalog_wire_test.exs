defmodule SmolqueryPg.CatalogWireTest do
  @moduledoc """
  The pg_catalog emulation, driven with the exact queries real clients send
  (PL-58 layer 3): Postgrex's type bootstrap, `psql`'s `\\dt`, `\\dn`, and
  `\\d` column query, and the session functions.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime

  @password "catalog-wire-password"

  setup do
    unique = :erlang.unique_integer([:positive])
    query = :"pg_catalog_query_#{unique}"
    pg = :"pg_catalog_edge_#{unique}"

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    schema =
      Schema.new!([
        {"id", :int64, nullable: false},
        {"ts", :timestamp},
        {"amount", {:numeric, 38, 2}}
      ])

    :ok = Catalog.create_table(catalog, {"analytics", "events"}, schema)

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg, password: @password, query_name: query, port: 0, catalog: catalog},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)
    {:ok, socket, _params} = PgClient.connect(port, password: @password)

    %{socket: socket, catalog: catalog}
  end

  test "answers Postgrex's type bootstrap with real Postgres OIDs", %{socket: socket} do
    bootstrap = """
    SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
           coalesce(d.typelem, t.typelem), coalesce(r.rngsubtype, 0),
           ARRAY (
             SELECT a.atttypid
             FROM pg_attribute AS a
             WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum
           )
    FROM pg_type AS t
    LEFT JOIN pg_type AS d ON t.typbasetype = d.oid
    LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR r.rngmultitypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)
    WHERE (t.typrelid = 0)
    AND (t.typelem = 0 OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type s WHERE s.typrelid != 0 AND s.oid = t.typelem))
    """

    answer = PgClient.query(socket, bootstrap)

    assert answer.errors == []
    assert [%{rows: rows}] = answer.results
    assert Enum.count_until(rows, 101) == 101

    by_oid = Map.new(rows, fn [oid | rest] -> {oid, rest} end)

    assert ["bool", "boolsend", "boolrecv", "boolout", "boolin", "0", "0", "{}"] = by_oid["16"]
    assert ["int8" | _rest] = by_oid["20"]
    assert ["numeric" | _rest] = by_oid["1700"]
  end

  test "answers psql's backslash commands", %{socket: socket} do
    dt = """
    SELECT n.nspname as "Schema",
      c.relname as "Name",
      CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as "Type",
      pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
    FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
    WHERE c.relkind IN ('r','p','')
          AND n.nspname <> 'pg_catalog'
          AND n.nspname !~ '^pg_toast'
          AND n.nspname <> 'information_schema'
      AND pg_catalog.pg_table_is_visible(c.oid)
    ORDER BY 1,2
    """

    assert %{errors: [], results: [%{rows: [["analytics", "events", "table", "smolquery"]]}]} =
             PgClient.query(socket, dt)

    dn = """
    SELECT n.nspname AS "Name",
      pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
    ORDER BY 1
    """

    assert %{errors: [], results: [%{rows: rows}]} = PgClient.query(socket, dn)
    assert ["analytics", "smolquery"] in rows
    assert ["public", "smolquery"] in rows
    refute Enum.any?(rows, fn [name, _owner] -> String.starts_with?(name, "pg_") end)
    refute Enum.any?(rows, fn [name, _owner] -> name == "information_schema" end)

    lookup = """
    SELECT c.oid, n.nspname, c.relname
    FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname OPERATOR(pg_catalog.~) '^(events)$' COLLATE pg_catalog.default
      AND pg_catalog.pg_table_is_visible(c.oid)
    ORDER BY 2, 3
    """

    assert %{errors: [], results: [%{rows: [[oid, "analytics", "events"]]}]} =
             PgClient.query(socket, lookup)

    columns = """
    SELECT a.attname,
      pg_catalog.format_type(a.atttypid, a.atttypmod),
      (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
       FROM pg_catalog.pg_attrdef d
       WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
      a.attnotnull,
      (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
       WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
      a.attidentity,
      a.attgenerated
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = '#{oid}' AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
    """

    assert %{errors: [], results: [%{rows: rows}]} = PgClient.query(socket, columns)

    assert [
             ["id", "bigint", nil, "t", nil, "", ""],
             ["ts", "timestamp without time zone", nil, "f", nil, "", ""],
             ["amount", "numeric(38,2)", nil, "f", nil, "", ""]
           ] = rows

    policies = """
    SELECT pol.polname, pol.polpermissive,
      CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
      pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
      pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
      CASE pol.polcmd
        WHEN 'r' THEN 'SELECT'
        WHEN 'a' THEN 'INSERT'
        WHEN 'w' THEN 'UPDATE'
        WHEN 'd' THEN 'DELETE'
        END AS cmd
    FROM pg_catalog.pg_policy pol
    WHERE pol.polrelid = '#{oid}' ORDER BY 1
    """

    assert %{errors: [], results: [%{rows: []}]} = PgClient.query(socket, policies)

    indexes = """
    SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
      pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
    FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
      LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
    WHERE c.oid = '#{oid}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
    ORDER BY i.indisprimary DESC, c2.relname
    """

    assert %{errors: [], results: [%{rows: []}]} = PgClient.query(socket, indexes)
  end

  test "a new table appears after the refresh TTL", %{socket: socket, catalog: catalog} do
    :ok = Catalog.create_table(catalog, {"analytics", "clicks"}, Schema.new!([{"id", :int64}]))

    Process.sleep(1_100)

    assert %{results: [%{rows: rows}]} =
             PgClient.query(socket, "SELECT relname FROM pg_class ORDER BY relname")

    assert ["clicks"] in rows
  end

  test "answers the session functions and information_schema", %{socket: socket} do
    assert %{results: [%{rows: [[version]]}]} = PgClient.query(socket, "SELECT version()")
    assert version =~ "PostgreSQL 14.10"

    assert %{results: [%{rows: [["smolquery"]]}]} =
             PgClient.query(socket, "SELECT current_database()")

    assert %{errors: [], results: [%{rows: rows}]} =
             PgClient.query(
               socket,
               "SELECT table_schema, table_name, column_name, data_type, is_nullable " <>
                 "FROM information_schema.columns WHERE table_name = 'events' ORDER BY ordinal_position"
             )

    assert [
             ["analytics", "events", "id", "bigint", "NO"],
             ["analytics", "events", "ts", "timestamp without time zone", "YES"],
             ["analytics", "events", "amount", "numeric(38,2)", "YES"]
           ] = rows
  end

  test "a user query still reaches the query service", %{socket: socket} do
    assert %{results: [%{rows: [["2"]]}]} = PgClient.query(socket, "SELECT 1 + 1")

    assert %{errors: [%{"C" => "42P01"}]} =
             PgClient.query(socket, "SELECT * FROM analytics.missing")
  end
end
