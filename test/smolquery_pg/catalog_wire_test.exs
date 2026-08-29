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

  test "two result columns of one name both answer: the re-select labels them (T-426)", %{
    socket: socket
  } do
    assert %{
             errors: [],
             results: [
               %{columns: [%{name: "count_star()"}, %{name: "count_star()_1"}], rows: [[n, n]]}
             ]
           } =
             PgClient.query(
               socket,
               "SELECT count(*), count(*) FROM pg_catalog.pg_class WHERE relname = 'events'"
             )

    assert n == "1"
  end

  test "to_regclass resolves a qualified name to its schema's table (T-426)", %{
    socket: socket,
    catalog: catalog
  } do
    :ok = Catalog.create_dataset(catalog, "billing")
    :ok = Catalog.create_table(catalog, {"billing", "events"}, Schema.new!([{"id", :int64}]))

    assert %{errors: [], results: [%{rows: [["billing"]]}]} =
             PgClient.query(
               socket,
               "SELECT n.nspname FROM pg_catalog.pg_class c " <>
                 "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " <>
                 "WHERE c.oid = to_regclass('billing.events')"
             )

    assert %{errors: [], results: [%{rows: [[_oid]]}]} =
             PgClient.query(
               socket,
               "SELECT to_regclass('events') FROM pg_catalog.pg_namespace LIMIT 1"
             )
  end

  test "pg_settings and current_setting answer the same value (T-426)", %{socket: socket} do
    assert %{errors: [], results: [%{rows: [["public", "public"]]}]} =
             PgClient.query(
               socket,
               "SELECT setting, current_setting('search_path') FROM pg_catalog.pg_settings " <>
                 "WHERE name = 'search_path'"
             )
  end

  test "a catalog query binds its parameters on the catalog engine (T-410)", %{socket: socket} do
    assert %{errors: [], results: [%{rows: [["events"]]}]} =
             PgClient.extended(
               socket,
               "SELECT relname FROM pg_catalog.pg_class WHERE relname = $1",
               [{25, 0, "events"}]
             )
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

  test "information_schema.columns carries the detail pgjdbc and psqlODBC read (T-412)", %{
    socket: socket
  } do
    assert %{errors: [], results: [%{rows: rows}]} =
             PgClient.query(
               socket,
               "SELECT column_name, udt_name, numeric_precision, numeric_scale, " <>
                 "datetime_precision, character_maximum_length, column_default, is_identity, " <>
                 "is_generated FROM information_schema.columns " <>
                 "WHERE table_schema = 'analytics' AND table_name = 'events' ORDER BY ordinal_position"
             )

    assert [
             ["id", "int8", "64", "0", nil, nil, nil, "NO", "NEVER"],
             ["ts", "timestamp", nil, nil, "6", nil, nil, "NO", "NEVER"],
             ["amount", "numeric", "38", "2", nil, nil, nil, "NO", "NEVER"]
           ] = rows
  end

  test "pgjdbc's getTables and getColumns (Metabase, DBeaver) bind and answer (T-412)", %{
    socket: socket
  } do
    tables = """
    SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,
      CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'
        WHEN true THEN CASE WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema'
          THEN CASE c.relkind WHEN 'r' THEN 'SYSTEM TABLE' WHEN 'v' THEN 'SYSTEM VIEW'
            WHEN 'i' THEN 'SYSTEM INDEX' ELSE NULL END
          WHEN n.nspname = 'pg_toast' THEN CASE c.relkind WHEN 'r' THEN 'SYSTEM TOAST TABLE'
            WHEN 'i' THEN 'SYSTEM TOAST INDEX' ELSE NULL END
          ELSE CASE c.relkind WHEN 'r' THEN 'TEMPORARY TABLE' WHEN 'p' THEN 'TEMPORARY TABLE'
            WHEN 'i' THEN 'TEMPORARY INDEX' WHEN 'S' THEN 'TEMPORARY SEQUENCE'
            WHEN 'v' THEN 'TEMPORARY VIEW' ELSE NULL END END
        WHEN false THEN CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE'
          WHEN 'i' THEN 'INDEX' WHEN 'P' then 'PARTITIONED INDEX' WHEN 'S' THEN 'SEQUENCE'
          WHEN 'v' THEN 'VIEW' WHEN 'c' THEN 'TYPE' WHEN 'f' THEN 'FOREIGN TABLE'
          WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE NULL END
        ELSE NULL END AS TABLE_TYPE, d.description AS REMARKS,
      '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME,
      '' AS REF_GENERATION
    FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c
      LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0
        and d.classoid = 'pg_class'::regclass)
    WHERE c.relnamespace = n.oid
      AND (false OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) )
    ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME
    """

    assert %{errors: [], results: [%{rows: [[nil, "analytics", "events", "TABLE" | _rest]]}]} =
             PgClient.query(socket, tables)

    columns = """
    SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,
      a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,
      t.typtypmod,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
      nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,
      pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype
    FROM pg_catalog.pg_namespace n
      JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
      JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
      JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
      LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
      LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
      LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
      LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
    WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped
      AND n.nspname LIKE 'analytics' AND c.relname LIKE 'events') c
    WHERE true ORDER BY nspname,c.relname,attnum
    """

    assert %{errors: [], results: [%{rows: rows}]} = PgClient.query(socket, columns)

    assert [["analytics", "events", "id", "20", "t", "-1", "8", "-1", "1" | _], _ts, _amount] =
             rows
  end

  test "pgjdbc's getPrimaryKeys expands the index key array, and finds none (T-412)", %{
    socket: socket
  } do
    primary_keys = """
    SELECT result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME,
      result.KEY_SEQ, result.PK_NAME
    FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME,
        a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
        ci.relname AS PK_NAME, information_schema._pg_expandarray(i.indkey) AS KEYS,
        a.attnum AS A_ATTNUM
      FROM pg_catalog.pg_class ct
        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)
        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE true AND n.nspname = 'analytics' AND ct.relname = 'events' AND i.indisprimary) result
    where result.A_ATTNUM = (result.KEYS).x
    ORDER BY result.table_name, result.pk_name, result.key_seq
    """

    assert %{errors: [], results: [%{rows: []}]} = PgClient.query(socket, primary_keys)
  end

  test "Metabase's foreign-key sync and schema filter bind (T-412)", %{socket: socket} do
    fks = """
    SELECT tc.table_schema AS fk_table_schema, tc.table_name AS fk_table_name,
      kcu.column_name AS fk_column_name, ccu.table_schema AS pk_table_schema,
      ccu.table_name AS pk_table_name, ccu.column_name AS pk_column_name
    FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
      JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    """

    assert %{errors: [], results: [%{rows: []}]} = PgClient.query(socket, fks)

    constraints = """
    SELECT fk_ns.nspname AS "fk-table-schema", fk_table.relname AS "fk-table-name",
      fk_column.attname AS "fk-column-name", pk_ns.nspname AS "pk-table-schema",
      pk_table.relname AS "pk-table-name", pk_column.attname AS "pk-column-name"
    FROM pg_constraint c
      JOIN pg_class fk_table ON c.conrelid = fk_table.oid
      JOIN pg_namespace fk_ns ON c.connamespace = fk_ns.oid
      JOIN pg_attribute fk_column ON c.conrelid = fk_column.attrelid AND fk_column.attnum = ANY(c.conkey)
      JOIN pg_class pk_table ON c.confrelid = pk_table.oid
      JOIN pg_namespace pk_ns ON pk_table.relnamespace = pk_ns.oid
      JOIN pg_attribute pk_column ON c.confrelid = pk_column.attrelid AND pk_column.attnum = ANY(c.confkey)
    WHERE c.contype = 'f' AND fk_ns.nspname !~ '^information_schema|catalog_history|pg_'
    """

    assert %{errors: [], results: [%{rows: []}]} = PgClient.query(socket, constraints)

    schemas = """
    SELECT schema_name FROM information_schema.schemata
    WHERE has_schema_privilege(schema_name, 'USAGE')
      AND schema_name NOT IN ('pg_catalog', 'information_schema') ORDER BY schema_name
    """

    assert %{errors: [], results: [%{rows: rows}]} = PgClient.query(socket, schemas)
    assert ["analytics"] in rows
  end

  test "DBeaver's table read and view definition bind (T-412)", %{socket: socket} do
    assert %{results: [%{rows: [[oid]]}]} =
             PgClient.query(socket, "SELECT oid FROM pg_class WHERE relname = 'events'")

    tables = """
    SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,
      pg_catalog.pg_get_partkeydef(c.oid) as partition_key
    FROM pg_catalog.pg_class c
      LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0
        AND d.classoid='pg_class'::regclass
    WHERE c.relkind not in ('i','I','c') AND c.oid = #{oid}
    """

    assert %{errors: [], results: [%{columns: columns, rows: [row]}]} =
             PgClient.query(socket, tables)

    names = Enum.map(columns, & &1.name)
    assert "relispopulated" in names and "relacl" in names and "reloptions" in names
    assert length(row) == length(names)

    assert %{errors: [], results: [%{rows: [[nil]]}]} =
             PgClient.query(socket, "SELECT pg_catalog.pg_get_viewdef(#{oid}, true)")

    assert %{errors: [], results: [%{rows: []}]} =
             PgClient.query(
               socket,
               "SELECT * FROM pg_catalog.pg_views WHERE schemaname = 'analytics'"
             )

    assert %{errors: [], results: [%{rows: [["analytics", "events" | _]]}]} =
             PgClient.query(socket, "SELECT * FROM pg_tables WHERE schemaname = 'analytics'")
  end

  test "psqlODBC's SQLColumns (Tableau) and its session probes bind (T-412)", %{socket: socket} do
    columns = """
    select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod,
      a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid),
      case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, attidentity
    from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace
      and c.relname like 'events' and n.nspname like 'analytics')
      inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid)
      inner join pg_catalog.pg_type t on t.oid = a.atttypid)
      left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum
    order by n.nspname, c.relname, attnum
    """

    assert %{errors: [], results: [%{rows: rows}]} = PgClient.query(socket, columns)
    assert [["analytics", "events", "id", "20", "int8", "1", "8" | _] | _rest] = rows

    assert %{errors: [], results: [%{rows: [[xid]]}]} =
             PgClient.query(socket, "SELECT txid_current()")

    assert {_xid, ""} = Integer.parse(xid)

    assert %{errors: [], results: [%{rows: [["32"]]}]} =
             PgClient.query(socket, "SELECT current_setting('max_index_keys')::int")

    assert %{errors: [], results: [%{rows: [["32"]]}]} =
             PgClient.query(
               socket,
               "SELECT setting FROM pg_settings WHERE name = 'max_index_keys'"
             )

    assert %{errors: [], results: [%{rows: [["off"]]}]} =
             PgClient.query(socket, "SELECT set_config('search_path', 'off', false)")
  end

  test "a user query still reaches the query service", %{socket: socket} do
    assert %{results: [%{rows: [["2"]]}]} = PgClient.query(socket, "SELECT 1 + 1")

    assert %{errors: [%{"C" => "42P01"}]} =
             PgClient.query(socket, "SELECT * FROM analytics.missing")
  end
end
