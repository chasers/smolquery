defmodule SmolqueryPg.PgCatalog do
  @moduledoc """
  The `pg_catalog` a Postgres client reads, emulated in a DuckDB engine
  (PL-58 layer 3).

  Clients read the catalog constantly: Postgrex bootstraps its type table
  from `pg_type` at connect, `psql`'s backslash commands join half a dozen
  catalogs, `postgres_fdw`'s `IMPORT FOREIGN SCHEMA` reads six. The OIDs
  they key on are Postgres's own, so DuckDB's built-in `pg_catalog` cannot
  serve them — its type OIDs are DuckDB's.

  One `Smolquery.Engine` per edge holds the emulation in its `main` schema,
  where an unqualified `pg_class` resolves before DuckDB's built-in one:

  - **Static fixtures** from a real Postgres, loaded once from
    `priv/pg_catalog/` (`fixture_dir/0`, resolved at runtime: a compile-time
    lookup bakes the builder's `_build` path into the release, and the edge
    then dies at boot): `pg_type` (with each type's `format_type` text),
    `pg_range`, `pg_collation`, and the column shapes of the catalog tables
    the corpus touches — those load empty, so a join against `pg_index` or
    `pg_trigger` answers no rows instead of no relation.
  - **Generated tables**, rebuilt from `Smolquery.Catalog` when a query
    arrives and the last build is older than `@refresh_ttl_ms`:
    `pg_namespace` from the datasets, `pg_class` and `pg_attribute` from
    the tables and their schemas. OIDs are stable hashes of the names, so
    the OID `psql` reads in one query still resolves in its next.
  - **Macros** for the functions the corpus calls: `version()`,
    `format_type`, `pg_get_expr`, `pg_table_is_visible`, and the rest.

  Every query is rewritten by `SmolqueryPg.PgCatalog.Rewrite` first, and
  every call runs through this server, so a refresh never interleaves with
  a read.

  All calls take the edge's instance name; the server and its engine derive
  from it (`SmolqueryPg.Runtime.pg_catalog/1`).
  """

  use GenServer

  alias Explorer.DataFrame
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.Frame
  alias Smolquery.Identifier
  alias SmolqueryPg.PgCatalog.Rewrite
  alias SmolqueryPg.Runtime

  @refresh_ttl_ms 1_000
  @call_timeout_ms 30_000
  @namespace_base 16_000
  @relation_base 100_000
  @oid_span 1_000_000_000

  @doc false
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.pg_catalog(runtime.name))
  end

  @doc false
  def child_spec(runtime), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [runtime]}}

  @doc """
  The directory of the static catalog fixtures, under the running
  application's `priv/`.
  """
  @spec fixture_dir() :: Path.t()
  def fixture_dir, do: Application.app_dir(:smolquery, "priv/pg_catalog")

  @doc """
  Whether `sql` is a catalog query: every base table it references lives in
  `pg_catalog` or `information_schema` (a bare `pg_*` name counts — the
  corpus queries unqualified under `search_path = pg_catalog`), or it
  references no table and calls a session function only the emulation
  serves. Statements that never mention `pg_` or `information_schema` are
  answered without asking the server.

  Classification runs the statement through `Rewrite.pre/2` and DuckDB's
  own parser (`json_serialize_sql`): the corpus is written in Postgres
  dialect that the parser refuses raw, and a statement the parser refuses
  would otherwise fall through to the query service and fail there. The
  same parse yields the canonical SQL the query path runs
  (`json_deserialize_sql`), so the emulated dialect is whatever DuckDB's
  parser accepts — regex touches only what it cannot parse
  (`SmolqueryPg.PgCatalog.Rewrite`).
  """
  @spec catalog_statement?(atom(), String.t()) :: boolean()
  def catalog_statement?(name, sql) do
    mentions_catalog?(sql) and
      GenServer.call(Runtime.pg_catalog(name), {:classify, sql}, @call_timeout_ms)
  catch
    :exit, _reason -> false
  end

  @doc """
  Runs a catalog query and answers its columns and rows, the terms the
  session renders. `settings` supplies the session values the rewrite
  inlines.
  """
  @spec query(atom(), String.t(), %{String.t() => String.t()}, [term()]) ::
          {:ok, [{String.t(), term(), boolean()}], [map()]} | {:error, term()}
  def query(name, sql, settings, params \\ []) do
    GenServer.call(Runtime.pg_catalog(name), {:query, sql, settings, params}, @call_timeout_ms)
  catch
    :exit, reason -> {:error, {:pg_catalog_unavailable, reason}}
  end

  @session_keywords ~w(version( current_setting current_schema current_database current_user
                       current_role session_user set_config)

  defp mentions_catalog?(<<>>), do: false

  defp mentions_catalog?(<<char, rest::binary>> = sql) do
    case char do
      c when c in ~c(pP) -> keyword_at?(sql, "pg_") or mentions_catalog?(rest)
      c when c in ~c(iI) -> keyword_at?(sql, "information_schema") or mentions_catalog?(rest)
      c when c in ~c(vVcCsS) -> session_keyword_at?(sql) or mentions_catalog?(rest)
      _other -> mentions_catalog?(rest)
    end
  end

  defp session_keyword_at?(sql), do: Enum.any?(@session_keywords, &keyword_at?(sql, &1))

  defp keyword_at?(sql, keyword) do
    size = byte_size(keyword)

    case sql do
      <<head::binary-size(^size), _rest::binary>> -> String.downcase(head) == keyword
      _short -> false
    end
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    engine = Runtime.catalog_engine(runtime.name)
    {:ok, _pid} = Engine.start_link(name: engine)

    load_fixtures(engine)
    create_macros(engine)
    create_static_rows(engine)

    {:ok, %{runtime: runtime, engine: engine, refreshed_at: 0}}
  end

  @impl GenServer
  def handle_call({:classify, sql}, _from, state) do
    reply =
      case serialize(state.engine, Rewrite.pre(sql, %{})) do
        {:ok, ast, _canonical} -> classify_refs(base_tables(ast))
        {:error, _reason} -> false
      end

    {:reply, reply, state}
  end

  def handle_call({:query, sql, settings, params}, _from, state) do
    state = ensure_fresh(state)

    with {:ok, _ast, canonical} <- serialize(state.engine, Rewrite.pre(sql, settings)),
         {:ok, frame} <- Engine.frame(state.engine, Rewrite.post(canonical), params) do
      {:reply, {:ok, columns(frame), rows(frame)}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp columns(frame) do
    dtypes = DataFrame.dtypes(frame)

    Enum.map(DataFrame.names(frame), &{&1, Map.fetch!(dtypes, &1), false})
  end

  defp rows(frame), do: Frame.to_rows(frame)

  defp serialize(engine, sql) do
    quoted = Identifier.sql_string(sql)

    with {:ok, result} <-
           Engine.query(
             engine,
             "SELECT json_serialize_sql(#{quoted}), " <>
               "CASE WHEN json_extract_string(json_serialize_sql(#{quoted}), '$.error') = 'false' " <>
               "THEN json_deserialize_sql(json_serialize_sql(#{quoted})) END"
           ),
         [[json, canonical]] <- result.rows,
         {:ok, %{"error" => false} = ast} <- JSON.decode(json) do
      {:ok, ast, canonical}
    else
      {:ok, %{"error" => true} = ast} ->
        {:error, {:invalid_query, Map.get(ast, "error_message", "unparseable")}}

      {:error, reason} ->
        {:error, reason}

      _unexpected ->
        {:error, :unparseable}
    end
  end

  defp classify_refs([]), do: true

  defp classify_refs(refs) do
    Enum.all?(refs, fn %{"schema_name" => schema, "table_name" => table} ->
      String.downcase(schema) in ["pg_catalog", "information_schema"] or
        (schema == "" and String.starts_with?(String.downcase(table), "pg_"))
    end)
  end

  defp base_tables(%{"type" => "BASE_TABLE"} = node), do: [node | child_tables(node)]
  defp base_tables(node) when is_map(node), do: child_tables(node)
  defp base_tables(node) when is_list(node), do: Enum.flat_map(node, &base_tables/1)
  defp base_tables(_leaf), do: []

  defp child_tables(node), do: Enum.flat_map(Map.values(node), &base_tables/1)

  defp ensure_fresh(state) do
    now = System.monotonic_time(:millisecond)

    if now - state.refreshed_at > @refresh_ttl_ms or state.refreshed_at == 0 do
      refresh(state.engine, state.runtime.catalog)

      %{state | refreshed_at: now}
    else
      state
    end
  end

  defp load_fixtures(engine) do
    for {file, name} <- [
          {"pg_type.csv", "pg_type"},
          {"pg_range.csv", "pg_range"},
          {"pg_collation.csv", "pg_collation"}
        ] do
      path = Path.join(fixture_dir(), file)

      Engine.query!(
        engine,
        "CREATE TABLE #{name} AS SELECT * FROM read_csv(#{Identifier.sql_string(path)}, header = true)"
      )
    end

    for {table, shape_columns} <- shapes() do
      columns = Enum.map_join(shape_columns, ", ", fn {column, type} -> "#{column} #{type}" end)

      Engine.query!(engine, "CREATE TABLE IF NOT EXISTS #{table} (#{columns})")
    end
  end

  defp shapes do
    Path.join(fixture_dir(), "catalog_shapes.csv")
    |> File.stream!()
    |> Stream.drop(1)
    |> Enum.reduce(%{}, fn line, acc ->
      [table, column, _attnum, pg_type] = parse_shape_line(line)

      Map.update(
        acc,
        table,
        [{column, duckdb_type(pg_type)}],
        &(&1 ++ [{column, duckdb_type(pg_type)}])
      )
    end)
    |> Map.drop(["pg_type", "pg_range", "pg_collation"])
  end

  defp parse_shape_line(line) do
    line
    |> String.trim_trailing("\n")
    |> String.split(",", parts: 4)
    |> case do
      [table, column, attnum, type] -> [table, column, attnum, String.trim(type, "\"")]
    end
  end

  @shape_scalars %{
    "oid" => "BIGINT",
    "xid" => "BIGINT",
    "cid" => "BIGINT",
    "tid" => "BIGINT",
    "regproc" => "BIGINT",
    "regtype" => "BIGINT",
    "smallint" => "BIGINT",
    "integer" => "BIGINT",
    "bigint" => "BIGINT",
    "int2vector" => "BIGINT[]",
    "oidvector" => "BIGINT[]",
    "boolean" => "BOOLEAN",
    "real" => "DOUBLE",
    "double precision" => "DOUBLE",
    "anyarray" => "VARCHAR[]",
    "aclitem" => "VARCHAR[]"
  }
  @numeric_array_bases ~w(oid xid cid smallint integer bigint real)

  defp duckdb_type(pg_type) do
    base = pg_type |> String.trim() |> String.downcase()

    cond do
      String.ends_with?(base, "[]") -> array_type(binary_part(base, 0, byte_size(base) - 2))
      scalar = Map.get(@shape_scalars, base) -> scalar
      String.starts_with?(base, "timestamp") -> "TIMESTAMP"
      true -> "VARCHAR"
    end
  end

  defp array_type(base) when base in @numeric_array_bases, do: "BIGINT[]"
  defp array_type(_base), do: "VARCHAR[]"

  defp create_static_rows(engine) do
    Engine.query!(engine, """
    INSERT INTO pg_namespace_static VALUES
      (11, 'pg_catalog', 10, NULL), (13, 'information_schema', 10, NULL), (2200, 'public', 10, NULL)
    """)

    Engine.query!(
      engine,
      "INSERT INTO pg_roles (rolname, oid) VALUES ('smolquery', 10)"
    )

    Engine.query!(engine, """
    INSERT INTO pg_database (oid, datname, datdba, encoding, datcollate, datctype,
                             datistemplate, datallowconn, datconnlimit)
    VALUES (1, 'smolquery', 10, 6, 'C', 'C', FALSE, TRUE, -1)
    """)
  end

  defp create_macros(engine) do
    statements = [
      "CREATE TABLE pg_namespace_static (oid BIGINT, nspname VARCHAR, nspowner BIGINT, nspacl VARCHAR[])",
      """
      CREATE MACRO version() AS
        'PostgreSQL 14.10 (smolquery) on x86_64-smolquery, compiled by smolquery, 64-bit'
      """,
      "CREATE MACRO current_schemas(implicit) AS ['pg_catalog', 'public']",
      """
      CREATE MACRO format_type(t, m) AS (
        SELECT CASE
          WHEN pt.typname = 'numeric' AND m IS NOT NULL AND m >= 4
            THEN 'numeric(' || ((m - 4) >> 16) || ',' || ((m - 4) & 65535) || ')'
          WHEN pt.typname = 'varchar' AND m IS NOT NULL AND m >= 4
            THEN 'character varying(' || (m - 4) || ')'
          ELSE pt.fmt
        END
        FROM pg_type pt
        WHERE pt.oid = t
      )
      """,
      "CREATE MACRO pg_get_expr(a, b) AS CAST(NULL AS VARCHAR), (a, b, c) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_table_is_visible(o) AS TRUE",
      "CREATE MACRO pg_type_is_visible(o) AS TRUE",
      "CREATE MACRO pg_function_is_visible(o) AS TRUE",
      "CREATE MACRO pg_get_userbyid(o) AS 'smolquery'",
      "CREATE MACRO obj_description(o) AS CAST(NULL AS VARCHAR), (o, c) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO shobj_description(o, c) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO col_description(o, n) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_indexdef(o, n, p) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_constraintdef(o) AS CAST(NULL AS VARCHAR), (o, p) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_triggerdef(o, p) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_statisticsobjdef_columns(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_relation_is_publishable(o) AS FALSE",
      "CREATE MACRO pg_encoding_to_char(e) AS 'UTF8'",
      "CREATE MACRO pg_backend_pid() AS 0",
      "CREATE MACRO pg_partition_ancestors(x) AS CAST(NULL AS BIGINT)",
      "CREATE MACRO pg_table_size(o) AS CAST(0 AS BIGINT)",
      "CREATE MACRO pg_indexes_size(o) AS CAST(0 AS BIGINT)",
      "CREATE MACRO pg_relation_size(o) AS CAST(0 AS BIGINT)",
      "CREATE MACRO pg_total_relation_size(o) AS CAST(0 AS BIGINT)",
      "CREATE MACRO pg_size_pretty(b) AS CAST(b AS VARCHAR) || ' bytes'",
      "CREATE MACRO pg_my_temp_schema() AS 0",
      "CREATE MACRO pg_postmaster_start_time() AS now()"
    ]

    Enum.each(statements, &Engine.query!(engine, &1))
  end

  defp refresh(_engine, nil), do: :ok

  defp refresh(engine, catalog) do
    tables = listed_tables(catalog)
    datasets = tables |> Enum.map(fn {dataset, _table, _schema} -> dataset end) |> Enum.uniq()

    Engine.transaction(engine, [
      "CREATE OR REPLACE TABLE pg_namespace AS " <>
        "SELECT * FROM pg_namespace_static" <> namespace_rows(datasets),
      "CREATE OR REPLACE TABLE pg_class AS " <> class_rows(tables),
      "CREATE OR REPLACE TABLE pg_attribute AS " <> attribute_rows(tables),
      "CREATE OR REPLACE VIEW is_tables AS " <>
        "SELECT 'smolquery' AS table_catalog, n.nspname AS table_schema, c.relname AS table_name, " <>
        "'BASE TABLE' AS table_type FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
      "CREATE OR REPLACE VIEW is_schemata AS " <>
        "SELECT 'smolquery' AS catalog_name, nspname AS schema_name, 'smolquery' AS schema_owner " <>
        "FROM pg_namespace",
      "CREATE OR REPLACE VIEW is_columns AS " <>
        "SELECT 'smolquery' AS table_catalog, n.nspname AS table_schema, c.relname AS table_name, " <>
        "a.attname AS column_name, CAST(a.attnum AS BIGINT) AS ordinal_position, " <>
        "format_type(a.atttypid, a.atttypmod) AS data_type, " <>
        "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable " <>
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid " <>
        "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE a.attnum > 0"
    ])
  end

  defp listed_tables(catalog) do
    case Catalog.tables(catalog) do
      {:ok, refs} -> Enum.flat_map(refs, &table_entry(catalog, &1))
      {:error, _reason} -> []
    end
  end

  defp table_entry(catalog, {dataset, table} = ref) do
    case Catalog.table_schema(catalog, ref) do
      {:ok, schema} -> [{dataset, table, schema}]
      {:error, _reason} -> []
    end
  end

  defp namespace_rows(datasets) do
    Enum.map_join(datasets, fn dataset ->
      " UNION ALL SELECT #{namespace_oid(dataset)}, #{Identifier.sql_string(dataset)}, 10, NULL"
    end)
  end

  defp class_rows([]), do: "SELECT * FROM pg_class WHERE FALSE"

  defp class_rows(tables) do
    values =
      Enum.map_join(tables, " UNION ALL ", fn {dataset, table, schema} ->
        "SELECT #{relation_oid(dataset, table)} AS oid, " <>
          "#{Identifier.sql_string(table)} AS relname, " <>
          "#{namespace_oid(dataset)} AS relnamespace, CAST('r' AS VARCHAR) AS relkind, " <>
          "10 AS relowner, 2 AS relam, 0 AS relchecks, FALSE AS relhasindex, " <>
          "FALSE AS relhasrules, FALSE AS relhastriggers, FALSE AS relrowsecurity, " <>
          "FALSE AS relforcerowsecurity, FALSE AS relispartition, 0 AS reloftype, " <>
          "0 AS reltablespace, CAST('p' AS VARCHAR) AS relpersistence, " <>
          "CAST('d' AS VARCHAR) AS relreplident, 0 AS reltoastrelid, " <>
          "#{length(schema.fields)} AS relnatts, CAST(-1 AS DOUBLE) AS reltuples, " <>
          "0 AS relpages, 0 AS relfilenode"
      end)

    values
  end

  defp attribute_rows([]), do: "SELECT * FROM pg_attribute WHERE FALSE"

  defp attribute_rows(tables) do
    Enum.map_join(tables, " UNION ALL ", fn {dataset, table, schema} ->
      schema.fields
      |> Enum.with_index(1)
      |> Enum.map_join(" UNION ALL ", fn {field, index} ->
        {atttypid, atttypmod} = attribute_type(field.type)

        "SELECT #{relation_oid(dataset, table)} AS attrelid, " <>
          "#{Identifier.sql_string(field.name)} AS attname, " <>
          "#{atttypid} AS atttypid, #{atttypmod} AS atttypmod, #{index} AS attnum, " <>
          "#{not field.nullable} AS attnotnull, FALSE AS attisdropped, " <>
          "FALSE AS atthasdef, 0 AS attcollation, CAST('' AS VARCHAR) AS attidentity, " <>
          "CAST('' AS VARCHAR) AS attgenerated, 0 AS attndims"
      end)
    end)
  end

  defp attribute_type({:numeric, precision, scale}),
    do: {1700, Bitwise.bor(Bitwise.bsl(precision, 16), scale) + 4}

  defp attribute_type(:int64), do: {20, -1}
  defp attribute_type(:float64), do: {701, -1}
  defp attribute_type(:string), do: {25, -1}
  defp attribute_type(:bool), do: {16, -1}
  defp attribute_type(:timestamp), do: {1114, -1}
  defp attribute_type(:date), do: {1082, -1}
  defp attribute_type(_map_or_variant), do: {3802, -1}

  defp namespace_oid(dataset), do: @namespace_base + stable_oid(dataset)

  defp relation_oid(dataset, table), do: @relation_base + stable_oid({dataset, table})

  defp stable_oid(term), do: :erlang.phash2(term, @oid_span)
end
