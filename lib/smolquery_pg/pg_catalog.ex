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
    `pg_trigger` answers no rows instead of no relation. A few carry the
    rows a driver expects of any server: `pg_database`, `pg_roles` and
    `pg_authid` (one role), `pg_language`, and `pg_settings`, built from
    the same two maps `current_setting` reads — the server settings in
    `SmolqueryPg.PgCatalog.Rewrite` and the session defaults in
    `SmolqueryPg.Session` — so the table and the function agree.
  - **Generated tables**, rebuilt from `Smolquery.Catalog` when a query
    arrives and the last build is older than `@refresh_ttl_ms`:
    `pg_namespace` from the datasets, `pg_class` and `pg_attribute` from
    the tables and their schemas — with the full column shape a tool's
    `c.*` reads. OIDs are stable hashes of the names, so the OID `psql`
    reads in one query still resolves in its next. The views over them
    rebuild with them: `information_schema`'s `tables`, `schemata`,
    `columns` (with `udt_name`, precision, scale, and the identity and
    generation columns pgjdbc and psqlODBC read), `table_privileges`,
    and `pg_tables`, `pg_stat_all_tables`, `pg_stat_user_tables`.
  - **Empty views** for what smolquery has none of — constraints, views,
    sequences, routines, indexes (`information_schema.table_constraints`,
    `key_column_usage`, `referential_constraints`, `views`, `sequences`,
    `routines`, `pg_views`, `pg_indexes`, ...) — so a BI tool's schema
    sync binds and finds nothing, rather than failing on a missing
    relation (T-412).
  - **Macros** for the functions the corpus calls: `version()`,
    `format_type`, `pg_get_expr`, `pg_table_is_visible`, the
    `has_*_privilege` family (always true), `pg_get_viewdef`,
    `txid_current`, `to_regclass`, `information_schema._pg_expandarray`
    (as `is__pg_expandarray`, the name the rewrite yields), and the rest.

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
  alias SmolqueryPg.Session

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
    create_static_views(engine)

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
         {:ok, frame} <- run(state.engine, Rewrite.post(canonical), params) do
      {:reply, {:ok, columns(frame), rows(frame)}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp run(engine, sql, params) do
    case Engine.frame(engine, sql, params) do
      {:error, reason} = error ->
        if duplicate_columns?(reason), do: deduplicated(engine, sql, params), else: error

      ok ->
        ok
    end
  end

  defp duplicate_columns?(%{message: message}), do: duplicate_columns?(message)
  defp duplicate_columns?(message) when is_binary(message), do: message =~ "duplicate"
  defp duplicate_columns?(_reason), do: false

  defp deduplicated(engine, sql, params) do
    with {:ok, %{rows: rows}} <- Engine.query(engine, "DESCRIBE " <> sql, params) do
      {aliases, _seen} =
        rows
        |> Enum.map(&hd/1)
        |> Enum.with_index(1)
        |> Enum.map_reduce(MapSet.new(), fn {name, position}, seen ->
          alias = unique_alias(name, seen)

          {"##{position} AS #{Identifier.quote_label(alias)}", MapSet.put(seen, alias)}
        end)

      Engine.frame(
        engine,
        IO.iodata_to_binary([
          "SELECT ",
          Enum.intersperse(aliases, ", "),
          " FROM (",
          sql,
          ") AS q"
        ]),
        params
      )
    end
  end

  defp unique_alias(name, seen), do: unique_alias(name, seen, 0)

  defp unique_alias(name, seen, 0) do
    if MapSet.member?(seen, name), do: unique_alias(name, seen, 1), else: name
  end

  defp unique_alias(name, seen, n) do
    candidate = "#{name}_#{n}"

    if MapSet.member?(seen, candidate), do: unique_alias(name, seen, n + 1), else: candidate
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

    Engine.query!(engine, "ALTER TABLE pg_type ADD COLUMN typtypmod BIGINT DEFAULT -1")
    Engine.query!(engine, "ALTER TABLE pg_type ADD COLUMN typndims BIGINT DEFAULT 0")
    Engine.query!(engine, "ALTER TABLE pg_type ADD COLUMN typdefault VARCHAR DEFAULT NULL")

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

    Engine.query!(engine, """
    INSERT INTO pg_authid (oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
                           rolcanlogin, rolreplication, rolbypassrls, rolconnlimit)
    VALUES (10, 'smolquery', FALSE, TRUE, FALSE, FALSE, TRUE, FALSE, FALSE, -1)
    """)

    Engine.query!(engine, """
    INSERT INTO pg_language (oid, lanname, lanowner, lanispl, lanpltrusted)
    VALUES (12, 'internal', 10, FALSE, FALSE), (13, 'c', 10, FALSE, FALSE),
           (14, 'sql', 10, FALSE, TRUE)
    """)

    Engine.query!(
      engine,
      "INSERT INTO pg_settings (name, setting, category, context, vartype, source, " <>
        "boot_val, reset_val, pending_restart) VALUES " <>
        Enum.map_join(settings_rows(), ", ", &setting_row/1)
    )
  end

  defp settings_rows do
    server =
      Enum.map(Rewrite.server_settings(), fn {name, value} ->
        {name, value, "Preset Options", "internal"}
      end)

    session =
      Enum.map(Session.defaults(), fn {name, value} ->
        {name, value, "Client Connection Defaults", "user"}
      end)

    server ++ session
  end

  defp setting_row({name, value, category, context}) do
    fields = [name, value, category, context, vartype(value), "default", value, value]

    "(" <> Enum.map_join(fields, ", ", &Identifier.sql_string/1) <> ", FALSE)"
  end

  defp vartype(value) do
    cond do
      value in ["on", "off"] -> "bool"
      match?({_integer, ""}, Integer.parse(value)) -> "integer"
      true -> "string"
    end
  end

  @empty_views %{
    "is_views" =>
      ~w(table_catalog table_schema table_name view_definition check_option is_updatable is_insertable_into is_trigger_updatable is_trigger_deletable is_trigger_insertable_into),
    "is_table_constraints" =>
      ~w(constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name constraint_type is_deferrable initially_deferred enforced),
    "is_key_column_usage" =>
      ~w(constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name column_name ordinal_position:BIGINT position_in_unique_constraint:BIGINT),
    "is_referential_constraints" =>
      ~w(constraint_catalog constraint_schema constraint_name unique_constraint_catalog unique_constraint_schema unique_constraint_name match_option update_rule delete_rule),
    "is_constraint_column_usage" =>
      ~w(table_catalog table_schema table_name column_name constraint_catalog constraint_schema constraint_name),
    "is_check_constraints" =>
      ~w(constraint_catalog constraint_schema constraint_name check_clause),
    "is_sequences" =>
      ~w(sequence_catalog sequence_schema sequence_name data_type numeric_precision:BIGINT numeric_precision_radix:BIGINT numeric_scale:BIGINT start_value minimum_value maximum_value increment cycle_option),
    "is_routines" =>
      ~w(specific_catalog specific_schema specific_name routine_catalog routine_schema routine_name routine_type data_type type_udt_catalog type_udt_schema type_udt_name routine_body routine_definition external_name external_language is_deterministic sql_data_access is_null_call security_type),
    "is_parameters" =>
      ~w(specific_catalog specific_schema specific_name ordinal_position:BIGINT parameter_mode is_result as_locator parameter_name data_type udt_catalog udt_schema udt_name parameter_default),
    "pg_views" => ~w(schemaname viewname viewowner definition),
    "pg_indexes" => ~w(schemaname tablename indexname tablespace indexdef)
  }

  defp create_static_views(engine) do
    empty =
      Enum.map(@empty_views, fn {view, columns} ->
        "CREATE VIEW #{view} AS SELECT " <> empty_columns(columns) <> " WHERE FALSE"
      end)

    statements =
      empty ++
        [
          "CREATE VIEW is_character_sets AS SELECT CAST(NULL AS VARCHAR) AS character_set_catalog, " <>
            "CAST(NULL AS VARCHAR) AS character_set_schema, 'UTF8' AS character_set_name, " <>
            "'UCS' AS character_repertoire, 'UTF8' AS form_of_use, " <>
            "'smolquery' AS default_collate_catalog, 'pg_catalog' AS default_collate_schema, " <>
            "'default' AS default_collate_name",
          "CREATE VIEW is_collations AS SELECT 'smolquery' AS collation_catalog, " <>
            "'pg_catalog' AS collation_schema, collname AS collation_name, " <>
            "'NO PAD' AS pad_attribute FROM pg_collation"
        ]

    Enum.each(statements, &Engine.query!(engine, &1))
  end

  defp empty_columns(columns) do
    Enum.map_join(columns, ", ", fn column ->
      case String.split(column, ":") do
        [name, type] -> "CAST(NULL AS #{type}) AS #{name}"
        [name] -> "CAST(NULL AS VARCHAR) AS #{name}"
      end
    end)
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
      "CREATE MACRO pg_postmaster_start_time() AS now()",
      "CREATE MACRO pg_conf_load_time() AS now()",
      "CREATE MACRO pg_get_viewdef(o) AS CAST(NULL AS VARCHAR), (o, p) AS CAST(NULL AS VARCHAR), " <>
        "(o, p, w) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_indexdef(o) AS CAST(NULL AS VARCHAR), (o, n) AS CAST(NULL AS VARCHAR), " <>
        "(o, n, p) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_partkeydef(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_serial_sequence(t, c) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_functiondef(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_function_arguments(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_function_result(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_function_identity_arguments(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_get_ruledef(o) AS CAST(NULL AS VARCHAR), (o, p) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_relation_filepath(o) AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO pg_tablespace_location(o) AS ''",
      "CREATE MACRO pg_database_size(d) AS CAST(0 AS BIGINT)",
      "CREATE MACRO pg_is_in_recovery() AS FALSE",
      "CREATE MACRO pg_is_other_temp_schema(o) AS FALSE",
      "CREATE MACRO pg_current_xact_id() AS txid_current()",
      "CREATE MACRO set_config(n, v, l) AS v",
      "CREATE MACRO pg_typeof(x) AS typeof(x)",
      "CREATE MACRO inet_server_addr() AS CAST(NULL AS VARCHAR)",
      "CREATE MACRO inet_server_port() AS 5432",
      "CREATE MACRO has_schema_privilege(s, p) AS TRUE, (u, s, p) AS TRUE",
      "CREATE MACRO has_table_privilege(t, p) AS TRUE, (u, t, p) AS TRUE",
      "CREATE MACRO has_any_column_privilege(t, p) AS TRUE, (u, t, p) AS TRUE",
      "CREATE MACRO has_column_privilege(t, c, p) AS TRUE, (u, t, c, p) AS TRUE",
      "CREATE MACRO has_database_privilege(d, p) AS TRUE, (u, d, p) AS TRUE",
      "CREATE MACRO has_sequence_privilege(s, p) AS TRUE, (u, s, p) AS TRUE",
      "CREATE MACRO has_function_privilege(f, p) AS TRUE, (u, f, p) AS TRUE",
      "CREATE MACRO has_language_privilege(l, p) AS TRUE, (u, l, p) AS TRUE",
      "CREATE MACRO has_tablespace_privilege(t, p) AS TRUE, (u, t, p) AS TRUE",
      "CREATE MACRO pg_has_role(r, p) AS TRUE, (u, r, p) AS TRUE",
      "CREATE MACRO to_regclass(n) AS (SELECT c.oid FROM pg_class c " <>
        "LEFT JOIN pg_namespace ns ON ns.oid = c.relnamespace " <>
        "WHERE c.relname = regexp_extract(n, '([^.]+)$', 1) " <>
        "ORDER BY ns.nspname = regexp_extract(n, '^([^.]+)\\.', 1) DESC NULLS LAST, c.oid " <>
        "LIMIT 1)",
      "CREATE MACRO to_regtype(n) AS (SELECT oid FROM pg_type " <>
        "WHERE typname = regexp_extract(n, '([^.]+)$', 1) ORDER BY oid LIMIT 1)",
      "CREATE MACRO is__pg_expandarray(a) AS " <>
        "unnest(list_transform(a, lambda v, i: {'x': v, 'n': i}))"
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
        "'BASE TABLE' AS table_type, CAST(NULL AS VARCHAR) AS self_referencing_column_name, " <>
        "CAST(NULL AS VARCHAR) AS reference_generation, " <>
        "CAST(NULL AS VARCHAR) AS user_defined_type_catalog, " <>
        "CAST(NULL AS VARCHAR) AS user_defined_type_schema, " <>
        "CAST(NULL AS VARCHAR) AS user_defined_type_name, 'NO' AS is_insertable_into, " <>
        "'NO' AS is_typed, CAST(NULL AS VARCHAR) AS commit_action " <>
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
      "CREATE OR REPLACE VIEW is_schemata AS " <>
        "SELECT 'smolquery' AS catalog_name, nspname AS schema_name, 'smolquery' AS schema_owner, " <>
        "CAST(NULL AS VARCHAR) AS default_character_set_catalog, " <>
        "CAST(NULL AS VARCHAR) AS default_character_set_schema, " <>
        "CAST(NULL AS VARCHAR) AS default_character_set_name, CAST(NULL AS VARCHAR) AS sql_path " <>
        "FROM pg_namespace",
      "CREATE OR REPLACE VIEW is_columns AS " <>
        "SELECT 'smolquery' AS table_catalog, n.nspname AS table_schema, c.relname AS table_name, " <>
        "a.attname AS column_name, CAST(a.attnum AS BIGINT) AS ordinal_position, " <>
        "CAST(NULL AS VARCHAR) AS column_default, " <>
        "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, " <>
        "format_type(a.atttypid, a.atttypmod) AS data_type, " <>
        "CAST(CASE WHEN a.atttypid = 1043 AND a.atttypmod >= 4 THEN a.atttypmod - 4 END AS BIGINT) " <>
        "AS character_maximum_length, " <>
        "CAST(CASE WHEN a.atttypid IN (25, 1043, 1042) THEN 1073741824 END AS BIGINT) " <>
        "AS character_octet_length, " <>
        "CAST(CASE a.atttypid WHEN 20 THEN 64 WHEN 701 THEN 53 WHEN 1700 THEN (a.atttypmod - 4) >> 16 END " <>
        "AS BIGINT) AS numeric_precision, " <>
        "CAST(CASE a.atttypid WHEN 20 THEN 2 WHEN 701 THEN 2 WHEN 1700 THEN 10 END AS BIGINT) " <>
        "AS numeric_precision_radix, " <>
        "CAST(CASE a.atttypid WHEN 20 THEN 0 WHEN 1700 THEN (a.atttypmod - 4) & 65535 END AS BIGINT) " <>
        "AS numeric_scale, " <>
        "CAST(CASE a.atttypid WHEN 1114 THEN 6 WHEN 1184 THEN 6 WHEN 1082 THEN 0 END AS BIGINT) " <>
        "AS datetime_precision, CAST(NULL AS VARCHAR) AS interval_type, " <>
        "CAST(NULL AS VARCHAR) AS collation_name, CAST(NULL AS VARCHAR) AS domain_name, " <>
        "'smolquery' AS udt_catalog, 'pg_catalog' AS udt_schema, t.typname AS udt_name, " <>
        "CAST(a.attnum AS VARCHAR) AS dtd_identifier, 'NO' AS is_self_referencing, " <>
        "'NO' AS is_identity, CAST(NULL AS VARCHAR) AS identity_generation, " <>
        "CAST(NULL AS VARCHAR) AS identity_start, CAST(NULL AS VARCHAR) AS identity_increment, " <>
        "CAST(NULL AS VARCHAR) AS identity_maximum, CAST(NULL AS VARCHAR) AS identity_minimum, " <>
        "CAST(NULL AS VARCHAR) AS identity_cycle, 'NEVER' AS is_generated, " <>
        "CAST(NULL AS VARCHAR) AS generation_expression, 'YES' AS is_updatable " <>
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid " <>
        "JOIN pg_namespace n ON n.oid = c.relnamespace " <>
        "LEFT JOIN pg_type t ON t.oid = a.atttypid WHERE a.attnum > 0",
      "CREATE OR REPLACE VIEW is_table_privileges AS " <>
        "SELECT 'smolquery' AS grantor, 'smolquery' AS grantee, 'smolquery' AS table_catalog, " <>
        "n.nspname AS table_schema, c.relname AS table_name, 'SELECT' AS privilege_type, " <>
        "'YES' AS is_grantable, 'YES' AS with_hierarchy " <>
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
      "CREATE OR REPLACE VIEW pg_tables AS " <>
        "SELECT n.nspname AS schemaname, c.relname AS tablename, 'smolquery' AS tableowner, " <>
        "CAST(NULL AS VARCHAR) AS tablespace, c.relhasindex AS hasindexes, " <>
        "c.relhasrules AS hasrules, c.relhastriggers AS hastriggers, " <>
        "c.relrowsecurity AS rowsecurity " <>
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
      "CREATE OR REPLACE VIEW pg_stat_all_tables AS " <>
        "SELECT c.oid AS relid, n.nspname AS schemaname, c.relname AS relname, " <>
        "CAST(0 AS BIGINT) AS seq_scan, CAST(0 AS BIGINT) AS seq_tup_read, " <>
        "CAST(0 AS BIGINT) AS idx_scan, CAST(0 AS BIGINT) AS idx_tup_fetch, " <>
        "CAST(0 AS BIGINT) AS n_tup_ins, CAST(0 AS BIGINT) AS n_tup_upd, " <>
        "CAST(0 AS BIGINT) AS n_tup_del, CAST(0 AS BIGINT) AS n_tup_hot_upd, " <>
        "CAST(0 AS BIGINT) AS n_live_tup, CAST(0 AS BIGINT) AS n_dead_tup, " <>
        "CAST(0 AS BIGINT) AS n_mod_since_analyze, CAST(0 AS BIGINT) AS n_ins_since_vacuum, " <>
        "CAST(NULL AS TIMESTAMP) AS last_vacuum, CAST(NULL AS TIMESTAMP) AS last_autovacuum, " <>
        "CAST(NULL AS TIMESTAMP) AS last_analyze, CAST(NULL AS TIMESTAMP) AS last_autoanalyze, " <>
        "CAST(0 AS BIGINT) AS vacuum_count, CAST(0 AS BIGINT) AS autovacuum_count, " <>
        "CAST(0 AS BIGINT) AS analyze_count, CAST(0 AS BIGINT) AS autoanalyze_count " <>
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
      "CREATE OR REPLACE VIEW pg_stat_user_tables AS SELECT * FROM pg_stat_all_tables"
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
          "0 AS relpages, 0 AS relfilenode, 0 AS reltype, 0 AS relallvisible, " <>
          "FALSE AS relisshared, FALSE AS relhassubclass, TRUE AS relispopulated, " <>
          "0 AS relfrozenxid, 0 AS relminmxid, 0 AS relrewrite, " <>
          "CAST(NULL AS VARCHAR[]) AS relacl, CAST(NULL AS VARCHAR[]) AS reloptions, " <>
          "CAST(NULL AS VARCHAR) AS relpartbound"
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
        {attlen, attbyval, attalign, attstorage} = attribute_layout(field.type)

        "SELECT #{relation_oid(dataset, table)} AS attrelid, " <>
          "#{Identifier.sql_string(field.name)} AS attname, " <>
          "#{atttypid} AS atttypid, #{atttypmod} AS atttypmod, #{index} AS attnum, " <>
          "#{not field.nullable} AS attnotnull, FALSE AS attisdropped, " <>
          "FALSE AS atthasdef, 0 AS attcollation, CAST('' AS VARCHAR) AS attidentity, " <>
          "CAST('' AS VARCHAR) AS attgenerated, 0 AS attndims, " <>
          "#{attlen} AS attlen, #{attbyval} AS attbyval, " <>
          "CAST('#{attalign}' AS VARCHAR) AS attalign, " <>
          "CAST('#{attstorage}' AS VARCHAR) AS attstorage, " <>
          "CAST('' AS VARCHAR) AS attcompression, -1 AS attstattarget, -1 AS attcacheoff, " <>
          "FALSE AS atthasmissing, TRUE AS attislocal, 0 AS attinhcount, " <>
          "CAST(NULL AS VARCHAR[]) AS attacl, CAST(NULL AS VARCHAR[]) AS attoptions, " <>
          "CAST(NULL AS VARCHAR[]) AS attfdwoptions, CAST(NULL AS VARCHAR[]) AS attmissingval"
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

  defp attribute_layout(:int64), do: {8, true, "d", "p"}
  defp attribute_layout(:float64), do: {8, true, "d", "p"}
  defp attribute_layout(:bool), do: {1, true, "c", "p"}
  defp attribute_layout(:timestamp), do: {8, true, "d", "p"}
  defp attribute_layout(:date), do: {4, true, "i", "p"}
  defp attribute_layout(_varlena), do: {-1, false, "i", "x"}

  defp namespace_oid(dataset), do: @namespace_base + stable_oid(dataset)

  defp relation_oid(dataset, table), do: @relation_base + stable_oid({dataset, table})

  defp stable_oid(term), do: :erlang.phash2(term, @oid_span)
end
