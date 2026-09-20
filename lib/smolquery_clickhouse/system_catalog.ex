defmodule SmolqueryClickHouse.SystemCatalog do
  @moduledoc """
  The `system` database a ClickHouse client reads, with `DESCRIBE`, `SHOW`
  and `EXISTS`, emulated in a DuckDB engine (T-482, T-483).

  A ClickHouse client reads the catalog before it runs anything a user
  typed. HyperDX asks `system.settings` before its first query and fails
  every query after if that one fails; it reads a table's columns with
  `DESCRIBE` and its sorting key from `system.tables`. clickhouse-go sends
  `DESCRIBE TABLE` before each batch it inserts. None of these is a table of
  the lake, so none can go to the query service.

  One `Smolquery.Engine` per edge holds the emulation, the way
  `SmolqueryPg.PgCatalog` holds `pg_catalog`:

  | table | rows |
  |---|---|
  | `system.databases` | the datasets, and `system` |
  | `system.tables` | every table: `engine` is `MergeTree`, `sorting_key` and `primary_key` are the clustering key |
  | `system.columns` | every column, with its ClickHouse type |
  | `system.settings`, `system.data_skipping_indices` | none: smolquery has no setting a client can read and no skip index |
  | `system.table_engines` | `MergeTree` |
  | `system.one` | one row, `dummy = 0` |

  The three generated tables are rebuilt from `Smolquery.Catalog` when a
  statement arrives and the last build is older than `@refresh_ttl_ms`. A
  rebuild that cannot read the catalog answers a retryable failure and
  keeps the last rows; it never answers an empty catalog.

  ## Which statements are the catalog's

  `answer/3` takes a statement whose quoting is already standard.
  `DESCRIBE [TABLE] db.t`, `SHOW DATABASES`, `SHOW TABLES [FROM db]` and
  `EXISTS [TABLE] db.t` are read here and answered from the tables above. A
  `SELECT` is the catalog's when every table it reads is `system.<name>`,
  by DuckDB's own parse (`Smolquery.CatalogEmulation`); it then runs in the
  emulation's engine, with no snapshot, no hot tier and no result cap. A
  `system` table that is not emulated answers code 60 `UNKNOWN_TABLE`. Any
  other statement is `:pass`, the query service's.

  `system.tables` is written `system_tables` before the engine sees it, and
  a bare `table` — a column of `system.columns`, and a reserved word to
  DuckDB — is quoted.

  ## The engine answers the catalog and nothing else

  The job engines that run a user's SQL are locked down by the query
  service. This one runs a client's `SELECT` too, so it is held as tightly,
  by four rules a statement must pass before it runs here:

  - the engine has external access off and its configuration locked, from
    the moment its tables exist, so no statement reads a file or a URL
    whatever reaches it;
  - a statement that names a table function (`read_text`, `range`), in its
    `FROM` or anywhere under it, or a `RECURSIVE` table expression, is not
    the catalog's: it has nothing here to read but a generator, and a
    generator has no end;
  - an answer is cut at `@max_rows`, and a statement is given
    `@statement_timeout_ms`;
  - a statement that outlives that does not take the server with it: the
    call's exit is caught, and the client is told to retry.

  ## Types

  `column_type/1` is the ClickHouse type of a smolquery column, the same one
  a RowBinary insert reads it as.
  """

  use GenServer

  alias Explorer.DataFrame
  alias Smolquery.Catalog
  alias Smolquery.CatalogEmulation
  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.Schema.Field
  alias Smolquery.Sql
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Runtime

  @refresh_ttl_ms 1_000
  @call_timeout_ms 30_000
  @statement_timeout_ms 10_000
  @max_rows 10_000

  @lockdown ["SET enable_external_access = false", "SET lock_configuration = true"]

  @name ~S/("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)/
  @qualified "(?:#{@name}\\s*\\.\\s*)?#{@name}"
  @describe Regex.compile!("\\A\\s*(?:DESCRIBE|DESC)\\s+(?:TABLE\\s+)?#{@qualified}\\s*\\z", "i")
  @exists Regex.compile!("\\A\\s*EXISTS\\s+(?:TABLE\\s+)?#{@qualified}\\s*\\z", "i")
  @show_tables Regex.compile!("\\A\\s*SHOW\\s+TABLES(?:\\s+(?:FROM|IN)\\s+#{@name})?\\s*\\z", "i")
  @show_databases ~r/\A\s*SHOW\s+DATABASES\s*\z/i

  @system_table ~r/(?<![\w."])system\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)/i
  @bare_table ~r/(?<![\w."])table(?![\w("])/i

  @static [
    "CREATE TABLE system_databases (name VARCHAR, engine VARCHAR, data_path VARCHAR, " <>
      "metadata_path VARCHAR, uuid VARCHAR, engine_full VARCHAR, comment VARCHAR)",
    "CREATE TABLE system_tables (database VARCHAR, name VARCHAR, uuid VARCHAR, engine VARCHAR, " <>
      "is_temporary UTINYINT, create_table_query VARCHAR, engine_full VARCHAR, as_select VARCHAR, " <>
      "partition_key VARCHAR, sorting_key VARCHAR, primary_key VARCHAR, sampling_key VARCHAR, " <>
      "storage_policy VARCHAR, total_rows UBIGINT, total_bytes UBIGINT, comment VARCHAR)",
    ~s|CREATE TABLE system_columns (database VARCHAR, "table" VARCHAR, name VARCHAR, type VARCHAR, | <>
      "position UBIGINT, default_kind VARCHAR, default_expression VARCHAR, comment VARCHAR, " <>
      "is_in_partition_key UTINYINT, is_in_sorting_key UTINYINT, is_in_primary_key UTINYINT, " <>
      "is_in_sampling_key UTINYINT, compression_codec VARCHAR)",
    "CREATE TABLE system_settings (name VARCHAR, value VARCHAR, changed UTINYINT, " <>
      ~s|description VARCHAR, min VARCHAR, max VARCHAR, readonly UTINYINT, type VARCHAR, "default" VARCHAR)|,
    ~s|CREATE TABLE system_data_skipping_indices (database VARCHAR, "table" VARCHAR, name VARCHAR, | <>
      "type VARCHAR, type_full VARCHAR, expr VARCHAR, granularity UBIGINT)",
    "CREATE TABLE system_table_engines AS SELECT 'MergeTree' AS name, " <>
      "CAST(1 AS UTINYINT) AS supports_settings, CAST(1 AS UTINYINT) AS supports_sort_order, " <>
      "CAST(1 AS UTINYINT) AS supports_ttl",
    "CREATE TABLE system_one AS SELECT CAST(0 AS UTINYINT) AS dummy"
  ]

  @doc false
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.system_catalog(runtime.name))
  end

  @doc false
  def child_spec(runtime), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [runtime]}}

  @doc """
  Answers `statement` when it is the catalog's.

  `database` is the request's database, which `SHOW TABLES` and an
  unqualified `DESCRIBE` read. `:pass` means the statement is the query
  service's — every statement that mentions no `system.` table and starts
  with none of the catalog's keywords is, without asking the server.
  """
  @spec answer(atom(), String.t(), String.t()) ::
          {:ok, DataFrame.t()} | :pass | {:error, Errors.t()}
  def answer(name, statement, database) do
    if mentions_catalog?(statement),
      do:
        GenServer.call(
          Runtime.system_catalog(name),
          {:answer, statement, database},
          @call_timeout_ms
        ),
      else: :pass
  catch
    :exit, _reason -> {:error, unavailable()}
  end

  @doc """
  The ClickHouse type of a smolquery column.
  """
  @spec column_type(Field.t()) :: String.t()
  def column_type(%Field{type: {:map, :string, :string}}), do: "Map(String, String)"
  def column_type(%Field{type: type, nullable: false}), do: base_type(type)
  def column_type(%Field{type: type}), do: "Nullable(" <> base_type(type) <> ")"

  defp base_type(:int64), do: "Int64"
  defp base_type(:float64), do: "Float64"
  defp base_type(:string), do: "String"
  defp base_type(:bool), do: "Bool"
  defp base_type(:timestamp), do: "DateTime64(6)"
  defp base_type(:timestamp_ns), do: "DateTime64(9)"
  defp base_type(:date), do: "Date32"
  defp base_type({:numeric, precision, scale}), do: "Decimal(#{precision}, #{scale})"
  defp base_type(:variant), do: "String"

  defp mentions_catalog?(statement) do
    Sql.leading_keyword(statement) in ~w(describe desc show exists) or
      Enum.any?(Sql.tokens(statement), fn
        {:code, code} -> Regex.match?(@system_table, code)
        {_kind, _text} -> false
      end)
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    engine = Runtime.catalog_engine(runtime.name)
    {:ok, _pid} = Engine.start_link(name: engine)

    Enum.each(@static ++ @lockdown, &Engine.query!(engine, &1))

    {:ok, %{runtime: runtime, engine: engine, refreshed_at: nil}}
  end

  @impl GenServer
  def handle_call({:answer, statement, database}, _from, state) do
    case read(statement, database) do
      {:select, sql} -> classified(sql, state)
      {:catalog, sql, empty} -> state |> run(sql) |> or_empty(empty)
    end
  end

  defp read(statement, database) do
    cond do
      names = Regex.run(@describe, statement, capture: :all_but_first) ->
        {db, table} = qualified(names, database)

        {:catalog,
         "SELECT name, type, default_kind AS default_type, default_expression, comment, " <>
           "compression_codec AS codec_expression, '' AS ttl_expression FROM system_columns " <>
           "WHERE database = #{Identifier.sql_string(db)} AND \"table\" = #{Identifier.sql_string(table)} " <>
           "ORDER BY position", {:unknown_table, db, table}}

      names = Regex.run(@exists, statement, capture: :all_but_first) ->
        {db, table} = qualified(names, database)

        {:catalog,
         "SELECT CAST(count(*) > 0 AS UTINYINT) AS result FROM system_tables " <>
           "WHERE database = #{Identifier.sql_string(db)} AND name = #{Identifier.sql_string(table)}",
         nil}

      names = Regex.run(@show_tables, statement, capture: :all_but_first) ->
        db = names |> List.first("") |> unquoted(database)

        {:catalog,
         "SELECT name FROM system_tables WHERE database = #{Identifier.sql_string(db)} ORDER BY name",
         nil}

      Regex.match?(@show_databases, statement) ->
        {:catalog, "SELECT name FROM system_databases ORDER BY name", nil}

      true ->
        {:select, renamed(statement)}
    end
  end

  defp qualified(["", table], database), do: {database, unquoted(table, table)}
  defp qualified([db, table], _database), do: {unquoted(db, db), unquoted(table, table)}

  defp unquoted("", default), do: default

  defp unquoted(<<?", _rest::binary>> = quoted, _default),
    do: quoted |> String.slice(1..-2//1) |> String.replace(~s(""), ~s("))

  defp unquoted(name, _default), do: name

  defp renamed(statement) do
    Sql.map_code(statement, fn code ->
      code
      |> String.replace(@system_table, "system_\\1")
      |> String.replace(@bare_table, ~s("table"))
    end)
  end

  defp classified(sql, state) do
    case CatalogEmulation.serialize(state.engine, sql) do
      {:ok, ast, _canonical} ->
        if system_only?(CatalogEmulation.base_tables(ast)) and not unbounded?(ast, sql),
          do: run(state, "SELECT * FROM (#{sql}) LIMIT #{@max_rows}"),
          else: {:reply, :pass, state}

      {:error, _unparseable} ->
        {:reply, :pass, state}
    end
  end

  defp unbounded?(ast, sql),
    do: table_function?(ast) or Regex.match?(~r/(?<![\w.])RECURSIVE(?![\w.])/i, sql)

  defp table_function?(%{"type" => "TABLE_FUNCTION"}), do: true

  defp table_function?(node) when is_map(node),
    do: Enum.any?(node, fn {_key, child} -> table_function?(child) end)

  defp table_function?(node) when is_list(node), do: Enum.any?(node, &table_function?/1)
  defp table_function?(_leaf), do: false

  defp system_only?([]), do: false

  defp system_only?(refs) do
    Enum.all?(refs, fn %{"schema_name" => schema, "table_name" => table} ->
      schema == "" and String.starts_with?(String.downcase(table), "system_")
    end)
  end

  defp run(state, sql) do
    with {:ok, state} <- ensure_fresh(state),
         {:ok, frame} <- frame(state.engine, sql) do
      {:reply, {:ok, frame}, state}
    else
      {:error, :statement_timeout} -> {:reply, {:error, unavailable()}, state}
      {:error, :catalog_unavailable} -> {:reply, {:error, unavailable()}, state}
      {:error, reason} -> {:reply, {:error, failure(reason)}, state}
    end
  end

  defp or_empty({:reply, {:ok, frame}, state} = reply, {:unknown_table, db, table}) do
    if DataFrame.n_rows(frame) == 0,
      do:
        {:reply, {:error, {404, 60, "UNKNOWN_TABLE", "Table #{db}.#{table} does not exist", nil}},
         state},
      else: reply
  end

  defp or_empty(reply, _empty), do: reply

  defp frame(engine, sql) do
    task = Task.async(fn -> Engine.frame(engine, sql) end)

    case Task.yield(task, @statement_timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      _timed_out_or_exited -> {:error, :statement_timeout}
    end
  end

  defp failure(reason) do
    message = reason |> Exception.message() |> String.replace("system_", "system.")

    case Regex.run(~r/Table with name system\.(\w+) does not exist/, message) do
      [_match, table] -> {404, 60, "UNKNOWN_TABLE", "Table system.#{table} does not exist", nil}
      nil -> Errors.engine_failure(message)
    end
  end

  defp unavailable,
    do: {503, 1002, "UNKNOWN_EXCEPTION", "the catalog could not be read; retry", 1}

  defp ensure_fresh(%{refreshed_at: refreshed_at} = state) do
    now = System.monotonic_time(:millisecond)

    if is_nil(refreshed_at) or now - refreshed_at > @refresh_ttl_ms do
      case refresh(state.engine, state.runtime.catalog) do
        :ok -> {:ok, %{state | refreshed_at: now}}
        {:error, _reason} -> {:error, :catalog_unavailable}
      end
    else
      {:ok, state}
    end
  end

  defp refresh(_engine, nil), do: :ok

  defp refresh(engine, catalog) do
    with {:ok, tables} <- CatalogEmulation.listed_tables(catalog),
         {:ok, datasets} <- Catalog.list_datasets(catalog) do
      listed = Enum.map(tables, fn {dataset, _table, _schema} -> dataset end)

      Engine.transaction(
        engine,
        [
          "DELETE FROM system_databases",
          "DELETE FROM system_tables",
          "DELETE FROM system_columns"
        ] ++
          insert("system_databases", database_rows(Enum.uniq(["system" | datasets] ++ listed))) ++
          insert("system_tables", Enum.map(tables, &table_row/1)) ++
          insert("system_columns", Enum.flat_map(tables, &column_rows/1))
      )
    end
  end

  defp insert(_table, []), do: []

  defp insert(table, rows) do
    [IO.iodata_to_binary(["INSERT INTO ", table, " VALUES ", Enum.intersperse(rows, ", ")])]
  end

  defp database_rows(names) do
    Enum.map(names, fn name ->
      engine = if name == "system", do: "Memory", else: "Atomic"

      values([name, engine, "", "", "00000000-0000-0000-0000-000000000000", engine, ""])
    end)
  end

  defp table_row({dataset, table, schema}) do
    key = Enum.join(schema.clustering, ", ")
    order = if key == "", do: "tuple()", else: "(#{key})"
    engine_full = "MergeTree ORDER BY #{order}"

    columns =
      Enum.map_join(schema.fields, ", ", fn field ->
        Identifier.quote_label(field.name) <> " " <> column_type(field)
      end)

    create =
      "CREATE TABLE #{dataset}.#{table} (#{columns}) ENGINE = #{engine_full}"

    values([
      dataset,
      table,
      "00000000-0000-0000-0000-000000000000",
      "MergeTree",
      0,
      create,
      engine_full,
      "",
      "",
      key,
      key,
      "",
      "default",
      nil,
      nil,
      ""
    ])
  end

  defp column_rows({dataset, table, schema}) do
    schema.fields
    |> Enum.with_index(1)
    |> Enum.map(fn {field, position} ->
      sorted = if field.name in schema.clustering, do: 1, else: 0
      {kind, expression} = default(field)

      values([
        dataset,
        table,
        field.name,
        column_type(field),
        position,
        kind,
        expression,
        "",
        0,
        sorted,
        sorted,
        0,
        ""
      ])
    end)
  end

  defp default(%Field{materialized: nil}), do: {"", ""}
  defp default(%Field{materialized: materialized}), do: {"MATERIALIZED", materialized.expression}

  defp values(row), do: "(" <> Enum.map_join(row, ", ", &value/1) <> ")"

  defp value(nil), do: "NULL"
  defp value(number) when is_integer(number), do: Integer.to_string(number)
  defp value(text) when is_binary(text), do: Identifier.sql_string(text)
end
