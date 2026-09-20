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

  ## `total_rows`

  ClickHouse keeps a table's row count in `system.tables`, and HyperDX's
  onboarding checklist sums it to decide whether a source has any data. The
  catalog holds no such number: rows are in two tiers, and only the planner
  knows both. So a statement that reads `total_rows` has it filled in, for
  the rows it reads it from:

  - **Which tables.** The statement's own `WHERE` says. Its `SELECT` list is
    replaced by `database, name`, its grouping, ordering and limit dropped,
    and the engine runs what is left, so `name != 'x'`, `name LIKE 'otel%'`
    and `database = 'default'` each select what they say. That holds for one
    `SELECT` over `system.tables` alone; any other shape gets no counts. More
    than the runtime's `total_rows_max_tables` tables is a refusal that says so, never a
    partial sum.
  - **The count.** The query service plans `SELECT * FROM db.t` without
    running it (`explain: :plan`), and the plan's sizes give the rows in both
    tiers, hot included, with no scan. `@count_concurrency` run at a time, so
    the counts do not take every job slot, each under the request's own
    `max_execution_time`.
  - **A count that cannot be had** — a full node, a timeout, a table the
    query service cannot plan — refuses the statement, with `retry-after`.
    A `NULL` there would be read as "this source has no data".
  - **Whose counts.** The statement's alone. They are written into the
    emulation's table for the one call that runs the statement and cleared
    when it returns, so a `SELECT *` a moment later reads `NULL`, as the
    docs say, and not what another client asked for.

  The counts are fetched by the caller, between two calls to the server, so
  a slow count holds up its own request and not the catalog. A statement
  that does not read the column pays none of this. `total_bytes` is always
  `NULL`.

  ## A name that arrives quoted

  HyperDX names every table with `Identifier` parameters, the `system` ones
  too, so its statement reads `FROM "system"."tables"`. The quotes are taken
  off a `system` database and a plain table name after it before anything
  else is read, so the four spellings are one.

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
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Statistics
  alias Smolquery.Schema.Field
  alias Smolquery.Sql
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Runtime

  @refresh_ttl_ms 1_000
  @call_timeout_ms 30_000
  @statement_timeout_ms 10_000
  @max_rows 10_000
  @emulated_type "CASE data_type WHEN 'VARCHAR' THEN 'String' WHEN 'UTINYINT' THEN 'UInt8' " <>
                   "WHEN 'UBIGINT' THEN 'UInt64' WHEN 'BIGINT' THEN 'Int64' ELSE data_type END"
  @count_concurrency 2

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
    ~s|CREATE TABLE system_tables (database VARCHAR, name VARCHAR, "table" VARCHAR, uuid VARCHAR, engine VARCHAR, | <>
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
  def answer(name, statement, database, opts \\ []) do
    statement = statement |> Sql.tokens() |> unquote_names() |> Enum.map_join(&elem(&1, 1))
    tokens = Sql.tokens(statement)

    if mentions_catalog?(tokens) do
      server = Runtime.system_catalog(name)

      with {:ok, counts} <- row_counts(name, server, tokens, statement, opts) do
        GenServer.call(server, {:answer, statement, database, counts}, @call_timeout_ms)
      end
    else
      :pass
    end
  catch
    :exit, _reason -> {:error, unavailable()}
  end

  defp unquote_names([{:quoted, ~s("system")}, {:code, dot} | rest]) do
    if Regex.match?(~r/\A\s*\./, dot),
      do: unquote_names([{:code, "system" <> dot} | rest]),
      else: [{:quoted, ~s("system")} | unquote_names([{:code, dot} | rest])]
  end

  defp unquote_names([{:code, code} = database, {:quoted, quoted} = table | rest]) do
    with true <- Regex.match?(~r/(?<![\w."])system\s*\.\s*\z/i, code),
         [_all, bare] <- Regex.run(~r/\A"([A-Za-z_][A-Za-z0-9_]*)"\z/, quoted) do
      [database, {:code, bare} | unquote_names(rest)]
    else
      _not_a_system_table -> [database | unquote_names([table | rest])]
    end
  end

  defp unquote_names([token | rest]), do: [token | unquote_names(rest)]
  defp unquote_names([]), do: []

  defp row_counts(name, server, tokens, statement, opts) do
    with true <- asks_for_rows?(tokens),
         {:ok, %Runtime{query_name: query_name}} <- Runtime.fetch(name),
         {:ok, [_table | _more] = refs} <-
           GenServer.call(server, {:counted_tables, statement}, @call_timeout_ms) do
      counted(query_name, refs, Keyword.get(opts, :timeout_ms, @statement_timeout_ms))
    else
      {:error, exception} -> {:error, exception}
      _no_count_asked_for -> {:ok, %{}}
    end
  end

  defp asks_for_rows?(tokens) do
    Enum.any?(tokens, fn
      {:code, code} -> Regex.match?(~r/(?<!\w)total_rows(?!\w)/i, code)
      {:quoted, quoted} -> String.downcase(quoted) == ~s("total_rows")
      {_kind, _text} -> false
    end)
  end

  defp counted(query_name, refs, timeout_ms) do
    refs
    |> Task.async_stream(&{&1, count(query_name, &1, timeout_ms)},
      max_concurrency: @count_concurrency,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, %{}}, fn
      {:ok, {ref, {:ok, rows}}}, {:ok, counts} -> {:cont, {:ok, Map.put(counts, ref, rows)}}
      {:ok, {ref, {:error, reason}}}, _counts -> {:halt, {:error, uncounted(ref, reason)}}
    end)
  end

  defp count(query_name, {dataset, table}, timeout_ms) do
    sql = "SELECT * FROM #{Identifier.quote_label(dataset)}.#{Identifier.quote_label(table)}"

    case Client.query(query_name, sql, explain: :plan, timeout_ms: timeout_ms) do
      {:ok, %Job{state: :done, statistics: %Statistics{} = statistics}, _plan} ->
        {:ok, Statistics.rows_scanned(statistics)}

      {:ok, %Job{error: error}, _plan} ->
        {:error, error}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp uncounted({dataset, table}, :too_many_jobs),
    do:
      {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES",
       "too many queries in flight to count the rows of #{dataset}.#{table}; retry later", 1}

  defp uncounted({dataset, table}, _reason),
    do:
      {503, 1002, "UNKNOWN_EXCEPTION",
       "the rows of #{dataset}.#{table} could not be counted for total_rows; retry", 1}

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

  defp mentions_catalog?(tokens) do
    leading = tokens |> Enum.map_join(&elem(&1, 1)) |> Sql.leading_keyword()

    leading in ~w(describe desc show exists) or
      Enum.any?(tokens, fn
        {:code, code} -> Regex.match?(@system_table, code)
        {_kind, _text} -> false
      end)
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    engine = Runtime.catalog_engine(runtime.name)
    {:ok, _pid} = Engine.start_link(name: engine)

    Enum.each(@static ++ @lockdown, &Engine.query!(engine, &1))

    {:ok, _ast, _canonical} =
      serialized = CatalogEmulation.serialize(engine, "SELECT database, name FROM system_tables")

    {:ok, %{"statements" => [%{"node" => %{"select_list" => columns}}]}, _sql} = serialized

    {:ok, %{runtime: runtime, engine: engine, refreshed_at: nil, columns: columns}}
  end

  @impl GenServer
  def handle_call({:counted_tables, statement}, _from, state) do
    with {:ok, state} <- ensure_fresh(state),
         {:ok, ast, _canonical} <- CatalogEmulation.serialize(state.engine, renamed(statement)),
         limit = state.runtime.total_rows_max_tables,
         {:ok, probe} <- counted_probe(ast, state.columns, limit),
         {:ok, result} <- Engine.query(state.engine, probe) do
      {:reply, within_limit(result.rows, limit), state}
    else
      _not_countable -> {:reply, {:ok, []}, state}
    end
  end

  def handle_call({:answer, statement, database, counts}, _from, state) do
    case read(statement, database) do
      {:select, sql} -> classified(sql, state, counts)
      {:catalog, sql, empty} -> state |> run(sql, counts) |> or_empty(empty)
    end
  end

  defp counted_probe(%{"statements" => [%{"node" => node} = statement]} = ast, columns, limit) do
    with %{"type" => "SELECT_NODE", "from_table" => %{"type" => "BASE_TABLE"} = from} <- node,
         "system_tables" <- String.downcase(from["table_name"]),
         true <- node["cte_map"]["map"] in [nil, []] do
      probe =
        node
        |> Map.merge(%{
          "select_list" => columns,
          "group_expressions" => [],
          "group_sets" => [],
          "aggregate_handling" => "STANDARD_HANDLING",
          "having" => nil,
          "qualify" => nil,
          "modifiers" => []
        })

      json = JSON.encode!(%{ast | "statements" => [%{statement | "node" => probe}]})

      {:ok,
       "SELECT DISTINCT * FROM query(json_deserialize_sql(#{Identifier.sql_string(json)})) " <>
         "LIMIT #{limit + 1}"}
    else
      _another_shape -> :error
    end
  end

  defp counted_probe(_ast, _columns, _limit), do: :error

  defp within_limit(rows, limit) when length(rows) > limit,
    do:
      {:error,
       {400, 36, "BAD_ARGUMENTS",
        "total_rows is answered for at most #{limit} tables, and this statement reads it " <>
          "from more; name the tables it should be read from, or raise " <>
          "SMOLQUERY_CLICKHOUSE_TOTAL_ROWS_MAX_TABLES", nil}}

  defp within_limit(rows, _limit),
    do: {:ok, Enum.map(rows, fn [dataset, table] -> {dataset, table} end)}

  defp read(statement, database) do
    cond do
      names = Regex.run(@describe, statement, capture: :all_but_first) ->
        names |> qualified(database) |> described()

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

  defp described({"system", table}) do
    {:catalog,
     "SELECT column_name AS name, " <>
       "CASE WHEN column_name IN ('total_rows', 'total_bytes') " <>
       "THEN 'Nullable(' || #{@emulated_type} || ')' ELSE #{@emulated_type} END AS type, " <>
       "'' AS default_type, '' AS default_expression, '' AS comment, " <>
       "'' AS codec_expression, '' AS ttl_expression FROM duckdb_columns() " <>
       "WHERE table_name = #{Identifier.sql_string("system_" <> table)} ORDER BY column_index",
     {:unknown_table, "system", table}}
  end

  defp described({db, table}) do
    {:catalog,
     "SELECT name, type, default_kind AS default_type, default_expression, comment, " <>
       "compression_codec AS codec_expression, '' AS ttl_expression FROM system_columns " <>
       "WHERE database = #{Identifier.sql_string(db)} AND \"table\" = #{Identifier.sql_string(table)} " <>
       "ORDER BY position", {:unknown_table, db, table}}
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

  defp classified(sql, state, counts) do
    case CatalogEmulation.serialize(state.engine, sql) do
      {:ok, ast, _canonical} ->
        if system_only?(CatalogEmulation.base_tables(ast)) and not unbounded?(ast, sql),
          do: run(state, "SELECT * FROM (#{sql}) LIMIT #{@max_rows}", counts),
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

  defp run(state, sql, counts) do
    with {:ok, state} <- ensure_fresh(state),
         :ok <- put_counts(state.engine, counts),
         {:ok, frame} <- counted_frame(state.engine, sql, counts) do
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

  defp put_counts(engine, counts) do
    updates =
      for {{dataset, table}, rows} when is_integer(rows) <- counts do
        "UPDATE system_tables SET total_rows = #{rows} WHERE database = #{Identifier.sql_string(dataset)} " <>
          "AND name = #{Identifier.sql_string(table)}"
      end

    if updates == [], do: :ok, else: Engine.transaction(engine, updates)
  end

  defp counted_frame(engine, sql, counts) when map_size(counts) == 0, do: frame(engine, sql)

  defp counted_frame(engine, sql, _counts) do
    frame(engine, sql)
  after
    Engine.query(
      engine,
      "UPDATE system_tables SET total_rows = NULL WHERE total_rows IS NOT NULL"
    )
  end

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
