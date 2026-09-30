defmodule Smolquery.Catalog.DuckLake.Reader do
  @moduledoc """
  Reads a Postgres-backed DuckLake's metadata from Elixir, over a Postgrex
  pool, instead of through DuckDB's `postgres` extension (T-608).

  The extension reads a Postgres table as parallel `ctid`-range binary
  `COPY`s and filters in DuckDB: it pushes down only the simple predicates it
  can translate, and DuckLake's metadata reads filter by `table_id`, join files
  to stats and take `ORDER BY ... LIMIT 1`, none of which reach Postgres. On
  the sandbox every query's `resolve` copied the whole lake's
  `ducklake_file_column_stats` (about 440,000 rows) and every snapshot read
  copied every snapshot: 0.3-0.4 s alone, 1.5-3.3 s at six at once, almost
  none of it Postgres. It also opened a `REPEATABLE READ` transaction per
  statement to rediscover the schema, and the query tier's catalog engine has
  one connection, so concurrent plans queued behind each other. The same read
  over Postgrex measured 0.32 ms against 11.6 ms through the extension
  (T-550).

  So when the metadata is Postgres, `Smolquery.Catalog.DuckLake` sends its
  reads here: the same SQL against DuckLake's own tables, which live in the
  metadata database's default schema, run by Postgres with its predicates. A
  read of more than one statement runs in one `REPEATABLE READ READ ONLY`
  transaction, so its parts see one state. Writes stay in DuckLake, which
  owns their commit and conflict rules. Nothing is cached: every read asks
  Postgres.

  The pool is started beside the lake's engine by
  `Smolquery.Catalog.DuckLake.children/2`, `pool_size` connections (4 by
  default, `SMOLQUERY_CATALOG_READER_POOL_SIZE`), so plans on one node read in
  parallel. `SMOLQUERY_CATALOG_READER=off` sends every read back through
  DuckDB. The connection comes from the `postgres:` metadata string itself;
  one that carries a key Postgrex has no equivalent for (`sslmode`, say) keeps
  the DuckDB path rather than connecting differently from DuckLake.

  ## Indexes

  DuckLake creates primary keys on Postgres and no other index, so the reads
  here scan `ducklake_data_file`, `ducklake_column` and
  `ducklake_file_column_stats` by `table_id`. The snapshot read uses
  `ducklake_snapshot`'s primary key. Nothing here creates an index: several
  nodes booting at once would race each other's `CREATE INDEX CONCURRENTLY`,
  and a failed concurrent build leaves an invalid index that `IF NOT EXISTS`
  then skips forever. An operator adds them once:

      CREATE INDEX CONCURRENTLY IF NOT EXISTS smolquery_data_file_table
        ON ducklake_data_file (table_id, begin_snapshot);
      CREATE INDEX CONCURRENTLY IF NOT EXISTS smolquery_file_column_stats_table
        ON ducklake_file_column_stats (table_id, data_file_id);
      CREATE INDEX CONCURRENTLY IF NOT EXISTS smolquery_column_table
        ON ducklake_column (table_id);

  They add to DuckLake's tables without changing its schema contract.
  """

  @default_pool_size 4

  @scalar_types %{
    "boolean" => "BOOLEAN",
    "int8" => "TINYINT",
    "int16" => "SMALLINT",
    "int32" => "INTEGER",
    "int64" => "BIGINT",
    "int128" => "HUGEINT",
    "uint8" => "UTINYINT",
    "uint16" => "USMALLINT",
    "uint32" => "UINTEGER",
    "uint64" => "UBIGINT",
    "uint128" => "UHUGEINT",
    "float32" => "FLOAT",
    "float64" => "DOUBLE",
    "varchar" => "VARCHAR",
    "blob" => "BLOB",
    "uuid" => "UUID",
    "json" => "JSON",
    "variant" => "VARIANT",
    "date" => "DATE",
    "time" => "TIME",
    "interval" => "INTERVAL",
    "timestamp" => "TIMESTAMP",
    "timestamptz" => "TIMESTAMP WITH TIME ZONE",
    "timestamp_s" => "TIMESTAMP_S",
    "timestamp_ms" => "TIMESTAMP_MS",
    "timestamp_ns" => "TIMESTAMP_NS"
  }

  @libpq_keys %{
    "dbname" => :database,
    "host" => :hostname,
    "port" => :port,
    "user" => :username,
    "password" => :password
  }

  @doc """
  The pool name for a lake whose engine is `engine`.
  """
  @spec pool(atom()) :: atom()
  def pool(engine), do: Module.concat(engine, "Reader")

  @doc """
  The Postgrex options a reader pool for `metadata` starts with, or
  `:none` when reads should stay in DuckDB: SQLite metadata, a reader
  switched off, or a `postgres:` string carrying a key Postgrex cannot
  honour.
  """
  @spec options(String.t() | nil, keyword()) :: {:ok, keyword()} | :none
  def options(metadata, config \\ Application.get_env(:smolquery, __MODULE__, []))

  def options("postgres:" <> libpq, config) do
    with true <- Keyword.get(config, :enabled, true),
         {:ok, connection} <- libpq_options(libpq) do
      {:ok,
       Keyword.put(connection, :pool_size, Keyword.get(config, :pool_size, @default_pool_size))}
    else
      _off_or_unsupported -> :none
    end
  end

  def options(_metadata, _config), do: :none

  @doc """
  The Postgrex options a libpq `key=value` string names, or `:error` when
  it carries a key without a Postgrex equivalent or does not parse. Values
  may be bare or single-quoted with `\\` escapes, the form
  `Smolquery.DatabaseUrl.libpq_metadata/1` writes.
  """
  @spec libpq_options(String.t()) :: {:ok, keyword()} | :error
  def libpq_options(libpq) do
    with {:ok, pairs} <- libpq_pairs(String.trim_leading(libpq), []) do
      Enum.reduce_while(pairs, {:ok, []}, &libpq_option/2)
    end
  end

  defp libpq_option({key, value}, {:ok, options}) do
    case Map.fetch(@libpq_keys, key) do
      {:ok, :port} -> port_option(value, options)
      {:ok, option} -> {:cont, {:ok, [{option, value} | options]}}
      :error -> {:halt, :error}
    end
  end

  defp port_option(value, options) do
    case Integer.parse(value) do
      {port, ""} -> {:cont, {:ok, [{:port, port} | options]}}
      _not_a_port -> {:halt, :error}
    end
  end

  defp libpq_pairs("", pairs), do: {:ok, Enum.reverse(pairs)}

  defp libpq_pairs(rest, pairs) do
    with [key, value_and_rest] <- String.split(rest, "=", parts: 2),
         {:ok, value, rest} <- libpq_value(value_and_rest) do
      libpq_pairs(String.trim_leading(rest), [{String.trim(key), value} | pairs])
    else
      _malformed -> :error
    end
  end

  defp libpq_value("'" <> quoted), do: quoted_value(quoted, [])

  defp libpq_value(bare) do
    case String.split(bare, ~r/\s/, parts: 2) do
      [value, rest] -> {:ok, value, rest}
      [value] -> {:ok, value, ""}
    end
  end

  defp quoted_value("\\" <> <<char::utf8, rest::binary>>, acc),
    do: quoted_value(rest, [char | acc])

  defp quoted_value("'" <> rest, acc), do: {:ok, acc |> Enum.reverse() |> List.to_string(), rest}
  defp quoted_value(<<char::utf8, rest::binary>>, acc), do: quoted_value(rest, [char | acc])
  defp quoted_value("", _acc), do: :error

  @doc """
  The child spec of the pool `pool/1` names.
  """
  @spec child_spec({atom(), keyword()}) :: Supervisor.child_spec()
  def child_spec({name, options}) do
    %{Postgrex.child_spec([name: name] ++ options) | id: name}
  end

  @doc """
  Runs one read statement on the pool, or on the connection of a
  `transaction/2` in progress.
  """
  @spec query(DBConnection.conn(), String.t(), [term()]) ::
          {:ok, %{rows: [list()]}} | {:error, Exception.t()}
  def query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, %{rows: rows || []}}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Runs `fun` on one connection inside a `REPEATABLE READ READ ONLY`
  transaction, so every statement it runs sees one state of the metadata.
  `fun` answers `{:ok, value}` or `{:error, reason}`; an error rolls back and
  is answered as it came.
  """
  @spec transaction(atom(), (DBConnection.conn() -> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def transaction(pool, fun) do
    Postgrex.transaction(pool, fn conn ->
      with {:ok, _result} <-
             query(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", []),
           {:ok, value} <- fun.(conn) do
        value
      else
        {:error, reason} -> Postgrex.rollback(conn, reason)
      end
    end)
  end

  @doc """
  The SQL listing a table's current columns from DuckLake's own tables, top
  level and nested, in column order; `column_rows/1` turns its rows into the
  shape `information_schema.columns` gives the DuckDB path.
  """
  @spec columns_sql() :: String.t()
  def columns_sql do
    "SELECT c.column_id, c.column_name, c.column_type, c.nulls_allowed, " <>
      "c.begin_snapshot, c.parent_column " <>
      "FROM ducklake_column c " <>
      "JOIN ducklake_table t ON t.table_id = c.table_id AND t.end_snapshot IS NULL " <>
      "JOIN ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL " <>
      "WHERE s.schema_name = $1 AND t.table_name = $2 AND c.end_snapshot IS NULL " <>
      "ORDER BY c.column_order"
  end

  @doc """
  `[name, duckdb_type, is_nullable, column_id, begin_snapshot]` for each
  top-level column of `columns_sql/0`'s rows, the type rebuilt from DuckLake's
  type names and child columns the way DuckDB reports it: `int64` as
  `BIGINT`, a `map` with `varchar` key and value as `MAP(VARCHAR, VARCHAR)`.
  """
  @spec column_rows([list()]) :: [list()]
  def column_rows(rows) do
    children = Enum.group_by(rows, fn [_id, _name, _type, _nulls, _since, parent] -> parent end)

    for [id, name, type, nulls_allowed, since, nil] <- rows do
      [name, duckdb_type(type, id, children), if(nulls_allowed, do: "YES", else: "NO"), id, since]
    end
  end

  defp duckdb_type("map", id, children) do
    case Map.get(children, id, []) do
      [[key_id, _name, key_type | _], [value_id, _value, value_type | _]] ->
        key = duckdb_type(key_type, key_id, children)
        value = duckdb_type(value_type, value_id, children)
        "MAP(#{key}, #{value})"

      _other ->
        "MAP"
    end
  end

  defp duckdb_type("list", id, children) do
    case Map.get(children, id, []) do
      [[child_id, _name, child_type | _]] -> "#{duckdb_type(child_type, child_id, children)}[]"
      _other -> "LIST"
    end
  end

  defp duckdb_type("struct", id, children) do
    fields =
      Enum.map_join(Map.get(children, id, []), ", ", fn [child_id, name, child_type | _] ->
        "#{name} #{duckdb_type(child_type, child_id, children)}"
      end)

    "STRUCT(#{fields})"
  end

  defp duckdb_type("decimal" <> precision, _id, _children), do: "DECIMAL" <> precision

  defp duckdb_type(type, _id, _children),
    do: Map.get_lazy(@scalar_types, type, fn -> String.upcase(type) end)
end
