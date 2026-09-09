defmodule Smolquery.Ddl do
  @moduledoc """
  The two `ALTER TABLE` statements smolquery accepts as SQL, and nothing else
  (PL-61 L3).

      ALTER TABLE dataset.table ADD [COLUMN] [IF NOT EXISTS] name TYPE [MATERIALIZED expr]
      ALTER TABLE dataset.table DROP [COLUMN] [IF EXISTS] name

  ## Why a parser of its own

  `Smolquery.QueryService.Planner` reads every query through DuckDB's
  `json_serialize_sql`, which serializes a `SELECT` and refuses everything
  else. That refusal is the read-only gate, and it is why no DDL ever reached
  a catalog before this. DuckDB will not hand back an AST for `ALTER TABLE`,
  so the statement is recognised here, before the planner, by a parser that
  knows exactly two shapes and treats every other statement as not its
  business: `parse/1` answers `:not_ddl` for anything that does not begin
  with `ALTER`, so what the query path pays is one keyword check.

  The edge modules carry a SQL lexer (`SmolqueryPg.Sql`), but the query
  service may not depend on an edge (`.reach.exs`), so this module scans for
  itself: words, quoted identifiers, integers, and the four punctuation marks
  the grammar needs. The accepted grammar has no comments and no string
  literals, so there is nothing else to scan — except a `MATERIALIZED`
  expression, which is arbitrary SQL and is therefore kept as the raw text
  after the keyword for `Smolquery.Catalog` to validate through DuckDB.

  An unquoted identifier keeps the case it was typed in, which is what the
  API does with a JSON field name; a quoted one is taken verbatim. Both must
  be a `Smolquery.Identifier`, so `"My Column"` is refused the same way
  `{"name": "My Column"}` is.

  ## Types

  A column type is accepted in the API's vocabulary (`INT64`, `STRING`,
  `NUMERIC(38,2)`, ...) and in DuckDB's (`BIGINT`, `VARCHAR`,
  `DECIMAL(38,2)`, ...), and always becomes a `Smolquery.Schema` logical
  type — the vocabulary `POST .../columns` speaks, so a type that works in one
  place works in the other.

  `NOT NULL`, `DEFAULT`, and anything else after the type are refused with a
  reason rather than ignored: an added column is nullable and has no default,
  because that is the only claim true of the rows that already exist
  (`Smolquery.Catalog.alter_table/3`).

  ## What executing one does

  `execute/2` is `Smolquery.Catalog.alter_table/3` plus the `IF [NOT] EXISTS`
  semantics: a duplicate add or a missing drop under the guard succeeds and
  reports `performed: false`, the way Postgres notices and moves on. A name
  the table once had is not a special case: the column that takes it is a
  new column, told apart from the old one by id in every file (PL-62).

  ## Where it runs

  `Smolquery.QueryService.Runner` consults `parse/1` before the planner, so
  an `ALTER TABLE` is a query job like any other — same submit, same await,
  same history row — that finishes with its `outcome` on
  `Smolquery.QueryService.Job.ddl` and no result frame. It needs no engine:
  the catalog call is the whole job. `explain:` and `describe:` are refused
  (`:ddl_not_explainable`), as are bind parameters (`:ddl_takes_no_params`):
  the statement has nothing to plan and nothing to bind.
  """

  alias Smolquery.Catalog
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  defmodule AlterTable do
    @moduledoc """
    A parsed `ALTER TABLE`: the table, the one change to its columns, and the
    `IF [NOT] EXISTS` guard.
    """

    @enforce_keys [:table, :change]
    defstruct [:table, :change, :materialized, if_exists: false]

    @type t :: %__MODULE__{
            table: Smolquery.Catalog.table_ref(),
            change: Smolquery.Catalog.column_change(),
            materialized: String.t() | nil,
            if_exists: boolean()
          }
  end

  @typedoc """
  What `execute/2` reports: the change, the table, the column, and whether
  anything happened — `false` only under an `IF [NOT] EXISTS` guard.
  """
  @type outcome :: %{
          operation: :add_column | :drop_column,
          table: Catalog.table_ref(),
          column: String.t(),
          performed: boolean()
        }

  @leading ~r/\A\s*alter\b/i

  @types %{
    "bigint" => "INT64",
    "int64" => "INT64",
    "integer" => "INT64",
    "int" => "INT64",
    "double" => "FLOAT64",
    "float64" => "FLOAT64",
    "float" => "FLOAT64",
    "real" => "FLOAT64",
    "varchar" => "STRING",
    "string" => "STRING",
    "text" => "STRING",
    "boolean" => "BOOL",
    "bool" => "BOOL",
    "timestamp" => "TIMESTAMP",
    "date" => "DATE",
    "variant" => "VARIANT",
    "json" => "VARIANT"
  }

  @doc """
  Parses `sql` as one of the accepted `ALTER TABLE` forms.

  `:not_ddl` when the statement does not begin with `ALTER` — the planner's
  business, not this module's. Leading whitespace and comments are skipped
  first, as the wire's own lexer skips them, so the two agree on what is
  DDL; a statement that is not valid UTF-8 is refused rather than scanned. `{:error, reason}` when it does and is not one
  of the two shapes; the reasons are the ones `error?/1` recognises.
  """
  @spec parse(String.t()) :: {:ok, AlterTable.t()} | :not_ddl | {:error, term()}
  def parse(sql) when is_binary(sql) do
    cond do
      not String.valid?(sql) ->
        {:error, {:invalid_ddl, "the statement is not valid UTF-8"}}

      not Regex.match?(@leading, trivia_stripped(sql)) ->
        :not_ddl

      true ->
        with({:ok, tokens, remainder} <- scan(trivia_stripped(sql)), do: alter(tokens, remainder))
    end
  end

  defp trivia_stripped(sql) do
    case Regex.replace(~r/\A(?:\s+|--[^\n]*(?:\n|\z)|\/\*.*?\*\/)+/s, sql, "", global: false) do
      ^sql -> sql
      stripped -> trivia_stripped(stripped)
    end
  end

  @doc """
  Runs a parsed statement against the catalog.
  """
  @spec execute(Catalog.t(), AlterTable.t()) :: {:ok, outcome()} | {:error, term()}
  def execute(_catalog, %AlterTable{materialized: expression}) when is_binary(expression),
    do: {:error, :materialized_unsupported}

  def execute(%Catalog{} = catalog, %AlterTable{table: table, change: change} = ddl) do
    case {change, Catalog.alter_table(catalog, table, change)} do
      {_change, :ok} -> {:ok, outcome(ddl, true)}
      {{:add_column, _}, {:error, {:duplicate_columns, _}}} when ddl.if_exists -> skipped(ddl)
      {{:drop_column, _}, {:error, {:unknown_column, _}}} when ddl.if_exists -> skipped(ddl)
      {_change, {:error, reason}} -> {:error, reason}
    end
  end

  @doc """
  Whether `reason` is one a DDL job fails with — the parser's refusals, the
  executor's, and `Smolquery.Catalog.alter_table/3`'s — so an edge can map it
  to a status instead of reporting a query error.
  """
  @spec error?(term()) :: boolean()
  def error?({tag, _detail})
      when tag in [
             :invalid_ddl,
             :unsupported_ddl,
             :unqualified_table,
             :unsupported_type,
             :invalid_identifier,
             :duplicate_columns,
             :unknown_column,
             :column_must_be_nullable,
             :clustering_column,
             :retention_column,
             :partition_ref,
             :unknown_table
           ],
      do: true

  def error?(reason)
      when reason in [
             :last_column,
             :multiple_statements,
             :alter_table_unsupported,
             :materialized_unsupported,
             :ddl_not_explainable,
             :ddl_takes_no_params
           ],
      do: true

  def error?(_reason), do: false

  @doc """
  The sentence an edge shows for a reason `error?/1` recognises, so the two
  edges cannot drift in what they tell a caller about the same refusal.
  """
  @spec message(term()) :: String.t()
  def message({:invalid_ddl, detail}), do: detail
  def message({:unsupported_ddl, clause}), do: "#{clause} is not supported in ALTER TABLE"
  def message({:unqualified_table, name}), do: "#{name}: a table is named dataset.table"
  def message({:unsupported_type, type}) when is_binary(type), do: "unsupported type: #{type}"
  def message({:unsupported_type, type}), do: "unsupported type: #{inspect(type)}"
  def message({:invalid_identifier, name}), do: "invalid identifier: #{inspect(name)}"
  def message({:duplicate_columns, [name | _rest]}), do: "column #{name} already exists"
  def message({:unknown_column, name}), do: "column #{name} does not exist"

  def message({:column_must_be_nullable, name}),
    do:
      "column #{name} must be nullable: an added column has no value for the rows that " <>
        "already exist"

  def message({:clustering_column, name}),
    do: "column #{name} is in the clustering key; clear the key before dropping it"

  def message({:retention_column, name}),
    do: "column #{name} is the retention column; clear the policy before dropping it"

  def message({:partition_ref, {dataset, table}}),
    do: "#{dataset}.#{table} is a partition, not a table"

  def message({:unknown_table, {dataset, table}}), do: "table #{dataset}.#{table} does not exist"
  def message(:last_column), do: "a table needs at least one column"
  def message(:multiple_statements), do: "one statement at a time"

  def message(:alter_table_unsupported),
    do: "this catalog does not support changing a table's columns"

  def message(:materialized_unsupported), do: "MATERIALIZED columns are not supported yet"
  def message(:ddl_not_explainable), do: "ALTER TABLE cannot be explained or described"
  def message(:ddl_takes_no_params), do: "ALTER TABLE takes no bind parameters"
  def message(reason), do: inspect(reason)

  defp outcome(%AlterTable{table: table, change: change}, performed) do
    {operation, column} =
      case change do
        {:add_column, %Field{name: name}} -> {:add_column, name}
        {:drop_column, name} -> {:drop_column, name}
      end

    %{operation: operation, table: table, column: column, performed: performed}
  end

  defp skipped(ddl), do: {:ok, outcome(ddl, false)}

  defp alter([{:word, "alter", _, _}, {:word, "table", _, _} | rest], remainder) do
    with {:ok, table, rest} <- table_ref(rest) do
      action(table, rest, remainder)
    end
  end

  defp alter(_tokens, _remainder),
    do: {:error, {:invalid_ddl, "only ALTER TABLE is supported"}}

  defp table_ref([first, {:punct, ".", _}, second, {:punct, ".", _} | _rest]) do
    with {:ok, dataset} <- name(first), {:ok, table} <- name(second) do
      {:error,
       {:invalid_ddl,
        "a table is named dataset.table; #{dataset}.#{table}.... has one part too many"}}
    end
  end

  defp table_ref([first, {:punct, ".", _}, second | rest]) do
    with {:ok, dataset} <- name(first),
         {:ok, table} <- name(second),
         {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table) do
      {:ok, {dataset, table}, rest}
    end
  end

  defp table_ref([first | _rest]) do
    with {:ok, bare} <- name(first), do: {:error, {:unqualified_table, bare}}
  end

  defp table_ref([]), do: {:error, {:invalid_ddl, "ALTER TABLE needs a dataset.table"}}

  defp action(table, [{:word, "add", _, _} | rest], remainder) do
    rest = skip(rest, ["column"])
    {if_not_exists, rest} = guard(rest, ["if", "not", "exists"])

    with {:ok, column, rest} <- column(rest),
         {:ok, type, rest} <- type(rest),
         {:ok, field} <- Field.new(column, type),
         {:ok, materialized} <- tail(rest, remainder) do
      {:ok,
       %AlterTable{
         table: table,
         change: {:add_column, field},
         materialized: materialized,
         if_exists: if_not_exists
       }}
    end
  end

  defp action(table, [{:word, "drop", _, _} | rest], _remainder) do
    rest = skip(rest, ["column"])
    {if_exists, rest} = guard(rest, ["if", "exists"])

    with {:ok, column, rest} <- column(rest),
         {:ok, column} <- Identifier.validate(column),
         :ok <- nothing_after(rest) do
      {:ok, %AlterTable{table: table, change: {:drop_column, column}, if_exists: if_exists}}
    end
  end

  defp action(_table, [other | _rest], _remainder),
    do: {:error, {:unsupported_ddl, describe(other)}}

  defp action(_table, [], _remainder),
    do: {:error, {:invalid_ddl, "expected ADD COLUMN or DROP COLUMN"}}

  defp column([token | rest]) do
    with {:ok, name} <- name(token), do: {:ok, name, rest}
  end

  defp column([]), do: {:error, {:invalid_ddl, "expected a column name"}}

  defp type([
         {:word, word, _, _},
         {:punct, "(", _},
         {:int, precision, _},
         {:punct, ",", _},
         {:int, scale, _},
         {:punct, ")", _} | rest
       ])
       when word in ["decimal", "numeric"] do
    with {:ok, type} <- Schema.type_from_api("NUMERIC(#{precision},#{scale})") do
      {:ok, type, rest}
    end
  end

  defp type([
         {:word, "map", _, _},
         {:punct, "(", _},
         {:word, key, _, _},
         {:punct, ",", _},
         {:word, value, _, _},
         {:punct, ")", _} | rest
       ])
       when key in ["string", "varchar"] and value in ["string", "varchar"] do
    with {:ok, type} <- Schema.type_from_api("MAP(STRING, STRING)"), do: {:ok, type, rest}
  end

  defp type([{:word, word, original, _} | rest]) do
    case Map.fetch(@types, word) do
      {:ok, api} ->
        with {:ok, type} <- Schema.type_from_api(api), do: {:ok, type, rest}

      :error ->
        {:error, {:unsupported_type, original}}
    end
  end

  defp type(_tokens), do: {:error, {:invalid_ddl, "expected a column type"}}

  defp tail([], _remainder), do: {:ok, nil}

  defp tail([{:word, "materialized", _, _}], remainder) do
    case String.trim(remainder) do
      "" -> {:error, {:invalid_ddl, "MATERIALIZED needs an expression"}}
      expression -> {:ok, expression}
    end
  end

  defp tail([{:word, "not", _, _}, {:word, "null", _, _} | _rest], _remainder),
    do: {:error, {:unsupported_ddl, "NOT NULL"}}

  defp tail([other | _rest], _remainder), do: {:error, {:unsupported_ddl, describe(other)}}

  defp nothing_after([]), do: :ok
  defp nothing_after([other | _rest]), do: {:error, {:unsupported_ddl, describe(other)}}

  defp name({:word, _down, original, _at}), do: {:ok, original}
  defp name({:quoted, quoted, _at}), do: {:ok, quoted}
  defp name(other), do: {:error, {:invalid_ddl, "expected an identifier, got #{describe(other)}"}}

  defp skip([{:word, word, _, _} | rest], [word]), do: rest
  defp skip(tokens, _words), do: tokens

  defp guard(tokens, words) do
    case Enum.split(tokens, length(words)) do
      {leading, rest} when length(leading) == length(words) ->
        if Enum.map(leading, &keyword/1) == words, do: {true, rest}, else: {false, tokens}

      _short ->
        {false, tokens}
    end
  end

  defp keyword({:word, word, _, _}), do: word
  defp keyword(_other), do: nil

  defp describe({:word, _down, original, _at}), do: original
  defp describe({:quoted, quoted, _at}), do: ~s|"#{quoted}"|
  defp describe({:punct, punct, _at}), do: punct
  defp describe({:int, int, _at}), do: Integer.to_string(int)

  defp scan(sql), do: scan(sql, 0, [])

  defp scan(<<>>, _at, acc), do: {:ok, Enum.reverse(acc), ""}

  defp scan(<<char, rest::binary>>, at, acc) when char in [?\s, ?\t, ?\n, ?\r],
    do: scan(rest, at + 1, acc)

  defp scan(<<?;, rest::binary>>, _at, acc) do
    if String.trim(rest) == "",
      do: {:ok, Enum.reverse(acc), ""},
      else: {:error, :multiple_statements}
  end

  defp scan(<<?", rest::binary>>, at, acc) do
    case quoted(rest, []) do
      {:ok, name, next} ->
        scan(next, at + byte_size(name) + 2, [{:quoted, name, at} | acc])

      :error ->
        {:error, {:invalid_ddl, "unterminated quoted identifier"}}
    end
  end

  defp scan(<<char, _::binary>> = sql, at, acc) when char in ?0..?9 do
    {digits, rest} = take(sql, &(&1 in ?0..?9))

    scan(rest, at + byte_size(digits), [{:int, String.to_integer(digits), at} | acc])
  end

  defp scan(<<?_, _::binary>> = sql, at, acc), do: word(sql, at, acc)

  defp scan(<<char, _::binary>> = sql, at, acc) when char in ?a..?z or char in ?A..?Z,
    do: word(sql, at, acc)

  defp scan(<<char, rest::binary>>, at, acc) when char in [?(, ?), ?,, ?.],
    do: scan(rest, at + 1, [{:punct, <<char>>, at} | acc])

  defp scan(<<char::utf8, _::binary>>, _at, _acc),
    do: {:error, {:invalid_ddl, "unexpected #{inspect(<<char::utf8>>)}"}}

  defp word(sql, at, acc) do
    {word, rest} = take(sql, &word_char?/1)
    token = {:word, String.downcase(word), word, at}

    if String.downcase(word) == "materialized",
      do: {:ok, Enum.reverse([token | acc]), String.trim_trailing(rest, ";")},
      else: scan(rest, at + byte_size(word), [token | acc])
  end

  defp quoted(<<>>, _acc), do: :error
  defp quoted(<<?", ?", rest::binary>>, acc), do: quoted(rest, [?" | acc])
  defp quoted(<<?", rest::binary>>, acc), do: {:ok, acc |> Enum.reverse() |> to_string(), rest}
  defp quoted(<<char, rest::binary>>, acc), do: quoted(rest, [char | acc])

  defp word_char?(char),
    do: char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_

  defp take(binary, fun), do: take(binary, fun, [])

  defp take(<<char, rest::binary>>, fun, acc) do
    if fun.(char),
      do: take(rest, fun, [char | acc]),
      else: finish_take(acc, <<char, rest::binary>>)
  end

  defp take(<<>>, _fun, acc), do: finish_take(acc, <<>>)

  defp finish_take(acc, rest), do: {acc |> Enum.reverse() |> to_string(), rest}
end
