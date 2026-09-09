defmodule Smolquery.Schema.Materialized do
  @moduledoc """
  A column computed from the row it belongs to — ClickHouse's `MATERIALIZED`
  (PL-61 L4).

  A materialized column is a normal nullable column in the catalog plus an
  expression over the table's other columns. The write path evaluates the
  expression as it writes each micro-segment, so the value is stored like any
  other column's and read like one: in `SELECT *`, in a `GET` of the table,
  in a pruning bound. The one client-visible difference is on insert: a row
  may not supply a value for it (`Smolquery.IngestService.Validator`), and a
  row whose expression fails on its values stores `NULL` — the writer wraps
  the expression in DuckDB's `TRY`, so one bad value never fails a batch.

  ## Why definition time is the security boundary

  The expression is user SQL that the buffer's write engine will evaluate —
  an engine with file access to its spool, not a locked-down job engine —
  and later the storage engine too (L5). Three gates run when the column is
  defined, in `validate/2`, and the expression is immutable after:

  1. DuckDB parses it (`json_serialize_sql`), and the AST is walked: a column
     reference must name a regular column of this table, and the node classes
     are an allowlist — functions, operators, casts, comparisons, `CASE`,
     `BETWEEN`, constants. A subquery, a window, a star, a parameter, a lambda
     are refused. What is stored for evaluation is the canonical text DuckDB
     hands back (`json_deserialize_sql`), never the raw bytes: a comment or a
     stray semicolon cannot survive the round trip.
  2. Every function is `CONSISTENT` in `duckdb_functions().stability`, every
     overload of it. That is what makes L5 sound — a rewrite recomputes the
     value, so it must be the same value — and what refuses `now()`
     (`CONSISTENT_WITHIN_QUERY`), `random()` and `gen_random_uuid()`
     (`VOLATILE`). `current_setting` is consistent and refused by name: it
     reads the engine's configuration.
  3. A type probe on a throwaway engine with `enable_external_access` off:
     `SELECT CAST(<expr> AS <type>) FROM (SELECT NULL::t1 AS c1, ...)`. An
     expression that does not bind there — an unknown function, a function
     that needs the file system, a cast the types refuse — is refused here.

  `sources` are the ids of the columns the expression reads, so a drop of one
  of them is refused while the column exists (`Smolquery.Schema.drop_field/2`)
  and, when rename arrives, the expression survives it.
  """

  alias Smolquery.Engine
  alias Smolquery.Engine.Ast
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @enforce_keys [:expression]
  defstruct [:expression, :canonical, sources: []]

  @typedoc """
  `expression` is the text as the client wrote it, echoed back on `GET`;
  `canonical` is what the write path evaluates, DuckDB's own rendering of
  the parsed expression; `sources` the ids of the columns it reads, ascending.
  The last two are `nil` and `[]` on a definition the catalog has not
  validated yet.
  """
  @type t :: %__MODULE__{
          expression: String.t(),
          canonical: String.t() | nil,
          sources: [pos_integer()]
        }

  @allowed_classes ~w(FUNCTION OPERATOR CAST COMPARISON CONJUNCTION CASE BETWEEN CONSTANT COLUMN_REF)
  @denied_functions ~w(current_setting)
  @probe_extensions [:json]

  @doc """
  Runs the three gates over `field`'s expression against `schema`, the table
  as it stands before the column is added.

  Returns the definition with `canonical` and `sources` filled. `sources`
  carry the ids the schema's fields have; a schema without ids (one a client
  is creating) yields the source *names* instead, for the caller to map once
  the catalog has assigned ids.
  """
  @spec validate(Schema.t(), Field.t()) ::
          {:ok, t()} | {:error, {:invalid_materialized, term()}}
  def validate(%Schema{} = schema, %Field{materialized: %__MODULE__{} = definition} = field) do
    name = :"materialized_probe_#{:erlang.unique_integer([:positive])}"

    case Engine.start_link(name: name, extensions: @probe_extensions) do
      {:ok, pid} ->
        try do
          gates(name, schema, field, definition)
        after
          Supervisor.stop(pid, :normal)
        end

      {:error, reason} ->
        {:error, {:invalid_materialized, {:engine_failed, inspect(reason)}}}
    end
  end

  defp gates(engine, schema, field, definition) do
    regular = Enum.reject(schema.fields, &(&1.materialized != nil))

    with {:ok, node, canonical} <- parsed(engine, definition.expression),
         {:ok, sources} <- walked(node, schema, regular),
         :ok <- consistent(engine, node),
         :ok <- bound(engine, canonical, field.type, regular) do
      {:ok, %{definition | canonical: canonical, sources: sources}}
    end
  end

  defp parsed(engine, expression) do
    sql = "SELECT " <> expression
    quoted = Identifier.sql_string(sql)

    with {:ok, %{rows: [[json, canonical]]}} <-
           Engine.query(
             engine,
             "SELECT json_serialize_sql(#{quoted}), " <>
               "CASE WHEN json_extract_string(json_serialize_sql(#{quoted}), '$.error') = 'false' " <>
               "THEN json_deserialize_sql(json_serialize_sql(#{quoted})) END"
           ),
         {:ok, ast} <- JSON.decode(json),
         {:ok, node} <- one_expression(ast) do
      {:ok, node, String.replace_prefix(canonical, "SELECT ", "")}
    else
      {:error, {:invalid_materialized, _detail} = refusal} -> {:error, refusal}
      {:error, error} -> refuse({:unparseable, Exception.message(error)})
      other -> refuse({:unparseable, inspect(other)})
    end
  end

  defp one_expression(%{"error" => true} = ast),
    do: refuse({:unparseable, Map.get(ast, "error_message", "syntax error")})

  defp one_expression(%{"statements" => [%{"node" => %{"select_list" => [node]}}]}),
    do: {:ok, node}

  defp one_expression(%{"statements" => [_one]}), do: refuse(:one_expression)
  defp one_expression(_ast), do: refuse(:one_expression)

  defp walked(node, schema, regular) do
    nodes = Ast.collect(node, &[&1])

    with :ok <- allowed(nodes),
         :ok <- named_functions(nodes) do
      sources(nodes, schema, regular)
    end
  end

  defp allowed(nodes) do
    nodes
    |> Enum.map(& &1["class"])
    |> Enum.reject(&is_nil/1)
    |> Enum.find(&(&1 not in @allowed_classes))
    |> case do
      nil -> :ok
      class -> refuse({:unsupported_expression, class})
    end
  end

  defp named_functions(nodes) do
    nodes
    |> function_names()
    |> Enum.find(&(&1 in @denied_functions))
    |> case do
      nil -> :ok
      function -> refuse({:unsupported_function, function})
    end
  end

  defp sources(nodes, schema, regular) do
    names = MapSet.new(regular, & &1.name)

    nodes
    |> Enum.filter(&(&1["class"] == "COLUMN_REF"))
    |> Enum.map(& &1["column_names"])
    |> Enum.reduce_while({:ok, []}, fn
      [name], {:ok, acc} ->
        cond do
          MapSet.member?(names, name) ->
            {:cont, {:ok, [name | acc]}}

          match?({:ok, _field}, Schema.field(schema, name)) ->
            {:halt, refuse({:materialized_column, name})}

          true ->
            {:halt, refuse({:unknown_column, name})}
        end

      qualified, _acc ->
        {:halt, refuse({:qualified_column, Enum.join(qualified, ".")})}
    end)
    |> case do
      {:ok, names} ->
        {:ok, names |> Enum.uniq() |> Enum.map(&source_id(regular, &1)) |> Enum.sort()}

      refusal ->
        refusal
    end
  end

  defp source_id(regular, name) do
    case Enum.find(regular, &(&1.name == name)) do
      %Field{id: id} when is_integer(id) -> id
      %Field{} -> name
    end
  end

  defp consistent(engine, node) do
    case node |> Ast.collect(&[&1]) |> function_names() do
      [] -> :ok
      functions -> consistent_functions(engine, functions)
    end
  end

  defp consistent_functions(engine, functions) do
    placeholders = Enum.map_join(1..length(functions), ", ", &"$#{&1}")

    sql =
      "SELECT function_name, list(DISTINCT coalesce(stability, 'UNKNOWN')) " <>
        "FROM duckdb_functions() WHERE function_name IN (#{placeholders}) GROUP BY ALL"

    with {:ok, %{rows: rows}} <- Engine.query(engine, sql, functions) do
      stabilities = Map.new(rows, fn [name, list] -> {name, list} end)

      Enum.reduce_while(functions, :ok, &stable(&1, Map.get(stabilities, &1), &2))
    end
  end

  defp stable(_function, ["CONSISTENT"], :ok), do: {:cont, :ok}
  defp stable(function, nil, :ok), do: {:halt, refuse({:unknown_function, function})}
  defp stable(function, _other, :ok), do: {:halt, refuse({:inconsistent_function, function})}

  defp bound(engine, canonical, type, regular) do
    {:ok, target} = Schema.duckdb_type(type)

    row =
      case regular do
        [] ->
          "SELECT 1"

        fields ->
          "SELECT " <>
            Enum.map_join(fields, ", ", fn %Field{} = field ->
              {:ok, duckdb} = Schema.duckdb_type(field.type)
              "NULL::#{duckdb} AS #{Identifier.quote_name!(field.name)}"
            end)
      end

    with {:ok, _locked} <- Engine.query(engine, "SET enable_external_access = false"),
         {:ok, _probe} <-
           Engine.query(engine, "SELECT CAST((#{canonical}) AS #{target}) FROM (#{row})") do
      :ok
    else
      {:error, error} -> refuse({:does_not_bind, Exception.message(error)})
    end
  end

  defp function_names(nodes) do
    nodes
    |> Enum.filter(&(&1["class"] == "FUNCTION"))
    |> Enum.map(& &1["function_name"])
    |> Enum.uniq()
  end

  defp refuse(detail), do: {:error, {:invalid_materialized, detail}}

  @doc """
  The sentence an edge shows for a `{:invalid_materialized, detail}`.
  """
  @spec message(term()) :: String.t()
  def message({:unparseable, detail}), do: "materialized expression does not parse: #{detail}"
  def message(:one_expression), do: "materialized takes exactly one expression"

  def message({:unsupported_expression, class}),
    do: "materialized expression may not contain a #{String.downcase(class)}"

  def message({:unsupported_function, function}),
    do: "materialized expression may not call #{function}()"

  def message({:unknown_function, function}),
    do: "materialized expression calls a function DuckDB does not have: #{function}()"

  def message({:inconsistent_function, function}),
    do:
      "materialized expression may not call #{function}(): it is not deterministic, and the " <>
        "value is recomputed at every rewrite"

  def message({:materialized_column, name}),
    do: "materialized expression may not read #{name}: it is materialized itself"

  def message({:unknown_column, name}),
    do: "materialized expression reads a column the table does not have: #{name}"

  def message({:qualified_column, name}),
    do: "materialized expression names #{name}; a column is named without a qualifier"

  def message({:does_not_bind, detail}), do: "materialized expression does not bind: #{detail}"

  def message({:engine_failed, detail}),
    do: "materialized expression could not be checked: #{detail}"

  def message(detail), do: "materialized expression refused: #{inspect(detail)}"
end
