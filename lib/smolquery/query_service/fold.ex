defmodule Smolquery.QueryService.Fold do
  @moduledoc """
  The value of every bound a statement writes as an expression, asked of the
  engine once per plan (T-534).

  `Smolquery.QueryService.Pruner` prunes by `column <op> bound`, and reads a
  bound itself when it is a constant, a TIMESTAMP or DATE cast of one, or a
  ClickHouse epoch function over an integer. Every other way to write one
  pruned nothing, and the statement opened every hot micro-segment:

      Timestamp >= toDateTime64('2026-09-19 10:11:00', 3)
      Timestamp >= parseDateTime64BestEffort('2026-09-19T10:11:00Z', 9)
      Timestamp >= fromUnixTimestamp64Milli(1789812660000) - INTERVAL 1 HOUR
      Timestamp >= now() - INTERVAL 15 MINUTE

  A clause in Elixir for each shape is a second implementation of the engine
  that has to be held to the first by a test, shape by shape. The planner
  holds a connection with the job's macros already defined, so the engine is
  asked instead. `Pruner.unread_bounds/2` hands over the sides of every
  comparison and BETWEEN it could not read; `bounds/3` keeps those that name
  no column, renders them into one `SELECT`, and answers their values keyed
  by expression. The pruner reads that map where its own rules find nothing.

  ## What is folded

  An expression built of constants, casts, operators and function calls
  only: no column, parameter, subquery, window, star or lambda anywhere in
  it, so it reads no table and no row. The shapes the pruner reads by itself
  never arrive, so a statement whose bounds are all of those — every
  statement HyperDX generates — costs no round trip here. At most
  `@max_expressions` are folded; a statement with more keeps its first.

  ## What the engine is trusted with

  The rule is `Smolquery.QueryService.TopN`'s. Every function the
  expressions name must be `CONSISTENT` or `CONSISTENT_WITHIN_QUERY` by the
  engine's own catalog, or one of `ClickHouseFunctions`' macros, trusted by
  bare name; one that is not folds nothing at all. Under `lockdown` the fold
  runs with extension autoload and autoinstall off, as the Top-N probe does
  and for its reason: it runs before the runner locks the engine down.

  ## A bound that reads the clock

  `now()` here is read milliseconds before the statement reads it. For a
  lower bound that is sound: `ts >= now() - INTERVAL 15 MINUTE` folded early
  keeps a little more. For an upper bound it is not: a file of rows stamped
  ahead of this node's clock can sit just past the folded `now()` and inside
  the statement's. So a fold that names a clock function is marked
  `:clock`, and the pruner takes it as a lower bound only.

  Every miss is an empty map: nothing folded, never an error.
  """

  require Logger

  alias Smolquery.Engine.Ast
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions

  @typedoc "An expression with its position and alias taken off, as a map key."
  @type key :: map()
  @type folded :: %{key() => {:fixed | :clock, term()}}

  @max_expressions 16
  @timeout_ms 5_000
  @foldable_classes ~w(CONSTANT CAST OPERATOR FUNCTION)
  @stable ["CONSISTENT", "CONSISTENT_WITHIN_QUERY"]
  @clock_macros ~w(now64)
  @no_autoload [
    "SET autoinstall_known_extensions = false",
    "SET autoload_known_extensions = false"
  ]

  @doc """
  The ClickHouse macros that read the clock, which the engine's catalog
  cannot say of a macro.
  """
  @spec clock_macros() :: [String.t()]
  def clock_macros, do: @clock_macros

  @doc """
  An expression as `bounds/3` keys it.
  """
  @spec key(map()) :: key()
  def key(node), do: Ast.shape(node)

  @doc """
  The folded value of each of `expressions` that names no column.
  """
  @spec bounds(GenServer.server(), [map()], boolean()) :: folded()
  def bounds(connection, expressions, lockdown) do
    foldable =
      expressions
      |> Enum.filter(&column_free?/1)
      |> Enum.map(&Ast.shape/1)
      |> Enum.uniq()
      |> Enum.take(@max_expressions)

    if foldable == [], do: %{}, else: folded(connection, foldable, lockdown)
  end

  defp column_free?(node) do
    node
    |> Ast.collect(fn
      %{"class" => class} when class in @foldable_classes -> []
      %{"class" => class} -> [class]
      _not_an_expression -> []
    end)
    |> Enum.empty?()
  end

  defp folded(connection, expressions, lockdown) do
    names = function_names(expressions)

    with :ok <- restrain(connection, lockdown),
         {:ok, sql, clock} <- rendered(connection, expressions, names),
         {:ok, %Result{rows: [values]}} <- Connection.query(connection, sql, [], @timeout_ms) do
      kind = if clock > 0 or Enum.any?(names, &(&1 in @clock_macros)), do: :clock, else: :fixed

      expressions |> Enum.zip(Enum.map(values, &{kind, &1})) |> Map.new()
    else
      :unstable ->
        %{}

      other ->
        Logger.debug(fn -> "bounds not folded, pruning by literals only: #{inspect(other)}" end)

        %{}
    end
  catch
    :exit, reason ->
      Logger.debug(fn -> "bounds not folded, pruning by literals only: #{inspect(reason)}" end)

      %{}
  end

  defp restrain(_connection, false), do: :ok

  defp restrain(connection, true) do
    Enum.reduce_while(@no_autoload, :ok, fn statement, :ok ->
      case Connection.query(connection, statement, [], @timeout_ms) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp rendered(connection, expressions, names) do
    json = expressions |> select() |> JSON.encode!()
    checked = Enum.reject(names, &ClickHouseFunctions.stable?/1)

    sql =
      "SELECT json_deserialize_sql(#{Identifier.sql_string(json)}), " <>
        counted(checked, "coalesce(stability, '') NOT IN (#{list(@stable)})") <>
        ", " <> counted(checked, "stability = 'CONSISTENT_WITHIN_QUERY'")

    case Connection.query(connection, sql, [], @timeout_ms) do
      {:ok, %Result{rows: [[select, 0, clock]]}} when is_binary(select) -> {:ok, select, clock}
      {:ok, %Result{rows: [[_select, _unstable, _clock]]}} -> :unstable
      {:ok, result} -> {:error, {:fold_not_rendered, result}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp function_names(expressions) do
    expressions
    |> Ast.collect(fn
      %{"class" => "FUNCTION", "function_name" => name} when is_binary(name) -> [name]
      _another_node -> []
    end)
    |> Enum.map(&String.downcase/1)
    |> Enum.uniq()
  end

  defp counted([], _condition), do: "0"

  defp counted(names, condition) do
    "(SELECT count(*) FROM duckdb_functions() WHERE lower(function_name) IN (#{list(names)}) " <>
      "AND #{condition})"
  end

  defp list(values), do: Enum.map_join(values, ", ", &Identifier.sql_string/1)

  defp select(expressions) do
    items =
      Enum.with_index(expressions, fn expression, index ->
        Map.put(expression, "alias", "f#{index}")
      end)

    %{
      "error" => false,
      "statements" => [
        %{
          "named_param_map" => [],
          "node" => %{
            "type" => "SELECT_NODE",
            "modifiers" => [],
            "cte_map" => %{"map" => []},
            "select_list" => items,
            "from_table" => %{"type" => "EMPTY", "alias" => "", "sample" => nil},
            "where_clause" => nil,
            "group_expressions" => [],
            "group_sets" => [],
            "aggregate_handling" => "STANDARD_HANDLING",
            "having" => nil,
            "sample" => nil,
            "qualify" => nil
          }
        }
      ]
    }
  end
end
