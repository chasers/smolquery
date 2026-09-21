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
  asked instead. `Pruner.unread_bounds/3` hands over the sides it could use
  and could not read; `bounds/3` keeps those that name no column, asks the
  engine for them in one `SELECT`, and answers their values keyed by
  `Smolquery.Engine.Ast.shape/1`. The pruner reads that map where its own
  rules find nothing.

  ## What is folded

  An expression built of constants, casts, operators and function calls
  only: no column, parameter, subquery, window, star or lambda anywhere in
  it, so it reads no table and no row. The shapes the pruner reads by itself
  never arrive, so a statement whose bounds are all of those — every search
  and chart HyperDX generates — costs no round trip here. At most
  `@max_expressions` are folded; a statement with more keeps its first.

  ## Each expression stands alone

  The rule for what the engine is trusted with is
  `Smolquery.QueryService.Stability`'s, the Top-N probe's too, and it is
  applied to each expression by the functions that expression names. A
  sampling filter beside a window (`AND rand() < 0.1`) loses its own side
  and nothing else. They are evaluated together, and one by one if that
  fails, so a side that cannot be evaluated (`CAST('abc' AS INTEGER)`) is
  one side with no value.

  Under `lockdown` the fold runs with extension autoload and autoinstall
  off, as the Top-N probe does and for its reason: it runs before the runner
  locks the engine down.

  ## What a value is good for

  A value comes back tagged:

  - `:fixed` bounds either way.
  - `:clock` named something stable within a query only, the clock above
    all. `now()` is read here milliseconds before the statement reads it.
    Early is sound for a lower bound that rises with the clock, where it
    keeps a little more, and for nothing else: above, a file of rows stamped
    ahead of this node's clock can sit between the two readings; and
    `X - (now() - Y)` falls as the clock rises, so early is late. So such an
    expression is folded only in a shape that rises with the clock — the
    call itself, under casts, `+` or `-` a clock-free amount, `date_trunc`,
    `toDateTime`, `toDateTime64` — and the pruner takes it as a lower bound
    only.
  - `:floored` is a `TIMESTAMP_NS`, which arrives cut to the microsecond. A
    lower bound cut short keeps a little more. The pruner adds the
    microsecond back before it uses one as an upper bound.

  A `TIMESTAMP WITH TIME ZONE` is compared with a plain timestamp column in
  the engine's `TimeZone`, and arrives here as its UTC instant. The two are
  the same bound only when that zone is UTC, so in any other zone such a
  value is dropped.

  ## An expression that does not return

  The fold gives up after `@timeout_ms`, and the expression keeps running on
  the connection, as any abandoned call does. It is the statement's own
  expression, which the statement would have evaluated anyway: what comes
  next on the connection waits behind it, and the job's timeout ends both.

  Every miss is a value not folded, never an error.
  """

  require Logger

  alias Smolquery.Engine.Ast
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Stability

  @type folded :: %{term() => {:fixed | :clock | :floored, term()}}

  @max_expressions 16
  @timeout_ms 5_000
  @foldable_classes ~w(CONSTANT CAST OPERATOR FUNCTION)
  @clock_macros ~w(now64)
  @rising_over_first ~w(todatetime todatetime64)
  @utc ~w(UTC Etc/UTC)
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
  The folded value of each of `expressions` that names no column, keyed by
  its `Smolquery.Engine.Ast.shape/1`.
  """
  @spec bounds(GenServer.server(), [map()], boolean()) :: folded()
  def bounds(connection, expressions, lockdown) do
    foldable =
      expressions
      |> Enum.filter(&column_free?/1)
      |> Enum.uniq_by(&Ast.shape/1)
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
    with :ok <- restrain(connection, lockdown),
         {:ok, catalog} <- catalog(connection, expressions) do
      expressions
      |> Enum.zip(catalog.selects)
      |> Enum.flat_map(&trusted(&1, catalog))
      |> evaluated(connection)
      |> Enum.flat_map(&tagged(&1, catalog.zone))
      |> Map.new()
    else
      other -> unfolded(other)
    end
  catch
    :exit, reason -> unfolded(reason)
  end

  defp unfolded(reason) do
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

  defp catalog(connection, expressions) do
    names = expressions |> Stability.function_names() |> Stability.checked()
    rendered = Enum.map_join(expressions, ", ", &"json_deserialize_sql(#{json(&1)})")

    sql =
      "SELECT [#{rendered}], #{Stability.unstable_names_sql(names)}, " <>
        "#{Stability.within_query_names_sql(names)}, current_setting('TimeZone')"

    case Connection.query(connection, sql, [], @timeout_ms) do
      {:ok, %Result{rows: [[selects, unstable, within_query, zone]]}} when is_list(selects) ->
        {:ok,
         %{selects: selects, unstable: unstable, clock: within_query ++ @clock_macros, zone: zone}}

      {:ok, result} ->
        {:error, {:fold_not_rendered, result}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp json(expression) do
    %{
      "error" => false,
      "statements" => [
        %{
          "named_param_map" => [],
          "node" => %{
            "type" => "SELECT_NODE",
            "modifiers" => [],
            "cte_map" => %{"map" => []},
            "select_list" => [Map.put(expression, "alias", "v")],
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
    |> JSON.encode!()
    |> Identifier.sql_string()
  end

  defp trusted({expression, select}, catalog) do
    names = Stability.function_names(expression)

    cond do
      Enum.any?(names, &(&1 in catalog.unstable)) -> []
      not Enum.any?(names, &(&1 in catalog.clock)) -> [{expression, select, :fixed}]
      rising?(expression, catalog.clock) -> [{expression, select, :clock}]
      true -> []
    end
  end

  defp rising?(%{"class" => "CAST", "child" => child}, clock), do: rising?(child, clock)

  defp rising?(%{"class" => "FUNCTION", "function_name" => name, "children" => children}, clock) do
    case {String.downcase(name), children} do
      {"+", [left, right]} ->
        rises_over?(left, right, clock) or rises_over?(right, left, clock)

      {"-", [left, right]} ->
        rises_over?(left, right, clock)

      {"date_trunc", [unit, value]} ->
        rises_over?(value, unit, clock)

      {wrapper, [value | rest]} when wrapper in @rising_over_first ->
        rises_over?(value, rest, clock)

      {call, arguments} ->
        call in clock and clock_free?(arguments, clock)
    end
  end

  defp rising?(_another_shape, _clock), do: false

  defp rises_over?(rising, fixed, clock), do: rising?(rising, clock) and clock_free?(fixed, clock)

  defp clock_free?(tree, clock),
    do: not Enum.any?(Stability.function_names(tree), &(&1 in clock))

  defp evaluated([], _connection), do: []

  defp evaluated(trusted, connection) do
    case values(connection, trusted) do
      {:ok, values} -> values
      :error -> Enum.flat_map(trusted, &(connection |> values([&1]) |> elem_or([])))
    end
  end

  defp elem_or({:ok, values}, _default), do: values
  defp elem_or(:error, default), do: default

  defp values(connection, trusted) do
    from =
      trusted
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {{_expression, select, _kind}, index} ->
        "(SELECT v AS v#{index}, typeof(v) AS t#{index} FROM (#{select}))"
      end)

    case Connection.query(connection, "SELECT * FROM #{from}", [], @timeout_ms) do
      {:ok, %Result{rows: [row]}} ->
        {:ok,
         trusted
         |> Enum.zip(Enum.chunk_every(row, 2))
         |> Enum.map(fn {{expression, _select, kind}, [value, type]} ->
           {expression, kind, value, type}
         end)}

      _failed_or_another_shape ->
        :error
    end
  end

  defp tagged({_expression, _kind, _value, "TIMESTAMP WITH TIME ZONE"}, zone)
       when zone not in @utc,
       do: []

  defp tagged({expression, :fixed, value, "TIMESTAMP_NS"}, _zone),
    do: [{Ast.shape(expression), {:floored, value}}]

  defp tagged({expression, kind, value, _type}, _zone),
    do: [{Ast.shape(expression), {kind, value}}]
end
