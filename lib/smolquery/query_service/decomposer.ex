defmodule Smolquery.QueryService.Decomposer do
  @moduledoc """
  Splits one aggregate SELECT into a per-shard *partial* query and a
  coordinator *final* query, or refuses (PL-49).

  `Smolquery.QueryService.Scatter` runs the partial on K DuckDB instances,
  each over a shard of the plan's file list, and the final over the union
  of their results. The split must be exact: PL-48 settled the arithmetic
  for count / sum / min / max / avg (`count` merges as `sum`; `avg` splits
  into a partial `sum` and `count` whose ratio the final takes), for
  GROUP BY (group per shard, regroup over the union), and for top-k
  (a full partial group-by, ordered and limited only at the final step).

  ## DuckDB's parser is the only parser, here too

  Like `Smolquery.QueryService.Planner`, this never reads SQL text. The
  statement arrives as `json_serialize_sql`'s AST; the partial statement is
  built by surgery on a copy of it — the select list becomes the resolved
  group expressions aliased `__pq_g<i>` plus the partial aggregates aliased
  `__pq_a<j>`, the modifiers are stripped, FROM and WHERE stay — and
  `json_deserialize_sql` renders it back to text. The partial still
  references the planned view name (`dataset.table`); each worker defines
  that view over its own shard. The final query is generated text over the
  partial aliases only, so it needs no parsing at all.

  ## The gate is conservative

  Refusal is the common case and costs nothing: the caller falls back to
  the single-engine path. Refused outright: multiple tables or any FROM
  that is not one base table, CTEs, `SELECT *`, DISTINCT, HAVING, QUALIFY,
  SAMPLE, grouping sets, window functions, subqueries anywhere, `count`
  variants with DISTINCT or FILTER, aggregates outside the five above,
  OFFSET, and ORDER BY on anything but an output column's name. A select
  item must be a supported aggregate or match a group expression. Table
  columns prefixed `__pq_` would collide with the generated aliases, so
  they refuse too. Volatile functions — `now()`, `random()`, and their
  kin — refuse as well: each worker would bind them on its own clock or
  seed, and the shards would disagree with the single-engine answer.

  ## `GROUP BY ALL`

  DuckDB serializes `GROUP BY ALL` as `aggregate_handling: FORCE_AGGREGATES`
  with no group expressions — the keys are resolved later, in binding. So
  the keys are resolved here the same way DuckDB does: every select item
  that contains no aggregate is a group key. The partial then names those
  keys explicitly under standard handling. `select count(key), key ... group
  by all` was the first production query to hit the distributed path, and
  it silently fell back until this case existed (T-356).

  ## Integer exactness

  DuckDB types `sum(BIGINT)` as HUGEINT, and parquet has no int128 — a
  HUGEINT column COPYed to parquet degrades to DOUBLE, which would make a
  distributed integer sum silently wrong past 2^53. So the partial is
  `DESCRIBE`d and any HUGEINT aggregate column is cast to `DECIMAL(38,0)`,
  which parquet stores exactly; the final's cast restores the original
  type. A HUGEINT *group key* cannot take that cast without changing the
  result schema, so it refuses instead.

  ## Output names and types come from `DESCRIBE`

  The final query must reproduce the original's result schema exactly:
  DuckDB names an unaliased aggregate after its expression text, and the
  merge arithmetic widens types (`sum` over BIGINT is HUGEINT). The caller
  passes the `DESCRIBE` of the original statement, run on the engine whose
  views the plan created; every final select item is aliased to the name
  and cast to the type reported there, positionally.
  """

  alias Smolquery.Engine.Connection

  @mergeable ~w(count_star count sum min max)
  @aggregates ["avg" | @mergeable]
  @prefix "__pq_"
  @refused_classes ~w(SUBQUERY WINDOW STAR)
  @volatile ~w(now get_current_timestamp current_date current_localtime
               current_localtimestamp today random uuid uuidv4 uuidv7
               gen_random_uuid setseed nextval currval)

  @enforce_keys [:partial_sql, :final_select, :final_group, :final_tail]
  defstruct [:partial_sql, :final_select, :final_group, :final_tail]

  @type t :: %__MODULE__{
          partial_sql: String.t(),
          final_select: String.t(),
          final_group: String.t(),
          final_tail: String.t()
        }

  @type output :: {String.t(), String.t()}

  @doc """
  Decomposes `sql` for a table whose columns are `table_columns`, given the
  `DESCRIBE` outputs of the original statement.

  `connection` is used to serialize, to deserialize, and to `DESCRIBE` the
  partial for the integer-exactness cast — three round trips, no data
  touched. The partial references the planned view name, so the connection
  must already hold the plan's views.
  """
  @spec decompose(GenServer.server(), String.t(), [output()], [String.t()]) ::
          {:ok, t()} | {:error, term()}
  def decompose(connection, sql, outputs, table_columns) do
    with :ok <- gate_columns(table_columns),
         {:ok, node} <- select_node(connection, sql),
         :ok <- gate_shape(node),
         :ok <- gate_classes(node),
         :ok <- gate_volatile(node),
         {:ok, keys} <- group_keys(node, table_columns),
         {:ok, items} <- classified_items(node, keys),
         :ok <- gate_outputs(items, outputs),
         {:ok, tail} <- tail(node, outputs),
         {:ok, partial_sql} <- partial(connection, node, keys, items) do
      {:ok,
       %__MODULE__{
         partial_sql: partial_sql,
         final_select: final_select(items, outputs),
         final_group: final_group(keys),
         final_tail: tail
       }}
    end
  end

  @doc """
  The final query over `from` — a `read_parquet` across the partial files.
  """
  @spec final_sql(t(), String.t()) :: String.t()
  def final_sql(%__MODULE__{} = decomposition, from) do
    ["SELECT #{decomposition.final_select} FROM #{from}"]
    |> append(decomposition.final_group)
    |> append(decomposition.final_tail)
    |> Enum.join(" ")
  end

  defp append(parts, ""), do: parts
  defp append(parts, clause), do: parts ++ [clause]

  defp gate_columns(table_columns) do
    if Enum.any?(table_columns, &String.starts_with?(&1, @prefix)) do
      {:error, :reserved_column_prefix}
    else
      :ok
    end
  end

  defp select_node(connection, sql) do
    quoted = Smolquery.Identifier.sql_string(sql)

    with {:ok, result} <-
           Connection.query(connection, "SELECT json_serialize_sql(#{quoted})", [], :infinity),
         [[json]] <- result.rows,
         {:ok, ast} <- JSON.decode(json) do
      case ast do
        %{"error" => false, "statements" => [%{"node" => %{"type" => "SELECT_NODE"} = node}]} ->
          {:ok, node}

        _refused ->
          {:error, :not_a_single_select}
      end
    else
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:error, :not_a_single_select}
    end
  end

  defp gate_shape(node) do
    with :ok <- gate_structure(node),
         :ok <- gate_grouping(node) do
      gate_modifiers(node["modifiers"])
    end
  end

  defp gate_structure(node) do
    cond do
      node["cte_map"]["map"] != [] -> {:error, :cte}
      node["from_table"]["type"] != "BASE_TABLE" -> {:error, :from_not_a_base_table}
      node["from_table"]["sample"] != nil -> {:error, :sample}
      node["sample"] != nil -> {:error, :sample}
      true -> :ok
    end
  end

  defp gate_grouping(node) do
    cond do
      node["qualify"] != nil ->
        {:error, :qualify}

      node["having"] != nil ->
        {:error, :having}

      node["aggregate_handling"] not in ["STANDARD_HANDLING", "FORCE_AGGREGATES"] ->
        {:error, {:aggregate_handling, node["aggregate_handling"]}}

      match?([_first, _second | _rest], node["group_sets"]) ->
        {:error, :grouping_sets}

      true ->
        :ok
    end
  end

  defp gate_modifiers(modifiers) do
    Enum.reduce_while(modifiers, :ok, fn modifier, :ok ->
      case modifier do
        %{"type" => "ORDER_MODIFIER"} -> {:cont, :ok}
        %{"type" => "LIMIT_MODIFIER", "offset" => nil} -> {:cont, :ok}
        %{"type" => "LIMIT_MODIFIER"} -> {:halt, {:error, :offset}}
        %{"type" => other} -> {:halt, {:error, {:unsupported_modifier, other}}}
      end
    end)
  end

  defp gate_classes(node) do
    refused =
      node
      |> Map.take(["select_list", "where_clause", "group_expressions"])
      |> classes()
      |> Enum.find(&(&1 in @refused_classes))

    case refused do
      nil -> :ok
      class -> {:error, {:unsupported_expression, class}}
    end
  end

  defp gate_volatile(node) do
    volatile =
      node
      |> Map.take(["select_list", "where_clause", "group_expressions"])
      |> collect_values("function_name", [])
      |> Enum.find(&(&1 in @volatile))

    case volatile do
      nil -> :ok
      name -> {:error, {:volatile_function, name}}
    end
  end

  defp classes(node), do: collect_values(node, "class", [])

  defp collect_values(node, key, acc) when is_map(node) do
    acc =
      case node[key] do
        nil -> acc
        value -> [value | acc]
      end

    Enum.reduce(node, acc, fn {_key, value}, inner -> collect_values(value, key, inner) end)
  end

  defp collect_values(node, key, acc) when is_list(node),
    do: Enum.reduce(node, acc, &collect_values(&1, key, &2))

  defp collect_values(_leaf, _key, acc), do: acc

  defp group_keys(%{"aggregate_handling" => "FORCE_AGGREGATES"} = node, _table_columns) do
    keys =
      node["select_list"]
      |> Enum.reject(&nested_aggregate?([&1]))
      |> Enum.map(&Map.put(&1, "alias", ""))

    {:ok, keys}
  end

  defp group_keys(node, table_columns) do
    node["group_expressions"]
    |> Enum.reduce_while({:ok, []}, fn expression, {:ok, acc} ->
      case resolve_key(expression, node["select_list"], table_columns) do
        {:ok, resolved} -> {:cont, {:ok, [resolved | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, keys} -> {:ok, Enum.reverse(keys)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_key(
         %{"class" => "COLUMN_REF", "column_names" => [name]} = expression,
         items,
         columns
       ) do
    cond do
      name in columns -> {:ok, expression}
      item = Enum.find(items, &(&1["alias"] == name)) -> {:ok, Map.put(item, "alias", "")}
      true -> {:error, {:unknown_group_reference, name}}
    end
  end

  defp resolve_key(expression, _items, _columns), do: {:ok, expression}

  defp classified_items(node, keys) do
    normalized_keys = Enum.map(keys, &normalize/1)

    node["select_list"]
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case classify_item(item, normalized_keys) do
        {:ok, classified} -> {:cont, {:ok, [classified | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, items} -> ensure_aggregated(Enum.reverse(items), keys)
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify_item(item, normalized_keys) do
    case Enum.find_index(normalized_keys, &(&1 == normalize(item))) do
      nil -> classify_aggregate(item)
      index -> {:ok, {:group, index}}
    end
  end

  defp classify_aggregate(%{"class" => "FUNCTION", "function_name" => name} = item)
       when name in @aggregates do
    cond do
      item["distinct"] -> {:error, {:distinct_aggregate, name}}
      item["filter"] != nil -> {:error, {:filtered_aggregate, name}}
      item["order_bys"]["orders"] != [] -> {:error, {:ordered_aggregate, name}}
      nested_aggregate?(item["children"]) -> {:error, {:nested_aggregate, name}}
      true -> {:ok, {:aggregate, name, item}}
    end
  end

  defp classify_aggregate(%{"class" => "FUNCTION", "function_name" => name} = item) do
    if nested_aggregate?([item]) do
      {:error, {:unsupported_aggregate_shape, name}}
    else
      {:error, {:ungrouped_expression, name}}
    end
  end

  defp classify_aggregate(_item), do: {:error, :ungrouped_expression}

  defp nested_aggregate?(children) do
    children
    |> classes_and_names()
    |> Enum.any?(fn name -> name in @aggregates end)
  end

  defp classes_and_names(node), do: collect_values(node, "function_name", [])

  defp ensure_aggregated(items, keys) do
    aggregates = Enum.count(items, &match?({:aggregate, _name, _item}, &1))

    if aggregates == 0 and keys == [] do
      {:error, :nothing_to_merge}
    else
      {:ok, items}
    end
  end

  defp gate_outputs(items, outputs) do
    if length(items) == length(outputs) do
      :ok
    else
      {:error, :describe_mismatch}
    end
  end

  defp normalize(node) when is_map(node) do
    node
    |> Map.drop(["query_location", "alias"])
    |> Map.new(fn {key, value} -> {key, normalize(value)} end)
  end

  defp normalize(node) when is_list(node), do: Enum.map(node, &normalize/1)
  defp normalize(leaf), do: leaf

  defp partial(connection, node, keys, items) do
    select_list =
      Enum.with_index(keys, fn key, index -> Map.put(key, "alias", "#{@prefix}g#{index}") end) ++
        Enum.with_index(items, fn item, index -> partial_aggregates(item, index) end)

    partial_node =
      node
      |> Map.put("select_list", List.flatten(select_list))
      |> Map.put(
        "group_expressions",
        Enum.with_index(keys, fn _key, i -> column_ref("#{@prefix}g#{i}") end)
      )
      |> Map.put("group_sets", group_sets(keys))
      |> Map.put("aggregate_handling", "STANDARD_HANDLING")
      |> Map.put("modifiers", [])

    with {:ok, sql} <- deserialize(connection, partial_node) do
      exact(connection, sql)
    end
  end

  defp exact(connection, partial_sql) do
    with {:ok, columns} <- Connection.describe(connection, partial_sql, :infinity) do
      cond do
        Enum.any?(columns, fn {name, type} ->
          type == "HUGEINT" and not String.starts_with?(name, "#{@prefix}a")
        end) ->
          {:error, :hugeint_group_key}

        Enum.any?(columns, fn {_name, type} -> type == "HUGEINT" end) ->
          {:ok, "SELECT #{exact_select(columns)} FROM (#{partial_sql})"}

        true ->
          {:ok, partial_sql}
      end
    end
  end

  defp exact_select(columns) do
    Enum.map_join(columns, ", ", fn
      {name, "HUGEINT"} -> "CAST(#{quoted(name)} AS DECIMAL(38,0)) AS #{quoted(name)}"
      {name, _type} -> quoted(name)
    end)
  end

  defp partial_aggregates({:group, _index}, _position), do: []

  defp partial_aggregates({:aggregate, "avg", item}, position) do
    [
      item |> Map.put("function_name", "sum") |> Map.put("alias", "#{@prefix}a#{position}_s"),
      item |> Map.put("function_name", "count") |> Map.put("alias", "#{@prefix}a#{position}_c")
    ]
  end

  defp partial_aggregates({:aggregate, _name, item}, position),
    do: [Map.put(item, "alias", "#{@prefix}a#{position}")]

  defp group_sets([]), do: []
  defp group_sets(keys), do: [Enum.to_list(0..(length(keys) - 1))]

  defp column_ref(name),
    do: %{
      "alias" => "",
      "class" => "COLUMN_REF",
      "type" => "COLUMN_REF",
      "column_names" => [name]
    }

  defp deserialize(connection, node) do
    ast = %{
      "error" => false,
      "statements" => [%{"named_param_map" => [], "node" => node}]
    }

    quoted = Smolquery.Identifier.sql_string(JSON.encode!(ast))

    with {:ok, result} <-
           Connection.query(
             connection,
             "SELECT json_deserialize_sql(#{quoted}::JSON)",
             [],
             :infinity
           ),
         [[sql]] when is_binary(sql) <- result.rows do
      {:ok, sql}
    else
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:error, :deserialize_failed}
    end
  end

  defp final_select(items, outputs) do
    items
    |> Enum.zip(outputs)
    |> Enum.with_index()
    |> Enum.map_join(", ", fn {{item, {name, type}}, position} ->
      merged(item, position, type) <> " AS #{quoted(name)}"
    end)
  end

  defp merged({:group, index}, _position, _type), do: quoted("#{@prefix}g#{index}")

  defp merged({:aggregate, "avg", _item}, position, type) do
    "CAST(CAST(sum(#{quoted("#{@prefix}a#{position}_s")}) AS DOUBLE) / " <>
      "CAST(sum(#{quoted("#{@prefix}a#{position}_c")}) AS DOUBLE) AS #{type})"
  end

  defp merged({:aggregate, name, _item}, position, type) when name in @mergeable do
    "CAST(#{merge_function(name)}(#{quoted("#{@prefix}a#{position}")}) AS #{type})"
  end

  defp merge_function("min"), do: "min"
  defp merge_function("max"), do: "max"
  defp merge_function(_count_or_sum), do: "sum"

  defp final_group([]), do: ""

  defp final_group(keys) do
    "GROUP BY " <>
      Enum.map_join(0..(length(keys) - 1), ", ", fn index -> quoted("#{@prefix}g#{index}") end)
  end

  defp tail(node, outputs) do
    names = Enum.map(outputs, fn {name, _type} -> name end)

    node["modifiers"]
    |> Enum.reduce_while({:ok, []}, fn modifier, {:ok, acc} ->
      case render_modifier(modifier, names) do
        {:ok, clause} -> {:cont, {:ok, [clause | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, clauses} ->
        {:ok, clauses |> Enum.reverse() |> Enum.reject(&(&1 == "")) |> Enum.join(" ")}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp render_modifier(%{"type" => "ORDER_MODIFIER", "orders" => orders}, names) do
    orders
    |> Enum.reduce_while({:ok, []}, fn order, {:ok, acc} ->
      case render_order(order, names) do
        {:ok, rendered} -> {:cont, {:ok, [rendered | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, rendered} ->
        {:ok,
         IO.iodata_to_binary(["ORDER BY " | rendered |> Enum.reverse() |> Enum.intersperse(", ")])}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp render_modifier(%{"type" => "LIMIT_MODIFIER", "limit" => limit}, _names) do
    case limit do
      %{"class" => "CONSTANT", "value" => %{"is_null" => false, "value" => count}}
      when is_integer(count) and count >= 0 ->
        {:ok, "LIMIT #{count}"}

      nil ->
        {:ok, ""}

      _expression ->
        {:error, :unsupported_limit}
    end
  end

  defp render_order(order, names) do
    case order["expression"] do
      %{"class" => "COLUMN_REF", "column_names" => [name]} ->
        if name in names do
          {:ok, String.trim("#{quoted(name)} #{direction(order)} #{nulls(order)}")}
        else
          {:error, {:order_by_unknown_column, name}}
        end

      _expression ->
        {:error, :order_by_expression}
    end
  end

  defp direction(%{"type" => "ASCENDING"}), do: "ASC"
  defp direction(%{"type" => "DESCENDING"}), do: "DESC"
  defp direction(_default), do: ""

  defp nulls(%{"null_order" => "NULLS_FIRST"}), do: "NULLS FIRST"
  defp nulls(%{"null_order" => "NULLS_LAST"}), do: "NULLS LAST"
  defp nulls(_default), do: ""

  defp quoted(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""
end
