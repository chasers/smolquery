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
  that is not one base table, CTEs, `SELECT *`, DISTINCT ON, QUALIFY,
  SAMPLE, grouping sets, window functions, subqueries anywhere, a DISTINCT
  aggregate other than `count` and `list`, an aggregate outside the five
  above, their conditional forms and the ones that merge through a value,
  OFFSET, and ORDER BY on anything but an output column: its name, or an
  expression that is a select item's own (T-536), which is how HyperDX
  orders its histogram (`ORDER BY toStartOfInterval(...)`) and a top-k chart
  its groups (`ORDER BY count() DESC`). The final step orders by the output
  column that item became, which is the same order. A constant is not such
  an expression: `ORDER BY 2` is a position to the engine, and would match
  a select item that is the literal `2`. A select
  item must be a supported aggregate or match a group expression. Table
  columns prefixed `__pq_` would collide with the generated aliases, so
  they refuse too. Volatile functions — `now()`, `random()`, and their
  kin — refuse as well: each worker would bind them on its own clock or
  seed, and the shards would disagree with the single-engine answer.

  ## A condition on an aggregate

  `agg(x) FILTER (WHERE c)` splits as `agg(x)` does (T-537): the condition
  decides which rows of a shard the partial sees, and the merge is the
  same. ClickHouse writes it `countIf(c)`, `sumIf(x, c)`, `avgIf`, `minIf`,
  `maxIf`, which is how a chart counts its errors beside its total.
  `countIf` is the engine's own aggregate and merges as a sum. The others
  are this codebase's macros over the filtered form, and are read as that
  form, so `avgIf(x, c)` splits into a filtered sum and a filtered count of
  `x` as `avg` does.

  ## Aggregates that merge through a value

  Some aggregates have no number to add up, and merge exactly all the same
  (T-539), because what a shard answers is a value the final step can choose
  among, or a list it can join:

  - `any(x)`: any shard's answer is an answer.
  - `argMax(v, k)`, `argMin`: the partial carries `v` and the `k` it was
    taken at; the final takes the arg of the args. The engine skips a row
    whose `v` is NULL, so the partial's `k` is taken over rows with a `v`
    too, or a shard's largest `k` could belong to a row the engine ignored.
  - `groupArray`, `list`: partial lists, flattened. `groupUniqArray`,
    `list(DISTINCT x)`: the same, de-duplicated. `list(DISTINCT x)` keeps a
    NULL and `list_distinct` drops one, so the final puts it back when any
    partial held one. `groupUniqArrayArray` flattens and de-duplicates in
    the partial, as its macro does, so a shard ships its distinct elements
    and not every row's array. An `n` slices the partial and the final list
    both: `n` of each shard's are enough for `n` of the union. A list over
    no rows is NULL, not an empty list, so the final answers NULL when no
    partial holds one.
  - `uniq`, `uniqExact`, `count(DISTINCT x)`: partial lists of distinct
    values, and the count of their distinct union. Exact, and not the sum of
    the shards' counts, which would count a value once per shard.

  A value reaches the final step through parquet, which has no HUGEINT, no
  UNION, no VARIANT and no ENUM: a HUGEINT list degrades to DOUBLE, an ENUM
  key compares as text. A partial column of such a type refuses, as a
  HUGEINT group key always has.

  Every one but `any` ships values, and as many as the data holds distinct.
  A decomposition that does says so (`value_lists`), and
  `Smolquery.QueryService.Scatter` refuses it over a plan with more rows
  than the runtime's `value_list_max_rows`.

  Which element a list holds first, which of two tied rows an `argMax`
  answers and which value `any` picks are the engine's to choose on one
  engine and on many.

  ## HAVING, and SELECT DISTINCT

  HAVING is a condition on groups, and a shard has only its part of a group,
  so it cannot run in the partial. It runs over the merged result (T-538):
  every aggregate and key it names must be a select item, by shape or by
  alias, and is read as that item's output column; the final query is then
  filtered from outside, before its ORDER BY and LIMIT. An aggregate that is
  no select item would need a partial of its own, and refuses: whether a
  function is one is the engine's catalog's to say, and
  `ClickHouseFunctions.aggregate?/1`'s for a macro, so `bool_or` refuses
  here and not at the merge, after every shard has run. A `$n` refuses too,
  since the final step binds none. So does a
  name that is a column of the table and the alias of some other select
  item, which the engine and this module might read differently.

  `SELECT DISTINCT a, b` with no aggregate is `GROUP BY a, b`: each shard
  answers its own distinct rows and the final step groups them again.
  `DISTINCT ON`, and DISTINCT over aggregates, refuse.

  ## A bare `count(*)` has no scan to shard

  `SELECT count(*) FROM t` — no WHERE, no group keys, nothing but
  `count(*)` in the select list — refuses as `:metadata_only` (T-448).
  DuckDB answers it from each parquet footer's row count without reading a
  row group, so there is nothing to parallelize, and the scatter would
  still pay an engine per worker, a partial per shard, the transfer and
  the merge; prod measured it slower distributed than not. `count(col)`
  scans for nulls and stays eligible, and so does any count under a WHERE
  or a GROUP BY.

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

  alias Smolquery.Engine.Ast
  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.Stability

  @mergeable ~w(count_star count countif count_if sum min max)
  @conditional %{"sumif" => "sum", "avgif" => "avg", "minif" => "min", "maxif" => "max"}
  @aggregates ["avg" | @mergeable]
  @by_value %{
    "any_value" => :any,
    "arg_max" => {:arg, "max"},
    "argmax" => {:arg, "max"},
    "max_by" => {:arg, "max"},
    "arg_min" => {:arg, "min"},
    "argmin" => {:arg, "min"},
    "min_by" => {:arg, "min"},
    "list" => :list,
    "grouparray" => :list,
    "groupuniqarray" => :distinct_list,
    "groupuniqarrayarray" => :flat_distinct_list,
    "uniq" => :count_distinct,
    "uniqexact" => :count_distinct
  }
  @aggregate_names @aggregates ++ Map.keys(@conditional) ++ Map.keys(@by_value)
  @prefix "__pq_"
  @filter_prefix "SELECT 1 WHERE "
  @refused_classes ~w(SUBQUERY WINDOW STAR)
  @volatile ~w(now now64 get_current_timestamp current_date current_localtime
               current_localtimestamp today random uuid uuidv4 uuidv7
               gen_random_uuid setseed nextval currval)

  @enforce_keys [:partial_sql, :final_select, :final_group, :final_tail]
  defstruct [
    :partial_sql,
    :final_select,
    :final_group,
    :final_tail,
    final_having: "",
    value_lists: false,
    params: []
  ]

  @type t :: %__MODULE__{
          partial_sql: String.t(),
          params: [term()],
          final_select: String.t(),
          final_group: String.t(),
          final_tail: String.t(),
          final_having: String.t(),
          value_lists: boolean()
        }

  @type output :: {String.t(), String.t()}

  @doc """
  Decomposes `sql` for a table whose columns are `table_columns`, given the
  `DESCRIBE` outputs of the original statement.

  `connection` is used to serialize, to deserialize, and to `DESCRIBE` the
  partial for the integer-exactness cast — three round trips, no data
  touched. The partial references the planned view name, so the connection
  must already hold the plan's views.

  `params` bind the query's `$n` placeholders: the `DESCRIBE` that checks
  the partial binds them, and they ride the result as `params` so each
  scatter partial binds the same values.
  """
  @spec decompose(GenServer.server(), String.t(), [output()], [String.t()], [term()]) ::
          {:ok, t()} | {:error, term()}
  def decompose(connection, sql, outputs, table_columns, params \\ []) do
    with :ok <- gate_columns(table_columns),
         {:ok, node} <- select_node(connection, sql),
         {:ok, node} <- distinct_as_groups(node),
         :ok <- gate_shape(node),
         :ok <- gate_classes(node),
         :ok <- gate_volatile(node),
         {:ok, keys} <- group_keys(node, table_columns),
         {:ok, items} <- classified_items(node, keys),
         :ok <- gate_scan(node, keys, items),
         :ok <- gate_outputs(items, outputs),
         {:ok, tail} <- tail(node, outputs),
         {:ok, having} <- having(connection, node, outputs, table_columns),
         {:ok, partial_sql} <- partial(connection, node, keys, items, params) do
      {:ok,
       %__MODULE__{
         partial_sql: partial_sql,
         params: params,
         final_select: final_select(items, outputs),
         final_group: final_group(keys),
         final_having: having,
         final_tail: tail,
         value_lists: Enum.any?(items, &match?({:by_value, kind, _item} when kind != :any, &1))
       }}
    end
  end

  @doc """
  The final query over `from` — a `read_parquet` across the partial files.
  """
  @spec final_sql(t(), String.t()) :: String.t()
  def final_sql(%__MODULE__{final_having: ""} = decomposition, from) do
    [merged_sql(decomposition, from)]
    |> append(decomposition.final_tail)
    |> Enum.join(" ")
  end

  def final_sql(%__MODULE__{} = decomposition, from) do
    ["SELECT * FROM (#{merged_sql(decomposition, from)}) WHERE #{decomposition.final_having}"]
    |> append(decomposition.final_tail)
    |> Enum.join(" ")
  end

  defp merged_sql(decomposition, from) do
    ["SELECT #{decomposition.final_select} FROM #{from}"]
    |> append(decomposition.final_group)
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
      |> Map.take(["select_list", "where_clause", "group_expressions", "having"])
      |> classes()
      |> Enum.find(&(&1 in @refused_classes))

    case refused do
      nil -> :ok
      class -> {:error, {:unsupported_expression, class}}
    end
  end

  defp volatile_macros, do: Enum.map(ClickHouseFunctions.volatile(), &String.downcase/1)

  defp gate_volatile(node) do
    volatile =
      node
      |> Map.take(["select_list", "where_clause", "group_expressions", "having"])
      |> collect_values("function_name", [])
      |> Enum.find(&(&1 in @volatile or &1 in volatile_macros()))

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
    normalized_keys = Enum.map(keys, &Ast.shape/1)

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
    case Enum.find_index(normalized_keys, &(&1 == Ast.shape(item))) do
      nil -> classify_aggregate(item)
      index -> {:ok, {:group, index}}
    end
  end

  defp classify_aggregate(
         %{
           "class" => "FUNCTION",
           "function_name" => name,
           "children" => [value, condition],
           "filter" => nil
         } = item
       )
       when is_map_key(@conditional, name) do
    classify_aggregate(%{
      item
      | "function_name" => Map.fetch!(@conditional, name),
        "children" => [value],
        "filter" => condition
    })
  end

  defp classify_aggregate(
         %{"class" => "FUNCTION", "function_name" => "count", "distinct" => true} = item
       ),
       do: by_value(:count_distinct, "count", %{item | "distinct" => false})

  defp classify_aggregate(
         %{"class" => "FUNCTION", "function_name" => "list", "distinct" => true} = item
       ),
       do: by_value(:distinct_list, "list", %{item | "distinct" => false})

  defp classify_aggregate(%{"class" => "FUNCTION", "function_name" => name} = item)
       when is_map_key(@by_value, name),
       do: by_value(Map.fetch!(@by_value, name), name, item)

  defp classify_aggregate(%{"class" => "FUNCTION", "function_name" => name} = item)
       when name in @aggregates do
    cond do
      item["distinct"] ->
        {:error, {:distinct_aggregate, name}}

      item["order_bys"]["orders"] != [] ->
        {:error, {:ordered_aggregate, name}}

      nested_aggregate?([item["filter"] | item["children"]]) ->
        {:error, {:nested_aggregate, name}}

      true ->
        {:ok, {:aggregate, name, item}}
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

  defp by_value(kind, name, item) do
    cond do
      item["distinct"] ->
        {:error, {:distinct_aggregate, name}}

      item["order_bys"]["orders"] != [] ->
        {:error, {:ordered_aggregate, name}}

      item["filter"] != nil and kind != :any ->
        {:error, {:filtered_aggregate, name}}

      nested_aggregate?([item["filter"] | item["children"]]) ->
        {:error, {:nested_aggregate, name}}

      not arguments?(kind, item["children"]) ->
        {:error, {:unsupported_aggregate_shape, name}}

      true ->
        {:ok, {:by_value, kind, item}}
    end
  end

  defp arguments?({:arg, _extreme}, [_value, _key]), do: true
  defp arguments?(kind, [_value]) when kind in [:any, :count_distinct], do: true
  defp arguments?(kind, [_value]) when is_atom(kind), do: true

  defp arguments?(kind, [_value, %{"class" => "CONSTANT", "value" => %{"value" => n}}])
       when kind in [:list, :distinct_list, :flat_distinct_list] and is_integer(n) and n > 0,
       do: true

  defp arguments?(_kind, _children), do: false

  defp nested_aggregate?(children) do
    children
    |> classes_and_names()
    |> Enum.any?(fn name -> name in @aggregate_names end)
  end

  defp classes_and_names(node), do: collect_values(node, "function_name", [])

  defp ensure_aggregated(items, keys) do
    aggregates = Enum.count(items, &(elem(&1, 0) in [:aggregate, :by_value]))

    if aggregates == 0 and keys == [] do
      {:error, :nothing_to_merge}
    else
      {:ok, items}
    end
  end

  # Nothing but `count(*)`, with no keys and no WHERE, is answered from
  # parquet footers and hot-manifest row counts rather than a scan; sharding
  # it only adds the fixed costs (T-448).
  defp gate_scan(%{"where_clause" => nil}, [], items) do
    if Enum.all?(items, &match?({:aggregate, "count_star", %{"filter" => nil}}, &1)) do
      {:error, :metadata_only}
    else
      :ok
    end
  end

  defp gate_scan(_node, _keys, _items), do: :ok

  defp gate_outputs(items, outputs) do
    if length(items) == length(outputs) do
      :ok
    else
      {:error, :describe_mismatch}
    end
  end

  defp partial(connection, node, keys, items, params) do
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
      |> Map.put("having", nil)
      |> Map.put("modifiers", [])

    with {:ok, sql} <- deserialize(connection, partial_node) do
      exact(connection, sql, params)
    end
  end

  defp exact(connection, partial_sql, params) do
    with {:ok, columns} <- Connection.describe(connection, partial_sql, params, :infinity),
         :ok <- exact_columns(columns) do
      if Enum.any?(columns, &match?({_name, "HUGEINT"}, &1)),
        do: {:ok, "SELECT #{exact_select(columns)} FROM (#{partial_sql})"},
        else: {:ok, partial_sql}
    end
  end

  defp exact_columns(columns) do
    Enum.reduce_while(columns, :ok, fn {name, type}, :ok ->
      cond do
        type == "HUGEINT" and not String.starts_with?(name, "#{@prefix}a") ->
          {:halt, {:error, :hugeint_group_key}}

        String.starts_with?(name, "#{@prefix}a") and type != "HUGEINT" and
            type =~ ~r/HUGEINT|UNION|VARIANT|ENUM/ ->
          {:halt, {:error, {:inexact_partial_column, type}}}

        true ->
          {:cont, :ok}
      end
    end)
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

  defp partial_aggregates({:by_value, :any, item}, position),
    do: [Map.put(item, "alias", "#{@prefix}a#{position}")]

  defp partial_aggregates(
         {:by_value, {:arg, extreme}, %{"children" => [value, key]} = item},
         position
       ) do
    [
      Map.put(item, "alias", "#{@prefix}a#{position}_v"),
      item
      |> called(extreme, [key])
      |> Map.put("filter", not_null(value))
      |> Map.put("alias", "#{@prefix}a#{position}_k")
    ]
  end

  defp partial_aggregates({:by_value, kind, %{"children" => [value | limit]} = item}, position) do
    values =
      item
      |> called("list", [value])
      |> Map.put("distinct", kind in [:distinct_list, :count_distinct])
      |> Map.put("filter", if(kind == :count_distinct, do: not_null(value)))

    [values |> distinct_flat(kind, item) |> sliced_to(limit, item) |> aliased(position)]
  end

  defp distinct_flat(values, :flat_distinct_list, item),
    do: called(item, "list_distinct", [called(item, "flatten", [values])])

  defp distinct_flat(values, _kind, _item), do: values

  defp sliced_to(values, [], _item), do: values
  defp sliced_to(values, [n], item), do: called(item, "list_slice", [values, whole(1), n])

  defp aliased(node, position), do: Map.put(node, "alias", "#{@prefix}a#{position}")

  defp whole(value) do
    %{
      "class" => "CONSTANT",
      "type" => "VALUE_CONSTANT",
      "alias" => "",
      "value" => %{
        "type" => %{"id" => "INTEGER", "type_info" => nil},
        "is_null" => false,
        "value" => value
      }
    }
  end

  defp called(item, name, children) do
    %{
      item
      | "function_name" => name,
        "children" => children,
        "distinct" => false,
        "alias" => ""
    }
  end

  defp not_null(value) do
    %{
      "class" => "OPERATOR",
      "type" => "OPERATOR_IS_NOT_NULL",
      "alias" => "",
      "children" => [Map.put(value, "alias", "")]
    }
  end

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

  defp merged({:by_value, :any, _item}, position, type),
    do: "CAST(any_value(#{quoted("#{@prefix}a#{position}")}) AS #{type})"

  defp merged({:by_value, {:arg, extreme}, _item}, position, type) do
    "CAST(arg_#{extreme}(#{quoted("#{@prefix}a#{position}_v")}, " <>
      "#{quoted("#{@prefix}a#{position}_k")}) AS #{type})"
  end

  defp merged({:by_value, :count_distinct, _item}, position, type),
    do: "CAST(len(list_distinct(#{flat(position)})) AS #{type})"

  defp merged({:by_value, :list, item}, position, type),
    do: "CAST(#{position |> flat() |> sliced(item) |> unless_empty(position)} AS #{type})"

  defp merged({:by_value, :flat_distinct_list, item}, position, type) do
    distinct = "list_distinct(#{flat(position)})"

    "CAST(#{distinct |> sliced(item) |> unless_empty(position)} AS #{type})"
  end

  defp merged({:by_value, :distinct_list, item}, position, type) do
    part = quoted("#{@prefix}a#{position}")

    distinct =
      "list_concat(list_distinct(#{flat(position)}), " <>
        "CASE WHEN bool_or(len(#{part}) > list_count(#{part})) THEN [NULL] ELSE [] END)"

    "CAST(#{distinct |> sliced(item) |> unless_empty(position)} AS #{type})"
  end

  defp unless_empty(list, position),
    do: "CASE WHEN count(#{quoted("#{@prefix}a#{position}")}) = 0 THEN NULL ELSE #{list} END"

  defp flat(position), do: "flatten(list(#{quoted("#{@prefix}a#{position}")}))"

  defp sliced(list, %{"children" => [_value, %{"value" => %{"value" => n}}]}),
    do: "list_slice(#{list}, 1, #{n})"

  defp sliced(list, _item), do: list

  defp merge_function("min"), do: "min"
  defp merge_function("max"), do: "max"
  defp merge_function(_count_or_sum), do: "sum"

  defp final_group([]), do: ""

  defp final_group(keys) do
    "GROUP BY " <>
      Enum.map_join(0..(length(keys) - 1), ", ", fn index -> quoted("#{@prefix}g#{index}") end)
  end

  defp distinct_as_groups(%{"modifiers" => modifiers} = node) do
    case Enum.split_with(modifiers, &(&1["type"] == "DISTINCT_MODIFIER")) do
      {[], _modifiers} ->
        {:ok, node}

      {[%{"distinct_on_targets" => []}], rest} ->
        if node["group_expressions"] == [] and not nested_aggregate?(node["select_list"]),
          do: {:ok, %{node | "modifiers" => rest, "aggregate_handling" => "FORCE_AGGREGATES"}},
          else: {:error, :distinct_over_aggregates}

      {_distinct_on, _rest} ->
        {:error, :distinct_on}
    end
  end

  defp having(_connection, %{"having" => nil}, _outputs, _columns), do: {:ok, ""}

  defp having(connection, %{"having" => condition} = node, outputs, columns) do
    names = Enum.map(outputs, fn {name, _type} -> name end)
    items = node["select_list"] |> Enum.map(&Ast.shape/1) |> Enum.zip(names)

    with :ok <- gate_having_parameters(condition),
         {:ok, over_outputs} <- over_outputs(condition, {names, items, columns}),
         :ok <- gate_scalars(connection, over_outputs, :having_aggregate),
         {:ok, sql} <- deserialize(connection, filter(over_outputs)),
         @filter_prefix <> rendered <- sql do
      {:ok, rendered}
    else
      {:error, reason} -> {:error, reason}
      _another_rendering -> {:error, :having_not_rendered}
    end
  end

  defp gate_having_parameters(condition) do
    if "PARAMETER" in classes(condition), do: {:error, :parameter_in_having}, else: :ok
  end

  defp gate_scalars(connection, tree, reason) do
    names = Stability.function_names(tree)

    case Enum.find(names, &ClickHouseFunctions.aggregate?/1) do
      nil -> gate_catalog_scalars(connection, Stability.checked(names), reason)
      macro -> {:error, {reason, macro}}
    end
  end

  defp gate_catalog_scalars(_connection, [], _reason), do: :ok

  defp gate_catalog_scalars(connection, names, reason) do
    case Connection.query(
           connection,
           "SELECT #{Stability.not_scalar_names_sql(names)}",
           [],
           :infinity
         ) do
      {:ok, %{rows: [[[]]]}} -> :ok
      {:ok, %{rows: [[[name | _more]]]}} -> {:error, {reason, name}}
      {:ok, _another_shape} -> {:error, {reason, :unknown}}
      {:error, error} -> {:error, error}
    end
  end

  defp filter(condition) do
    %{
      "type" => "SELECT_NODE",
      "modifiers" => [],
      "cte_map" => %{"map" => []},
      "select_list" => [
        %{
          "class" => "CONSTANT",
          "type" => "VALUE_CONSTANT",
          "alias" => "",
          "value" => %{
            "type" => %{"id" => "INTEGER", "type_info" => nil},
            "is_null" => false,
            "value" => 1
          }
        }
      ],
      "from_table" => %{"type" => "EMPTY", "alias" => "", "sample" => nil},
      "where_clause" => condition,
      "group_expressions" => [],
      "group_sets" => [],
      "aggregate_handling" => "STANDARD_HANDLING",
      "having" => nil,
      "sample" => nil,
      "qualify" => nil
    }
  end

  defp over_outputs(%{"class" => _class} = expression, {names, items, _columns} = outputs) do
    case for({shape, name} <- items, shape == Ast.shape(expression), do: name) do
      [name | _same_item_again] -> output_column(name, names)
      [] -> over_outputs_within(expression, outputs)
    end
  end

  defp over_outputs(node, outputs) when is_map(node) do
    Enum.reduce_while(node, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case over_outputs(value, outputs) do
        {:ok, rewritten} -> {:cont, {:ok, Map.put(acc, key, rewritten)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp over_outputs(nodes, outputs) when is_list(nodes) do
    nodes
    |> Enum.reduce_while({:ok, []}, fn node, {:ok, acc} ->
      case over_outputs(node, outputs) do
        {:ok, rewritten} -> {:cont, {:ok, [rewritten | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, rewritten} -> {:ok, Enum.reverse(rewritten)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp over_outputs(leaf, _outputs), do: {:ok, leaf}

  defp over_outputs_within(
         %{"class" => "COLUMN_REF", "column_names" => [name]},
         {names, _items, columns}
       ) do
    if name in columns and name in names,
      do: {:error, {:having_ambiguous, name}},
      else: output_column(name, names)
  end

  defp over_outputs_within(%{"class" => "COLUMN_REF", "column_names" => names}, _outputs),
    do: {:error, {:having_reference, Enum.join(names, ".")}}

  defp over_outputs_within(%{"class" => "FUNCTION", "function_name" => name} = call, outputs) do
    if name in @aggregate_names or call["filter"] != nil or call["distinct"],
      do: {:error, {:having_aggregate, name}},
      else: call |> Map.delete("class") |> over_outputs(outputs) |> reclassed("FUNCTION")
  end

  defp over_outputs_within(%{"class" => class} = expression, outputs),
    do: expression |> Map.delete("class") |> over_outputs(outputs) |> reclassed(class)

  defp reclassed({:ok, node}, class), do: {:ok, Map.put(node, "class", class)}
  defp reclassed({:error, reason}, _class), do: {:error, reason}

  defp output_column(name, names) do
    case Enum.count(names, &(&1 == name)) do
      1 -> {:ok, column_ref(name)}
      0 -> {:error, {:having_reference, name}}
      _shared -> {:error, {:having_ambiguous, name}}
    end
  end

  defp tail(node, outputs) do
    names = Enum.map(outputs, fn {name, _type} -> name end)
    items = node["select_list"] |> Enum.map(&Ast.shape/1) |> Enum.zip(names)

    node["modifiers"]
    |> Enum.reduce_while({:ok, []}, fn modifier, {:ok, acc} ->
      case render_modifier(modifier, {names, items}) do
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

  defp render_modifier(%{"type" => "ORDER_MODIFIER", "orders" => orders}, outputs) do
    orders
    |> Enum.reduce_while({:ok, []}, fn order, {:ok, acc} ->
      case render_order(order, outputs) do
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

  defp render_modifier(%{"type" => "LIMIT_MODIFIER", "limit" => limit}, _outputs) do
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

  defp render_order(order, {names, items}) do
    case order["expression"] do
      %{"class" => "COLUMN_REF", "column_names" => [name]} ->
        if name in names,
          do: {:ok, ordered(name, order)},
          else: {:error, {:order_by_unknown_column, name}}

      %{"class" => "CONSTANT"} ->
        {:error, :order_by_position}

      expression ->
        ordered_by_item(Ast.shape(expression), order, {names, items})
    end
  end

  defp ordered_by_item(expression, order, {names, items}) do
    with [name | _same_item_again] <- for({^expression, name} <- items, do: name),
         1 <- Enum.count(names, &(&1 == name)) do
      {:ok, ordered(name, order)}
    else
      _no_item_or_an_ambiguous_name -> {:error, :order_by_expression}
    end
  end

  defp ordered(name, order),
    do: String.trim("#{quoted(name)} #{direction(order)} #{nulls(order)}")

  defp direction(%{"type" => "ASCENDING"}), do: "ASC"
  defp direction(%{"type" => "DESCENDING"}), do: "DESC"
  defp direction(_default), do: ""

  defp nulls(%{"null_order" => "NULLS_FIRST"}), do: "NULLS FIRST"
  defp nulls(%{"null_order" => "NULLS_LAST"}), do: "NULLS LAST"
  defp nulls(_default), do: ""

  defp quoted(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""
end
