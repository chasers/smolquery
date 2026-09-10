defmodule Smolquery.QueryService.SingleTable do
  @moduledoc """
  The statement shape the planner's bounds share: one SELECT over one table.

  `Smolquery.QueryService.TopN` (an ordered `LIMIT n`) and
  `Smolquery.QueryService.AnyN` (an unordered one) both prune a table's hot
  entries by a bound they derive from the statement, and both are only sound
  when the statement is one SELECT over exactly one table reference — no
  join, no CTE, no subquery or window anywhere, no GROUP BY, HAVING, QUALIFY,
  SAMPLE or DISTINCT — with a constant LIMIT. This module is that shared
  reading of DuckDB's serialized AST, so the two bounds cannot drift on what
  "one table" means. Each still names the expression classes it excludes:
  the any-N bound refuses any function call, the Top-N bound only subqueries
  and windows.
  """

  alias Smolquery.Catalog

  @typedoc "The one base table a statement reads, and the name its columns resolve under."
  @type source :: %{ref: Catalog.table_ref(), name: String.t()}

  @doc """
  The statement's FROM as one plain base table among `refs`, or `:error`.

  Plain means dataset-qualified, unpinned (`AT` refuses), unsampled, and
  without a `FROM t AS e(a, b)` column rename, which moves names between
  positions.
  """
  @spec source(map() | nil, [Catalog.table_ref()]) :: {:ok, source()} | :error
  def source(
        %{
          "type" => "BASE_TABLE",
          "schema_name" => dataset,
          "table_name" => table,
          "alias" => alias,
          "catalog_name" => "",
          "at_clause" => nil,
          "sample" => nil,
          "column_name_alias" => []
        },
        refs
      )
      when dataset != "" do
    ref = {dataset, table}
    name = if alias == "", do: table, else: alias

    if ref in refs, do: {:ok, %{ref: ref, name: name}}, else: :error
  end

  def source(_from, _refs), do: :error

  @doc """
  Whether the SELECT node groups, filters groups, qualifies, samples, or
  opens a CTE — any of which reads the pruned view and answers differently.
  """
  @spec simple?(map()) :: boolean()
  def simple?(node) do
    node["group_expressions"] == [] and Map.get(node, "group_sets", []) == [] and
      is_nil(node["having"]) and is_nil(node["qualify"]) and is_nil(node["sample"]) and
      node["aggregate_handling"] == "STANDARD_HANDLING" and
      get_in(node, ["cte_map", "map"]) in [nil, []]
  end

  @doc """
  Whether the whole statement names exactly one base table and no expression
  of an `excluded` class anywhere.
  """
  @spec single_reference?(map(), [String.t()]) :: boolean()
  def single_reference?(statement, excluded) do
    %{tables: tables, excluded: found} =
      walk(statement, %{tables: 0, excluded: false}, fn node, acc ->
        %{
          tables: acc.tables + if(node["type"] == "BASE_TABLE", do: 1, else: 0),
          excluded: acc.excluded or node["class"] in excluded
        }
      end)

    tables == 1 and not found
  end

  @doc """
  A LIMIT modifier's row count as `n + offset` — the offset rows are read
  before they are skipped — or `:error` unless both are integer constants
  (`LIMIT 10%`, `LIMIT 2 + 3` and `LIMIT $1` all refuse).
  """
  @spec limit(map()) :: {:ok, pos_integer()} | :error
  def limit(%{"limit" => limit, "offset" => offset}) do
    with {:ok, n} when n >= 1 <- integer(limit),
         {:ok, skip} when skip >= 0 <- integer(offset) do
      {:ok, n + skip}
    else
      _not_constant -> :error
    end
  end

  def limit(_modifier), do: :error

  defp integer(nil), do: {:ok, 0}

  defp integer(%{"class" => "CONSTANT", "value" => %{"is_null" => false, "value" => value}})
       when is_integer(value),
       do: {:ok, value}

  defp integer(_expression), do: :error

  @doc """
  Folds `fun` over every map node of a serialized AST, depth first — the
  walk both bounds read expression classes and function names with.
  """
  @spec walk(term(), acc, (map(), acc -> acc)) :: acc when acc: term()
  def walk(node, acc, fun) when is_map(node) do
    Enum.reduce(node, fun.(node, acc), fn {_key, value}, inner -> walk(value, inner, fun) end)
  end

  def walk(node, acc, fun) when is_list(node),
    do: Enum.reduce(node, acc, &walk(&1, &2, fun))

  def walk(_leaf, acc, _fun), do: acc
end
