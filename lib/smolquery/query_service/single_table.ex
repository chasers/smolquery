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
  alias Smolquery.Engine.Ast

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
    tags =
      Ast.collect(statement, fn node ->
        List.flatten([
          if(node["type"] == "BASE_TABLE", do: [:table], else: []),
          if(node["class"] in excluded, do: [:excluded], else: [])
        ])
      end)

    Enum.count(tags, &(&1 == :table)) == 1 and :excluded not in tags
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

  @doc """
  The leading `entries` whose row counts cover `rows`, in the order given —
  none for `rows` of zero, all of them when even all fall short. An entry
  without a row count is taken and counts for nothing.
  """
  @spec take_rows([map()], non_neg_integer()) :: [map()]
  def take_rows(entries, rows) do
    entries
    |> Enum.reduce_while({[], 0}, fn entry, {taken, covered} ->
      if covered >= rows do
        {:halt, {taken, covered}}
      else
        {:cont, {[entry | taken], covered + (entry["row_count"] || 0)}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp integer(nil), do: {:ok, 0}

  defp integer(%{"class" => "CONSTANT", "value" => %{"is_null" => false, "value" => value}})
       when is_integer(value),
       do: {:ok, value}

  defp integer(_expression), do: :error
end
