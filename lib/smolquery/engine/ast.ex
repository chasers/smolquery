defmodule Smolquery.Engine.Ast do
  @moduledoc """
  A walk over the JSON DuckDB's `json_serialize_sql` hands back.

  The tree is maps and lists all the way down, with no schema beyond each
  node's `"class"` and `"type"`; a caller that wants every node of one kind
  visits all of them and keeps what it recognises. Two readers do that: the
  planner, for table references and table functions, and the materialized
  column gates, for column references and node classes.
  """

  @doc """
  Every value `fun` returns for a node of `tree`, in document order.

  `fun` sees each map in the tree once and answers a list — empty for a node
  it does not care about — and the lists are concatenated in the order the
  nodes appear.
  """
  @spec collect(term(), (map() -> [term()])) :: [term()]
  def collect(tree, fun), do: tree |> collect(fun, []) |> Enum.reverse()

  defp collect(node, fun, acc) when is_map(node) do
    acc = node |> fun.() |> Enum.reduce(acc, &[&1 | &2])

    Enum.reduce(node, acc, fn {_key, value}, inner -> collect(value, fun, inner) end)
  end

  defp collect(node, fun, acc) when is_list(node),
    do: Enum.reduce(node, acc, &collect(&1, fun, &2))

  defp collect(_leaf, _fun, acc), do: acc
end
