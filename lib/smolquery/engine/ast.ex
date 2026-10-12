defmodule Smolquery.Engine.Ast do
  @moduledoc """
  A walk over the JSON DuckDB's `json_serialize_sql` hands back.

  The tree is maps and lists all the way down, with no schema beyond each
  node's `"class"` and `"type"`; a caller that wants every node of one kind
  visits all of them and keeps what it recognises. Two readers do that: the
  planner, for table references and table functions, and the materialized
  column gates, for column references and node classes.
  """

  @unreadable "DuckDB's parse of this statement is not valid JSON; a number past ~3.4e38 " <>
                "written as a literal is one cause, and CAST('1e308' AS DOUBLE) avoids it"

  @doc """
  The tree `json_serialize_sql` answered, decoded, or
  `{:error, {:invalid_query, message}}` when it is not JSON, the message
  naming the byte where it breaks.

  DuckDB 2.0 writes a DOUBLE literal past ~3.4e38 as a bare `Infinity`,
  which no JSON reader takes. Decoding leniently would read the literal as
  an infinity the statement never wrote, so the statement is refused, by
  name (T-653).
  """
  @spec decode(String.t()) :: {:ok, term()} | {:error, {:invalid_query, String.t()}}
  def decode(json) do
    case JSON.decode(json) do
      {:ok, tree} -> {:ok, tree}
      {:error, reason} -> {:error, {:invalid_query, @unreadable <> at(reason)}}
    end
  end

  defp at({_kind, offset}), do: breaks_at(offset)
  defp at({_kind, offset, _found}), do: breaks_at(offset)

  defp breaks_at(offset), do: " (the JSON breaks at byte #{offset})"

  @doc """
  `node` without where it was written or what it was called: its
  `query_location` and `alias` taken off, all the way down.

  Two expressions with the same shape are the same expression. It is how a
  select item is matched to a group key or an ORDER BY, and how a bound is
  matched to the value the engine folded it to.
  """
  @spec shape(term()) :: term()
  def shape(node) when is_map(node) do
    node
    |> Map.drop(["query_location", "alias"])
    |> Map.new(fn {key, value} -> {key, shape(value)} end)
  end

  def shape(node) when is_list(node), do: Enum.map(node, &shape/1)
  def shape(leaf), do: leaf

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
