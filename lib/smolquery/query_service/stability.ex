defmodule Smolquery.QueryService.Stability do
  @moduledoc """
  Which functions a statement names, and what the engine's catalog says of
  how their answers hold still.

  Two readers run part of a user's statement ahead of the statement itself:
  `Smolquery.QueryService.TopN` probes its WHERE for a bound, and
  `Smolquery.QueryService.Fold` asks for the value of a bound written as an
  expression. Either is sound only if the part answers the same both times,
  so both ask `duckdb_functions()` about every function named, and both
  trust the macros `Smolquery.QueryService.ClickHouseFunctions` defines by
  bare name, since a macro has no stability the catalog can report. The rule
  is here once so that tightening it tightens both.

  `CONSISTENT` and `CONSISTENT_WITHIN_QUERY` are stable. The second reads
  something that moves between statements, the clock above all, and what a
  reader may do with such an answer is the reader's to say.
  """

  alias Smolquery.Engine.Ast
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions

  @stable ["CONSISTENT", "CONSISTENT_WITHIN_QUERY"]

  @doc """
  Every function `tree` names, lower-cased and once each.
  """
  @spec function_names(term()) :: [String.t()]
  def function_names(tree) do
    tree
    |> Ast.collect(fn
      %{"class" => "FUNCTION", "function_name" => name} when is_binary(name) -> [name]
      _another_node -> []
    end)
    |> Enum.map(&String.downcase/1)
    |> Enum.uniq()
  end

  @doc """
  The names of `names` the catalog is asked about: all but the ClickHouse
  macros, which are trusted by name.
  """
  @spec checked([String.t()]) :: [String.t()]
  def checked(names), do: Enum.reject(names, &ClickHouseFunctions.stable?/1)

  @doc """
  A scalar SQL expression: how many of `names` the catalog does not call
  stable. `names` are `checked/1`'s.
  """
  @spec unstable_count_sql([String.t()]) :: String.t()
  def unstable_count_sql([]), do: "0"

  def unstable_count_sql(names) do
    "(SELECT count(*) FROM duckdb_functions() WHERE #{named(names)} AND #{unstable()})"
  end

  @doc """
  A scalar SQL expression: the list of `names` the catalog does not call
  stable.
  """
  @spec unstable_names_sql([String.t()]) :: String.t()
  def unstable_names_sql(names), do: names_sql(names, unstable())

  @doc """
  A scalar SQL expression: the list of `names` that are stable within a
  query only.
  """
  @spec within_query_names_sql([String.t()]) :: String.t()
  def within_query_names_sql(names),
    do: names_sql(names, "stability = 'CONSISTENT_WITHIN_QUERY'")

  @doc """
  A scalar SQL expression: the list of `names` the catalog has as anything
  but a scalar function, an aggregate above all. `names` are `checked/1`'s:
  which ClickHouse macro aggregates is
  `Smolquery.QueryService.ClickHouseFunctions.aggregate?/1`'s to say.
  """
  @spec not_scalar_names_sql([String.t()]) :: String.t()
  def not_scalar_names_sql(names), do: names_sql(names, "function_type != 'scalar'")

  defp names_sql([], _condition), do: "CAST([] AS VARCHAR[])"

  defp names_sql(names, condition) do
    "(SELECT coalesce(list(DISTINCT lower(function_name)), CAST([] AS VARCHAR[])) " <>
      "FROM duckdb_functions() WHERE #{named(names)} AND #{condition})"
  end

  defp named(names),
    do: "lower(function_name) IN (#{Enum.map_join(names, ", ", &Identifier.sql_string/1)})"

  defp unstable,
    do:
      "coalesce(stability, '') NOT IN (#{Enum.map_join(@stable, ", ", &Identifier.sql_string/1)})"
end
