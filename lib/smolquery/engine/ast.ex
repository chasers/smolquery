defmodule Smolquery.Engine.Ast do
  @moduledoc """
  A walk over the JSON DuckDB's `json_serialize_sql` hands back.

  The tree is maps and lists all the way down, with no schema beyond each
  node's `"class"` and `"type"`; a caller that wants every node of one kind
  visits all of them and keeps what it recognises. Two readers do that: the
  planner, for table references and table functions, and the materialized
  column gates, for column references and node classes.

  The node shapes moved with DuckDB 2.0's parser, and the readers here take
  each spelling seen so far. A CAST's target was a resolved `cast_type` with
  an `id` (1.5); the pinned 2.0 preview leaves it unbound, a `TYPE` node
  under `cast_type.type_info.expr` carrying the name as written, and the
  later 2.0 builds (alpha41396) put that node under `type_expr`. A
  FUNCTION's `children` are `arguments` on 2.0, each a name and an
  expression. A CONSTANT is a typed `value` with `is_null` on 1.5 and the
  pinned build, and a `literal` of `kind` and `text` on the later builds.
  `test/smolquery/engine/ast_test.exs` runs the readers against the pinned
  parser as well as the fixtures, so a pin that moves to a spelling they do
  not read fails there rather than silently reading nothing.
  """

  @type constant :: {String.t(), term()} | :null

  @type_aliases %{
    "TIMESTAMPTZ" => "TIMESTAMP WITH TIME ZONE",
    "TIMETZ" => "TIME WITH TIME ZONE",
    "DATETIME" => "TIMESTAMP"
  }

  @integer_types ~w(TINYINT SMALLINT INTEGER BIGINT HUGEINT UTINYINT USMALLINT UINTEGER UBIGINT UHUGEINT)

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

  @doc """
  The name a TYPE node carries, upper-cased and with DuckDB's aliases for the
  zoned and datetime types expanded (`TIMESTAMPTZ` reads as
  `TIMESTAMP WITH TIME ZONE`, as 1.5 resolved it), or `nil` for any other node.

  A composite target (`STRUCT(a TIMESTAMPTZ)`, `TIMESTAMPTZ[]`) is a TYPE node
  with TYPE children, so a reader that wants every type named in a cast
  collects them all rather than reading the cast's top alone.
  """
  @spec type_name(term()) :: String.t() | nil
  def type_name(%{"class" => "TYPE", "type_name" => name}) when is_binary(name),
    do: canonical_type(name)

  def type_name(_node), do: nil

  @doc """
  The target type of a CAST node, as `type_name/1` spells it, or `nil` for a
  node that is not a cast or whose target cannot be read.
  """
  @spec cast_type(term()) :: String.t() | nil
  def cast_type(%{"type_expr" => %{"class" => "TYPE"} = type}), do: type_name(type)

  def cast_type(%{"cast_type" => %{"id" => "UNBOUND", "type_info" => %{"expr" => expr}}}),
    do: type_name(expr)

  def cast_type(%{"cast_type" => %{"id" => id}}) when is_binary(id) and id != "UNBOUND",
    do: canonical_type(id)

  def cast_type(_node), do: nil

  defp canonical_type(name) do
    upper = String.upcase(name)
    Map.get(@type_aliases, upper, upper)
  end

  @doc """
  A CONSTANT node's value as `{:ok, {type, value}}` — the type DuckDB gave a
  resolved constant, or the one its literal kind implies (`INTEGER` text is a
  `BIGINT`, `NUMERIC` a `DECIMAL`, `STRING` a `VARCHAR`) — `{:ok, :null}` for
  a NULL, and `:error` for any other node.

  A resolved DECIMAL's value is DuckDB's unscaled integer (`1.5` at scale 1 is
  `15`), so a reader that needs a whole number asks `integer/1`.
  """
  @spec constant(term()) :: {:ok, constant()} | :error
  def constant(%{"class" => "CONSTANT", "value" => %{"is_null" => true}}), do: {:ok, :null}

  def constant(%{
        "class" => "CONSTANT",
        "value" => %{"type" => %{"id" => type}, "value" => value}
      }),
      do: {:ok, {type, value}}

  def constant(%{"class" => "CONSTANT", "literal" => %{"kind" => kind, "text" => text}}),
    do: literal(kind, text)

  def constant(_node), do: :error

  defp literal("NULL_LITERAL", _text), do: {:ok, :null}
  defp literal("STRING", text), do: {:ok, {"VARCHAR", text}}
  defp literal("BOOLEAN", text), do: {:ok, {"BOOLEAN", String.downcase(text) == "true"}}

  defp literal("INTEGER", text) do
    case Integer.parse(text) do
      {value, ""} -> {:ok, {"BIGINT", value}}
      _not_an_integer -> :error
    end
  end

  defp literal("NUMERIC", text) do
    case Float.parse(text) do
      {value, ""} -> {:ok, {"DECIMAL", value}}
      _not_a_number -> :error
    end
  end

  defp literal(_kind, _text), do: :error

  @doc """
  A CONSTANT node's value as `{:ok, integer}` when its type is one of DuckDB's
  integer types, and `:error` otherwise — a DECIMAL `1.5` is not `15`, and a
  string is not a number however it reads.
  """
  @spec integer(term()) :: {:ok, integer()} | :error
  def integer(node) do
    case constant(node) do
      {:ok, {type, value}} when type in @integer_types and is_integer(value) -> {:ok, value}
      _not_a_whole_number -> :error
    end
  end

  @doc """
  The argument expressions of a FUNCTION node, in call order, under either
  spelling; `[]` for a node without any.
  """
  @spec arguments(map()) :: [term()]
  def arguments(%{"arguments" => arguments}) when is_list(arguments),
    do: Enum.map(arguments, & &1["expression"])

  def arguments(%{"children" => children}) when is_list(children), do: children
  def arguments(_node), do: []
end
