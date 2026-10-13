defmodule Smolquery.Engine.Ast do
  @moduledoc """
  A walk over the JSON DuckDB's `json_serialize_sql` hands back.

  The tree is maps and lists all the way down, with no schema beyond each
  node's `"class"` and `"type"`; a caller that wants every node of one kind
  visits all of them and keeps what it recognises. Two readers do that: the
  planner, for table references and table functions, and the materialized
  column gates, for column references and node classes.

  The node shapes moved with DuckDB 2.0's parser, and the readers here take
  each spelling seen so far: 1.5.3, and the 2.0 previews alpha38195 and
  alpha46516 (2026-10-12).

    * A FUNCTION's `children` are `arguments` on 2.0, each a name and an
      expression, beside a `qualified_name` path.
    * A CAST's target was a resolved `cast_type` with an `id` (1.5);
      alpha38195 leaves it unbound, a `TYPE` node under
      `cast_type.type_info.expr`, and later builds put that node under
      `type_expr`.
    * A CONSTANT is a typed `value` with `is_null` on 1.5 and alpha38195,
      and a `literal` of `kind` and `text` from alpha41396.
    * A set operation's `left` and `right` are a `children` list on 2.0.
    * A CTE's statement is `query.node` on 1.5 and `query_node` on 2.0.
    * A percent LIMIT is its own `LIMIT_PERCENT_MODIFIER` on 1.5, and a
      `LIMIT_MODIFIER` with `limit_type` `PERCENTAGE` on 2.0.
    * Every node carries a `query_location_length` on 2.0.

  2.0 still renders the 1.5 spellings (`children`, a typed `value`) through
  `json_deserialize_sql`, so a node built here is written that way and
  renders on both. On 2.0, a FUNCTION's `qualified_name` wins over its
  `function_name` (and a BASE_TABLE's over its `table_name`), so `portable/1`
  drops it.
  `test/smolquery/engine/ast_test.exs` runs the readers against the driver's
  own parser as well as the fixtures, so a pin that moves to a spelling they
  do not read fails there, rather than silently reading nothing.
  """

  @type constant :: {String.t(), term()} | :null

  @type_aliases %{
    "TIMESTAMPTZ" => "TIMESTAMP WITH TIME ZONE",
    "TIMETZ" => "TIME WITH TIME ZONE",
    "DATETIME" => "TIMESTAMP",
    "INT" => "INTEGER",
    "INT4" => "INTEGER",
    "SIGNED" => "INTEGER",
    "INT8" => "BIGINT",
    "LONG" => "BIGINT",
    "INT2" => "SMALLINT",
    "SHORT" => "SMALLINT",
    "INT1" => "TINYINT",
    "FLOAT8" => "DOUBLE",
    "DOUBLE PRECISION" => "DOUBLE",
    "FLOAT4" => "FLOAT",
    "REAL" => "FLOAT",
    "TEXT" => "VARCHAR",
    "STRING" => "VARCHAR",
    "CHAR" => "VARCHAR",
    "BPCHAR" => "VARCHAR",
    "DEC" => "DECIMAL",
    "NUMERIC" => "DECIMAL",
    "BOOL" => "BOOLEAN",
    "LOGICAL" => "BOOLEAN"
  }

  @int64 -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  @integer_types ~w(TINYINT SMALLINT INTEGER BIGINT HUGEINT UTINYINT USMALLINT UINTEGER UBIGINT UHUGEINT)

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
  `query_location`, `query_location_length` and `alias` taken off, all the
  way down.

  Two expressions with the same shape are the same expression. It is how a
  select item is matched to a group key or an ORDER BY, and how a bound is
  matched to the value the engine folded it to.
  """
  @spec shape(term()) :: term()
  def shape(node) when is_map(node) do
    node
    |> Map.drop(["query_location", "query_location_length", "alias"])
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

  @doc """
  The name a TYPE node carries, upper-cased and with DuckDB's documented
  aliases expanded to the names 1.5 resolved them to (`TIMESTAMPTZ` reads as
  `TIMESTAMP WITH TIME ZONE`, `INT8` as `BIGINT`, `TEXT` as `VARCHAR`), or
  `nil` for any other node. 2.0 resolves some aliases itself and leaves
  others as written.

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

  @doc """
  Every type a CAST node names, as `type_name/1` spells them: its target and,
  for a composite one, the types inside it (`STRUCT(a TIMESTAMPTZ)` names
  `STRUCT` and `TIMESTAMP WITH TIME ZONE`). A 1.5 cast resolves them all
  under `cast_type`; a 2.0 cast keeps them as TYPE nodes, which a reader
  walking every node meets on its own, so only the target is answered here.
  """
  @spec cast_types(term()) :: [String.t()]
  def cast_types(%{"cast_type" => %{"id" => id} = type}) when is_binary(id) and id != "UNBOUND",
    do: collect(type, &resolved_type/1)

  def cast_types(node), do: node |> cast_type() |> List.wrap()

  defp resolved_type(%{"id" => id}) when is_binary(id), do: [canonical_type(id)]
  defp resolved_type(_type_info), do: []

  defp canonical_type(name) do
    upper = String.upcase(name)
    Map.get(@type_aliases, upper, upper)
  end

  @doc """
  A CONSTANT node's value as `{:ok, {type, value}}` — the type DuckDB gave a
  resolved constant, or the one its literal kind implies — `{:ok, :null}` for
  a NULL, and `:error` for any other node.

  A literal reads as 1.5 resolves the same text: `INTEGER` text is a
  `BIGINT` (a `HUGEINT` past 64 bits), `STRING` a `VARCHAR`, and `NUMERIC`
  text a `DOUBLE` when it carries an exponent (`1e5`) and a `DECIMAL`
  otherwise. A DECIMAL's value is the unscaled integer either way (`1.5` is
  `15`, `.5` is `5`), so a reader that needs a whole number asks
  `integer/1`.
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
      {value, ""} when value in @int64 -> {:ok, {"BIGINT", value}}
      {value, ""} -> {:ok, {"HUGEINT", value}}
      _not_an_integer -> :error
    end
  end

  defp literal("NUMERIC", text) do
    if String.contains?(text, ["e", "E"]), do: double(text), else: decimal(text)
  end

  defp literal(_kind, _text), do: :error

  defp double(text) do
    normalized =
      text
      |> String.replace(~r/\A(-?)\./, "\\g{1}0.")
      |> String.replace(~r/\.(?=[eE])/, ".0")

    case Float.parse(normalized) do
      {value, ""} -> {:ok, {"DOUBLE", value}}
      _not_a_number -> :error
    end
  end

  defp decimal(text) do
    case Integer.parse(String.replace(text, ".", "", global: false)) do
      {unscaled, ""} -> {:ok, {"DECIMAL", unscaled}}
      _not_a_number -> :error
    end
  end

  @doc """
  A CONSTANT node's value as `{:ok, integer}` when its type is one of DuckDB's
  integer types and it fits 64 bits, and `:error` otherwise — a DECIMAL
  `1.5` is not `15`, a string is not a number however it reads, and a
  HUGEINT is no LIMIT DuckDB takes.
  """
  @spec integer(term()) :: {:ok, integer()} | :error
  def integer(node) do
    case constant(node) do
      {:ok, {type, value}} when type in @integer_types and value in @int64 -> {:ok, value}
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

  @doc """
  `tree` in the spelling 1.5 parses to, which 2.0 still renders: every
  FUNCTION's arguments written as `children`, and no FUNCTION or BASE_TABLE
  carrying a `qualified_name`. A reader written against 1.5 then reads a 2.0
  tree, and a node built by renaming a function or a table renders by its
  new name: on 2.0 the `qualified_name` path wins over `function_name` and
  `table_name` alike.
  """
  @spec portable(term()) :: term()
  def portable(%{"class" => "FUNCTION"} = node) do
    node
    |> Map.drop(["arguments", "qualified_name"])
    |> Map.new(fn {key, value} -> {key, portable(value)} end)
    |> Map.put("children", node |> arguments() |> Enum.map(&portable/1))
  end

  def portable(%{"type" => "BASE_TABLE"} = node),
    do:
      node
      |> Map.delete("qualified_name")
      |> Map.new(fn {key, value} -> {key, portable(value)} end)

  def portable(node) when is_map(node),
    do: Map.new(node, fn {key, value} -> {key, portable(value)} end)

  def portable(node) when is_list(node), do: Enum.map(node, &portable/1)
  def portable(leaf), do: leaf

  @doc """
  The statement a CTE entry's value holds, under either spelling, or `nil`.
  """
  @spec cte_node(term()) :: map() | nil
  def cte_node(%{"query_node" => %{} = node}), do: node
  def cte_node(%{"query" => %{"node" => %{} = node}}), do: node
  def cte_node(_value), do: nil

  @doc """
  The operands of a SET_OPERATION_NODE, in order, under either spelling.
  """
  @spec set_operands(map()) :: [map()]
  def set_operands(%{"children" => [_ | _] = children}), do: children

  def set_operands(%{"left" => left, "right" => right}),
    do: Enum.reject([left, right], &is_nil/1)

  def set_operands(_node), do: []
end
