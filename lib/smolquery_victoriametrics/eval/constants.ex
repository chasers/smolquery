defmodule SmolqueryVictoriaMetrics.Eval.Constants do
  @moduledoc """
  What VictoriaMetrics v1.152.0 does to a parsed expression before it
  evaluates it (PL-70, T-565), and what it reads off the tree's shape.

    * `prepare/1` folds constants as `metricsql`'s `simplifyConstants`
      does while parsing (`1 + 2` is `3`, `-1` is `-1`, `"a" < "b"` is `1`),
      which the parser here leaves to the evaluator, then turns
      `num cmp q` around as `adjustCmpOps` does (`0.5 < foo` is
      `foo > 0.5`), so a comparison keeps the series' values.
    * `scalar?/1` is `isScalarLikeExpr`: an expression that always
      evaluates to a scalar. An instant query of one answers
      `resultType: "scalar"`.
    * `may_sort?/1` is `maySortResults`: whether the answer is sorted by
      labels, which it is not after `sort`, `topk` and the like, whose order
      is the answer, nor after `or`.
  """

  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral

  @comparisons [:==, :!=, :>, :<, :>=, :<=]
  @reversed %{>: :<, <: :>, >=: :<=, <=: :>=}
  @scalar_functions ~w(now pi scalar start end step time timezone_offset)
  @sorted_functions ~w(sort sort_desc limit_offset sort_by_label sort_by_label_desc
    sort_by_label_numeric sort_by_label_numeric_desc)
  @sorted_aggregates ~w(topk bottomk outliersk topk_max topk_min topk_avg topk_median topk_last
    bottomk_max bottomk_min bottomk_avg bottomk_median bottomk_last)

  @doc "Folds constants, then turns `num cmp q` around."
  @spec prepare(Ast.expr()) :: Ast.expr()
  def prepare(expr), do: expr |> fold() |> adjust()

  @doc "Whether `expr` is a comparison operator."
  @spec comparison?(atom()) :: boolean()
  def comparison?(op), do: op in @comparisons

  @doc "Whether `expr`, prepared, always evaluates to a scalar."
  @spec scalar?(Ast.expr()) :: boolean()
  def scalar?(%Number{}), do: true
  def scalar?(%Duration{}), do: true
  def scalar?(%BinaryOpExpr{left: left, right: right}), do: scalar?(left) and scalar?(right)
  def scalar?(%FuncExpr{name: name}), do: String.downcase(name) in @scalar_functions
  def scalar?(_expr), do: false

  @doc "Whether the answer to `expr` is sorted by labels."
  @spec may_sort?(Ast.expr()) :: boolean()
  def may_sort?(%FuncExpr{name: name}), do: String.downcase(name) not in @sorted_functions
  def may_sort?(%AggrFuncExpr{name: name}), do: name not in @sorted_aggregates
  def may_sort?(%BinaryOpExpr{op: :or}), do: false
  def may_sort?(_expr), do: true

  @doc "Whether `expr` is a union: `(a, b)` or `union(a, b)` (`isUnionFunc`)."
  @spec union?(Ast.expr()) :: boolean()
  def union?(%ParensExpr{}), do: true
  def union?(%FuncExpr{name: name}), do: String.downcase(name) == "union"
  def union?(_expr), do: false

  defp fold(%RollupExpr{at: nil} = node), do: %{node | expr: fold(node.expr)}
  defp fold(%RollupExpr{} = node), do: %{node | expr: fold(node.expr), at: fold(node.at)}
  defp fold(%AggrFuncExpr{args: args} = node), do: %{node | args: Enum.map(args, &fold/1)}
  defp fold(%FuncExpr{args: args} = node), do: %{node | args: Enum.map(args, &fold/1)}
  defp fold(%ParensExpr{exprs: exprs} = node), do: %{node | exprs: Enum.map(exprs, &fold/1)}

  defp fold(%BinaryOpExpr{} = node) do
    node = %{node | left: fold(node.left), right: fold(node.right)}

    case {node.left, node.right} do
      {%Number{value: a}, %Number{value: b}} -> number(numbers(node, a, b))
      {%StringLiteral{value: a}, %StringLiteral{value: b}} -> strings(node, a, b)
      _other -> node
    end
  end

  defp fold(leaf), do: leaf

  defp numbers(%BinaryOpExpr{op: op, bool: bool}, a, b) do
    a = Value.from_number(a)
    b = Value.from_number(b)

    if op in @comparisons do
      compared = compare(op, a, b)

      cond do
        bool and compared -> 1.0
        bool -> 0.0
        compared -> a
        true -> nil
      end
    else
      arithmetic(op, a, b)
    end
  end

  defp strings(%BinaryOpExpr{op: op, bool: bool}, a, b) when op in @comparisons do
    cond do
      string_compare(op, a, b) -> number(1.0)
      bool -> number(0.0)
      true -> number(nil)
    end
  end

  defp strings(node, _a, _b), do: node

  defp string_compare(:==, a, b), do: a == b
  defp string_compare(:!=, a, b), do: a != b
  defp string_compare(:>, a, b), do: a > b
  defp string_compare(:<, a, b), do: a < b
  defp string_compare(:>=, a, b), do: a >= b
  defp string_compare(:<=, a, b), do: a <= b

  @doc "Compares two values with a comparison operator, as `metricsql/binaryop` does."
  @spec compare(atom(), Value.t(), Value.t()) :: boolean()
  def compare(:==, a, b), do: Value.eq?(a, b)
  def compare(:!=, a, b), do: Value.neq?(a, b)
  def compare(:>, a, b), do: Value.gt?(a, b)
  def compare(:<, a, b), do: Value.lt?(a, b)
  def compare(:>=, a, b), do: Value.gte?(a, b)
  def compare(:<=, a, b), do: Value.lte?(a, b)

  @doc "Applies an arithmetic operator to two values."
  @spec arithmetic(atom(), Value.t(), Value.t()) :: Value.t()
  def arithmetic(:+, a, b), do: Value.add(a, b)
  def arithmetic(:-, a, b), do: Value.sub(a, b)
  def arithmetic(:*, a, b), do: Value.mul(a, b)
  def arithmetic(:/, a, b), do: Value.divide(a, b)
  def arithmetic(:%, a, b), do: Value.mod(a, b)
  def arithmetic(:^, a, b), do: Value.pow(a, b)
  def arithmetic(:atan2, a, b), do: Value.atan2(a, b)
  def arithmetic(:and, a, b), do: if(is_nil(a) or is_nil(b), do: nil, else: a)
  def arithmetic(:or, a, b), do: if(is_nil(a), do: b, else: a)
  def arithmetic(:unless, _a, _b), do: nil
  def arithmetic(:default, a, b), do: if(is_nil(a), do: b, else: a)
  def arithmetic(:if, a, b), do: if(is_nil(b), do: nil, else: a)
  def arithmetic(:ifnot, a, b), do: if(is_nil(b), do: a, else: nil)

  defp number(nil), do: %Number{value: :nan, text: "NaN"}
  defp number(value), do: %Number{value: value, text: Float.to_string(value)}

  defp adjust(%RollupExpr{at: nil} = node), do: %{node | expr: adjust(node.expr)}
  defp adjust(%RollupExpr{} = node), do: %{node | expr: adjust(node.expr), at: adjust(node.at)}
  defp adjust(%AggrFuncExpr{args: args} = node), do: %{node | args: Enum.map(args, &adjust/1)}
  defp adjust(%FuncExpr{args: args} = node), do: %{node | args: Enum.map(args, &adjust/1)}
  defp adjust(%ParensExpr{exprs: exprs} = node), do: %{node | exprs: Enum.map(exprs, &adjust/1)}

  defp adjust(%BinaryOpExpr{op: op} = node) do
    node = %{node | left: adjust(node.left), right: adjust(node.right)}

    if op in @comparisons and not match?(%Number{}, node.right) and time_or_number?(node.left),
      do: %{node | left: node.right, right: node.left, op: Map.get(@reversed, op, op)},
      else: node
  end

  defp adjust(leaf), do: leaf

  defp time_or_number?(%Number{}), do: true
  defp time_or_number?(%FuncExpr{name: name}), do: String.downcase(name) == "time"
  defp time_or_number?(_expr), do: false
end
