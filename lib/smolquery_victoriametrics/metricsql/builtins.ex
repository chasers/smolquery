defmodule SmolqueryVictoriaMetrics.MetricsQL.Builtins do
  @moduledoc """
  `metricsql` v0.87.4 has no `alias` function: `alias`, `range_median`,
  `ru` and `ttf` are built-in `WITH` templates (`getDefaultWithArgExprs` in
  `parser.go`), and the parser here expands them once the whole expression
  is parsed, as that one does (PL-70, T-565), since `WITH` itself is not
  supported:

      alias(q, name)      = label_set(q, "__name__", name)
      range_median(q)     = range_quantile(0.5, q)
      ru(freev, maxv)     = clamp_min(maxv - clamp_min(freev, 0), 0) / clamp_min(maxv, 0) * 100
      ttf(freev)          = smooth_exponential(
                              clamp_max(clamp_max(-freev, 0) / clamp_max(deriv_fast(freev), 0), 365*24*3600),
                              clamp_max(step()/300, 1))

  A call is expanded only when its name is spelled exactly so, as template
  names are matched in `metricsql`; `alias` with no parentheses is still a
  metric name. The expansion is what the tree holds and what
  `SmolqueryVictoriaMetrics.MetricsQL.to_string/1` prints.

  Expanding after parsing makes a template one operand: `ru(a, b) ^ 2`
  raises the whole template to the power, where expanding while parsing let
  the `^` bind to the template's last `*` operand. A `keep_metric_names`
  after a template call is dropped, as `metricsql` drops it for all four.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral

  @arity %{"alias" => 2, "range_median" => 1, "ru" => 2, "ttf" => 1}

  @doc """
  Expands the call `name(args)` when `name` is a built-in template:
  `{:ok, expr}`, `{:error, {:arity, message}}` for a wrong count, or `:none`
  for any other name.
  """
  @spec expand(String.t(), [SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()]) ::
          {:ok, SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()}
          | {:error, {:arity, String.t()}}
          | :none
  def expand(name, args) do
    case Map.fetch(@arity, name) do
      {:ok, arity} when length(args) == arity ->
        {:ok, template(name, args)}

      {:ok, arity} ->
        {:error,
         {:arity, "unexpected number of args for #{name}(); got #{length(args)}; want #{arity}"}}

      :error ->
        :none
    end
  end

  @doc """
  Expands every built-in template call in `expr`, innermost first:
  `{:ok, expr}`, or the first `{:error, {:arity, message}}`.
  """
  @spec expand_all(Ast.expr()) :: {:ok, Ast.expr()} | {:error, {:arity, String.t()}}
  def expand_all(%FuncExpr{name: name} = call) do
    with {:ok, args} <- expand_list(call.args) do
      case expand(name, args) do
        :none -> {:ok, %{call | args: args}}
        expanded -> expanded
      end
    end
  end

  def expand_all(%AggrFuncExpr{} = node) do
    with {:ok, args} <- expand_list(node.args), do: {:ok, %{node | args: args}}
  end

  def expand_all(%ParensExpr{exprs: exprs} = node) do
    with {:ok, exprs} <- expand_list(exprs), do: {:ok, %{node | exprs: exprs}}
  end

  def expand_all(%BinaryOpExpr{} = node) do
    with {:ok, left} <- expand_all(node.left),
         {:ok, right} <- expand_all(node.right),
         do: {:ok, %{node | left: left, right: right}}
  end

  def expand_all(%RollupExpr{} = node) do
    with {:ok, expr} <- expand_all(node.expr),
         {:ok, at} <- expand_at(node.at),
         do: {:ok, %{node | expr: expr, at: at}}
  end

  def expand_all(leaf), do: {:ok, leaf}

  defp expand_at(nil), do: {:ok, nil}
  defp expand_at(at), do: expand_all(at)

  defp expand_list(exprs) do
    exprs
    |> Enum.reduce_while({:ok, []}, fn expr, {:ok, acc} ->
      case expand_all(expr) do
        {:ok, expanded} -> {:cont, {:ok, [expanded | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp template("alias", [q, name]),
    do: call("label_set", [q, %StringLiteral{value: "__name__"}, name])

  defp template("range_median", [q]), do: call("range_quantile", [number(0.5, "0.5"), q])

  defp template("ru", [free, max]) do
    used = call("clamp_min", [op(:-, max, call("clamp_min", [free, zero()])), zero()])
    op(:*, op(:/, used, call("clamp_min", [max, zero()])), number(100.0, "100"))
  end

  defp template("ttf", [free]) do
    negated = op(:-, zero(), free)

    remaining =
      op(
        :/,
        call("clamp_max", [negated, zero()]),
        call("clamp_max", [call("deriv_fast", [free]), zero()])
      )

    year = op(:*, op(:*, number(365.0, "365"), number(24.0, "24")), number(3600.0, "3600"))
    factor = call("clamp_max", [op(:/, call("step", []), number(300.0, "300")), number(1.0, "1")])
    call("smooth_exponential", [call("clamp_max", [remaining, year]), factor])
  end

  defp call(name, args), do: %FuncExpr{name: name, args: args}
  defp op(op, left, right), do: %BinaryOpExpr{op: op, left: left, right: right}
  defp number(value, text), do: %Number{value: value, text: text}
  defp zero, do: number(0.0, "0")
end
