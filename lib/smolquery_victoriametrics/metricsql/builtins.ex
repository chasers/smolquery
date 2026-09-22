defmodule SmolqueryVictoriaMetrics.MetricsQL.Builtins do
  @moduledoc """
  `metricsql` v0.87.4 has no `alias` function: `alias`, `range_median`,
  `ru` and `ttf` are built-in `WITH` templates (`getDefaultWithArgExprs` in
  `parser.go`), and the parser here expands them where it meets them, as
  that one does (PL-70, T-565), since `WITH` itself is not supported:

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
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
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
