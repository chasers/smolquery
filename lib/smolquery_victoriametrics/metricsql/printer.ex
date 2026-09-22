defmodule SmolqueryVictoriaMetrics.MetricsQL.Printer do
  @moduledoc """
  Prints a `SmolqueryVictoriaMetrics.MetricsQL.Ast` tree as canonical
  MetricsQL, the way `AppendString` in VictoriaMetrics' `metricsql` v0.87.4
  writes it (PL-70, T-563): `sum(x) by(a,b) limit 10`, `a + on(x) group_left() b`,
  `m{a="1"}[5m:1m] offset 1h @ 12`, every nested binary operation in
  parentheses, strings double-quoted with Go's escapes, identifiers escaped.
  Numbers and durations print as they were typed.

  Parsing the printed text gives back the same tree. Where VictoriaMetrics'
  own printing would not, this one differs, and only there:

    * a selector whose `or` branches share a metric name while one of them is
      nothing but that name prints every name, `{__name__="a",b="1" or
      __name__="a"}`, where VictoriaMetrics prints `a{b="1"}` and loses the
      second branch;
    * a metric named `inf` or `nan` prints as `{__name__="inf"}`, which does
      not read back as a number;
    * a string on the left of `+` is parenthesized when what follows would
      read as more of the string, `("a") + "b"` and `("a") + b`, since
      `"a" + "b"` reads back as the one string `"ab"`.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Literal

  @filter_ops %{eq: "=", neq: "!=", re: "=~", nre: "!~"}
  @reserved ~w(on ignoring group_left group_right bool prefix fill fill_left fill_right)

  @doc """
  The canonical MetricsQL text of `expr`.
  """
  @spec to_string(SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()) :: String.t()
  def to_string(expr), do: expr |> print() |> IO.iodata_to_binary()

  defp print(%Number{text: text}), do: text
  defp print(%StringLiteral{value: value}), do: Literal.quote_string(value)
  defp print(%Duration{text: text}), do: text
  defp print(%MetricExpr{filter_sets: sets}), do: selector(sets)
  defp print(%ParensExpr{exprs: exprs}), do: arg_list(exprs)

  defp print(%FuncExpr{name: name, args: args, keep_metric_names: keep}),
    do: [Literal.escape_ident(name), arg_list(args), keep(keep)]

  defp print(%AggrFuncExpr{} = node) do
    [
      Literal.escape_ident(node.name),
      arg_list(node.args),
      modifier(node.modifier),
      if(node.limit, do: [" limit ", Integer.to_string(node.limit)], else: [])
    ]
  end

  defp print(%RollupExpr{} = node) do
    [
      parenthesized(node.expr, rollup_parens?(node.expr)),
      brackets(node),
      if(node.offset, do: [" offset ", node.offset.text], else: []),
      at(node.at)
    ]
  end

  defp print(%BinaryOpExpr{keep_metric_names: true} = node),
    do: ["(", operation(node), ") keep_metric_names"]

  defp print(%BinaryOpExpr{} = node), do: operation(node)

  defp arg_list(args), do: ["(", Enum.map_intersperse(args, ", ", &print/1), ")"]

  defp keep(true), do: " keep_metric_names"
  defp keep(false), do: []

  defp modifier(nil), do: []

  defp modifier(%Modifier{op: op, labels: labels}),
    do: [" ", Atom.to_string(op), label_list(labels)]

  defp label_list(:all), do: "(*)"

  defp label_list(labels),
    do: ["(", Enum.map_intersperse(labels, ",", &Literal.escape_ident/1), ")"]

  defp parenthesized(expr, true), do: ["(", print(expr), ")"]
  defp parenthesized(expr, false), do: print(expr)

  defp rollup_parens?(%RollupExpr{}), do: true
  defp rollup_parens?(%BinaryOpExpr{}), do: true
  defp rollup_parens?(%AggrFuncExpr{modifier: modifier}), do: modifier != nil
  defp rollup_parens?(_expr), do: false

  defp brackets(%RollupExpr{window: nil, step: nil, inherit_step: false}), do: []

  defp brackets(%RollupExpr{window: window, step: step, inherit_step: inherit_step}) do
    step_part =
      cond do
        step -> [":", step.text]
        inherit_step -> ":"
        true -> []
      end

    ["[", if(window, do: window.text, else: []), step_part, "]"]
  end

  defp at(nil), do: []
  defp at(%BinaryOpExpr{} = at), do: [" @ (", print(at), ")"]
  defp at(%RollupExpr{} = at), do: [" @ (", print(at), ")"]
  defp at(at), do: [" @ ", print(at)]

  defp operation(%BinaryOpExpr{} = node) do
    [
      parenthesized(node.left, left_parens?(node)),
      " ",
      Atom.to_string(node.op),
      if(node.bool, do: "bool", else: []),
      modifier(node.group_modifier),
      modifier(node.join_modifier),
      if(node.join_prefix, do: [" prefix ", Literal.quote_string(node.join_prefix)], else: []),
      fills(node.fill_left, node.fill_right),
      " ",
      parenthesized(node.right, right_parens?(node))
    ]
  end

  defp fills(nil, nil), do: []
  defp fills(same, same), do: [" fill(", same.text, ")"]

  defp fills(left, right) do
    [
      if(left, do: [" fill_left(", left.text, ")"], else: []),
      if(right, do: [" fill_right(", right.text, ")"], else: [])
    ]
  end

  defp left_parens?(%BinaryOpExpr{op: :+, left: %StringLiteral{}, right: right}),
    do: continues_string?(right)

  defp left_parens?(%BinaryOpExpr{left: left}), do: operand_parens?(left)

  defp continues_string?(%StringLiteral{}), do: true
  defp continues_string?(%RollupExpr{expr: expr}), do: continues_string?(expr)

  defp continues_string?(%MetricExpr{filter_sets: [[%LabelFilter{}]] = sets}),
    do: name_prefix(sets) != nil

  defp continues_string?(_expr), do: false

  defp right_parens?(%BinaryOpExpr{right: right, keep_metric_names: keep}) do
    operand_parens?(right) or reserved_operand?(right) or keeps_names?(right, keep)
  end

  defp operand_parens?(%BinaryOpExpr{}), do: true
  defp operand_parens?(%RollupExpr{expr: %BinaryOpExpr{keep_metric_names: true}}), do: true
  defp operand_parens?(%RollupExpr{offset: offset, at: at}), do: offset != nil or at != nil
  defp operand_parens?(_expr), do: false

  defp reserved_operand?(%MetricExpr{} = metric), do: reserved?(metric_name(metric.filter_sets))
  defp reserved_operand?(%FuncExpr{name: name}), do: reserved?(name)
  defp reserved_operand?(_expr), do: false

  defp reserved?(nil), do: false
  defp reserved?(name), do: String.downcase(name) in @reserved

  defp keeps_names?(%FuncExpr{keep_metric_names: func_keep}, keep), do: func_keep or keep
  defp keeps_names?(_expr, _keep), do: false

  defp selector([]), do: "{}"

  defp selector(sets) do
    case name_prefix(sets) do
      nil -> ["{", filter_sets(sets), "}"]
      name -> named_selector(name, Enum.map(sets, &tl/1))
    end
  end

  defp named_selector(name, [[]]), do: Literal.escape_ident(name)
  defp named_selector(name, rests), do: [Literal.escape_ident(name), "{", filter_sets(rests), "}"]

  defp name_prefix(sets) do
    name = metric_name(sets)
    rests = Enum.map(sets, &tl/1)

    cond do
      name in [nil, ""] -> nil
      String.downcase(name) in ["inf", "nan"] -> nil
      match?([_, _ | _], sets) and [] in rests -> nil
      true -> name
    end
  end

  defp metric_name(
         [[%LabelFilter{name: "__name__", op: :eq, value: name} | _rest] | _sets] = sets
       ) do
    if Enum.all?(
         sets,
         &match?([%LabelFilter{name: "__name__", op: :eq, value: ^name} | _rest], &1)
       ),
       do: name,
       else: nil
  end

  defp metric_name(_sets), do: nil

  defp filter_sets(sets), do: Enum.map_intersperse(sets, " or ", &filters/1)

  defp filters(set), do: Enum.map_intersperse(set, ",", &filter/1)

  defp filter(%LabelFilter{name: name, op: op, value: value}),
    do: [label_name(name), Map.fetch!(@filter_ops, op), Literal.quote_string(value)]

  defp label_name(""), do: ~s("")
  defp label_name(name), do: Literal.escape_ident(name)
end
