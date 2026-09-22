defmodule SmolqueryVictoriaMetrics.MetricsQL.AstTest do
  use ExUnit.Case, async: true

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

  test "modifiers and flags default to absent" do
    assert %RollupExpr{expr: nil} == %RollupExpr{
             expr: nil,
             window: nil,
             step: nil,
             offset: nil,
             at: nil,
             inherit_step: false
           }

    assert %FuncExpr{name: "rate", args: []}.keep_metric_names == false

    assert %AggrFuncExpr{name: "sum", args: []} |> Map.take([:modifier, :limit]) == %{
             modifier: nil,
             limit: nil
           }

    node = %BinaryOpExpr{op: :+, left: nil, right: nil}
    assert {node.bool, node.keep_metric_names} == {false, false}
    assert {node.group_modifier, node.join_modifier, node.join_prefix} == {nil, nil, nil}
    assert {node.fill_left, node.fill_right} == {nil, nil}
  end

  test "every node requires the fields that make it what it is" do
    for {module, fields} <- [
          {Number, %{value: 1.0}},
          {StringLiteral, %{}},
          {Duration, %{text: "5m", ms: 300_000}},
          {LabelFilter, %{name: "a", op: :eq}},
          {MetricExpr, %{}},
          {RollupExpr, %{}},
          {FuncExpr, %{name: "abs"}},
          {Modifier, %{op: :by}},
          {AggrFuncExpr, %{name: "sum"}},
          {BinaryOpExpr, %{op: :+, left: nil}},
          {ParensExpr, %{}}
        ] do
      assert_raise ArgumentError, fn -> struct!(module, fields) end
    end
  end
end
