defmodule SmolqueryVictoriaMetrics.MetricsQL.BuiltinsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.MetricsQL.Builtins

  defp printed(query) do
    {:ok, expr} = MetricsQL.parse(query)
    MetricsQL.to_string(expr)
  end

  test "expand/2 answers :none for anything but a template, and checks the count" do
    assert Builtins.expand("rate", []) == :none
    assert Builtins.expand("Alias", []) == :none
    assert {:error, {:arity, message}} = Builtins.expand("alias", [])
    assert message =~ "want 2"
  end

  test "the parser expands the four templates metricsql defines" do
    assert printed(~s|alias(time(), "foo")|) == ~s|label_set(time(), "__name__", "foo")|
    assert printed("range_median(m)") == "range_quantile(0.5, m)"
    assert printed("ru(free, max)") =~ "clamp_min(max - clamp_min(free, 0), 0)"
    assert printed("ttf(free)") =~ "smooth_exponential(clamp_max("
  end

  test "alias with no parentheses is still a metric" do
    assert printed("alias") == "alias"
    assert {:error, {:arity, _message}} = MetricsQL.parse("alias(1)")
  end

  test "expand_all/1 expands every template in a tree, innermost first" do
    {:ok, expr} = MetricsQL.parse(~s|sum(alias(range_median(m), "x")) + ru(a, b)|)

    assert MetricsQL.to_string(expr) ==
             ~s|sum(label_set(range_quantile(0.5, m), "__name__", "x")) + | <>
               "((clamp_min(b - clamp_min(a, 0), 0) / clamp_min(b, 0)) * 100)"

    call = %SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr{name: "ru", args: []}
    assert {:error, {:arity, _message}} = Builtins.expand_all(call)
  end
end
