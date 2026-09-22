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
end
