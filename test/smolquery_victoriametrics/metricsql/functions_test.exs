defmodule SmolqueryVictoriaMetrics.MetricsQL.FunctionsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Functions
  alias SmolqueryVictoriaMetrics.MetricsQL.Parser

  test "knows every name metricsql v0.87.4 lists, once" do
    names = Functions.names()
    assert Enum.count(names) == 228
    assert Enum.count(Enum.uniq(names)) == 228

    assert Enum.frequencies_by(names, &Functions.kind/1) == %{
             rollup: 80,
             transform: 111,
             aggregate: 37
           }
  end

  test "kind/1 is case-insensitive" do
    assert Functions.kind("RATE") == :rollup
    assert Functions.kind("histogram_quantile") == :transform
    assert Functions.kind("Count_Values") == :aggregate
    assert Functions.kind("frobnicate") == :unknown
  end

  test "arity/1 answers the range VictoriaMetrics v1.152.0 enforces" do
    assert Functions.arity("rate") == {:ok, {1, 1}}
    assert Functions.arity("quantile_over_time") == {:ok, {2, 2}}
    assert Functions.arity("holt_winters") == {:ok, {3, 3}}
    assert Functions.arity("histogram_quantile") == {:ok, {2, 3}}
    assert Functions.arity("label_replace") == {:ok, {5, 5}}
    assert Functions.arity("label_join") == {:ok, {3, :infinity}}
    assert Functions.arity("time") == {:ok, {0, 0}}
    assert Functions.arity("union") == {:ok, {0, :infinity}}
    assert Functions.arity("topk") == {:ok, {2, 2}}
    assert Functions.arity("sum") == {:ok, {1, :infinity}}
    assert Functions.arity("nope") == :error
  end

  describe "check/1" do
    test "passes known calls with counts in range, at any depth" do
      {:ok, expr} =
        Parser.parse("sum(rate(m[5m])) by (a) / on(a) group_left max(time() - timestamp(m))")

      assert Functions.check(expr) == :ok
    end

    test "names the first unknown function, innermost first" do
      {:error, reason} = Parser.parse("abs(nope(yikes(m)))")
      assert reason == {:unknown_function, "yikes"}
    end

    test "describes the count a function takes" do
      assert Parser.parse("time(1)") == {:error, {:arity, "time() takes 0 args; got 1"}}
      assert Parser.parse("rate()") == {:error, {:arity, "rate() takes 1 arg; got 0"}}

      assert Parser.parse("label_join(m)") ==
               {:error, {:arity, "label_join() takes at least 3 args; got 1"}}

      assert Parser.parse("histogram_quantile(1)") ==
               {:error, {:arity, "histogram_quantile() takes 2 to 3 args; got 1"}}
    end
  end
end
