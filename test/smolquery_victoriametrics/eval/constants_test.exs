defmodule SmolqueryVictoriaMetrics.Eval.ConstantsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Constants
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number

  defp prepared(query) do
    {:ok, expr} = MetricsQL.parse(query)
    Constants.prepare(expr)
  end

  describe "prepare/1" do
    test "folds arithmetic and comparisons on numbers" do
      assert %Number{value: 166.0} = prepared("-1+2 *3 ^ 4+5%6")
      assert %Number{value: -1.0} = prepared("-1")
      assert %Number{value: 1.0} = prepared("2 > bool 1")
      assert %Number{value: 2.0} = prepared("2 > 1")
      assert %Number{value: :nan} = prepared("1 > 2")
      assert %Number{value: :nan} = prepared("1 unless 2")
    end

    test "folds comparisons of strings" do
      assert %Number{value: 1.0} = prepared(~s|"a" < "b"|)
      assert %Number{value: +0.0} = prepared(~s|"a" > bool "b"|)
      assert %Number{value: :nan} = prepared(~s|"a" > "b"|)
    end

    test "folds inside calls, and turns num cmp q around" do
      assert %{args: [%Number{value: 3.0}, _series]} = prepared("topk(1 + 2, m)")
      assert %BinaryOpExpr{op: :>, right: %Number{value: 0.5}} = prepared("0.5 < foo")
      assert %BinaryOpExpr{op: :<=, left: %{filter_sets: _}} = prepared("time() >= foo")
      assert %BinaryOpExpr{op: :==, left: %{filter_sets: _}} = prepared("1 == foo")
    end
  end

  test "scalar?/1 is isScalarLikeExpr" do
    assert Constants.scalar?(prepared("1 + time()"))
    assert Constants.scalar?(prepared("scalar(m) * 2"))
    refute Constants.scalar?(prepared("vector(1)"))
    refute Constants.scalar?(prepared("m"))
  end

  test "may_sort?/1 is maySortResults" do
    refute Constants.may_sort?(prepared("sort(m)"))
    refute Constants.may_sort?(prepared("topk(2, m)"))
    refute Constants.may_sort?(prepared("a or b"))
    assert Constants.may_sort?(prepared("sum(m)"))
    assert Constants.may_sort?(prepared("a and b"))
  end

  test "union?/1, comparison?/1, compare/3 and arithmetic/3" do
    assert Constants.union?(prepared("(a, b)"))
    assert Constants.union?(prepared("union(a, b)"))
    refute Constants.union?(prepared("a"))
    assert Constants.comparison?(:>=)
    refute Constants.comparison?(:+)
    assert Constants.compare(:==, nil, nil)
    refute Constants.compare(:>, nil, 1.0)
    assert Constants.arithmetic(:default, nil, 2.0) == 2.0
    assert Constants.arithmetic(:ifnot, 1.0, nil) == 1.0
    assert Constants.arithmetic(:if, 1.0, nil) == nil
    assert Constants.arithmetic(:and, 1.0, 2.0) == 1.0
    assert Constants.arithmetic(:or, nil, 2.0) == 2.0
  end
end
