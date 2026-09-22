defmodule SmolqueryVictoriaMetrics.Eval.BinaryTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Binary
  alias SmolqueryVictoriaMetrics.Eval.Constants
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.MetricsQL

  @grid [0, 1000, 2000]

  defp series(labels, values), do: %Series{labels: labels, values: Enum.zip(@grid, values)}
  defp scalar(v), do: [Series.constant(@grid, v)]

  defp expression(query) do
    {:ok, expr} = MetricsQL.parse(query)
    Constants.prepare(expr)
  end

  defp apply_op(query, left, right), do: Binary.apply(expression(query), left, right)

  defp shape({:ok, list}), do: Enum.map(list, &{&1.labels, Series.values(&1)})

  describe "arithmetic" do
    test "a scalar pairs with every series; the name is dropped" do
      left = [series(%{"__name__" => "m", "a" => "1"}, [1.0, 2.0, nil])]

      assert shape(apply_op("a + 1", left, scalar(10.0))) == [{%{"a" => "1"}, [11.0, 12.0, nil]}]
      assert shape(apply_op("2 * a", scalar(2.0), left)) == [{%{"a" => "1"}, [2.0, 4.0, nil]}]
      assert shape(apply_op("a / 0", left, scalar(0.0))) |> hd() |> elem(1) |> hd() >= 1.0e308
    end

    test "keep_metric_names keeps the name" do
      left = [series(%{"__name__" => "m"}, [1.0, 1.0, 1.0])]

      assert [{%{"__name__" => "m"}, _values}] =
               shape(apply_op("(a + 1) keep_metric_names", left, scalar(1.0)))
    end

    test "series match on their labels without the name; on() and ignoring() narrow them" do
      left = [series(%{"__name__" => "a", "job" => "x", "i" => "1"}, [4.0, 4.0, 4.0])]
      right = [series(%{"__name__" => "b", "job" => "x", "i" => "1"}, [2.0, 2.0, 2.0])]
      other = [series(%{"__name__" => "b", "job" => "x", "i" => "2"}, [2.0, 2.0, 2.0])]

      assert shape(apply_op("a / b", left, right)) == [
               {%{"job" => "x", "i" => "1"}, [2.0, 2.0, 2.0]}
             ]

      assert shape(apply_op("a / b", left, other)) == []
      assert shape(apply_op("a / on(job) b", left, other)) == [{%{"job" => "x"}, [2.0, 2.0, 2.0]}]

      assert shape(apply_op("a / ignoring(i) b", left, other)) == [
               {%{"job" => "x"}, [2.0, 2.0, 2.0]}
             ]
    end

    test "group_left copies labels from the one side, (*) all of them with a prefix" do
      many = [
        series(%{"job" => "x", "i" => "1"}, [4.0, 4.0, 4.0]),
        series(%{"job" => "x", "i" => "2"}, [6.0, 6.0, 6.0])
      ]

      info = [series(%{"__name__" => "info", "job" => "x", "team" => "t"}, [2.0, 2.0, 2.0])]

      assert shape(apply_op("a / on(job) group_left(team) b", many, info)) == [
               {%{"job" => "x", "i" => "1", "team" => "t"}, [2.0, 2.0, 2.0]},
               {%{"job" => "x", "i" => "2", "team" => "t"}, [3.0, 3.0, 3.0]}
             ]

      assert [{%{"p_team" => "t", "i" => "1", "job" => "x"}, _v} | _rest] =
               shape(apply_op(~s|a / on(job) group_left(*) prefix "p_" b|, many, info))

      assert [{%{"team" => "t", "i" => "1"}, [0.5, 0.5, 0.5]} | _rest] =
               shape(apply_op("b / on(job) group_right(team) a", info, many))
    end

    test "duplicates on one side are an error, unless they do not overlap" do
      dupes = [
        series(%{"job" => "x", "i" => "1"}, [1.0, 1.0, 1.0]),
        series(%{"job" => "x", "i" => "2"}, [1.0, 1.0, 1.0])
      ]

      one = [series(%{"job" => "x"}, [1.0, 1.0, 1.0])]

      assert {:error, {:duplicate_series, message}} = apply_op("a + on(job) b", dupes, one)

      assert message ==
               ~s|duplicate time series on the left side of + on(job): {i="1", job="x"} and {i="2", job="x"}|

      split = [
        series(%{"job" => "x", "i" => "1"}, [1.0, nil, nil]),
        series(%{"job" => "x", "i" => "2"}, [nil, 2.0, 3.0])
      ]

      assert shape(apply_op("a + on(job) b", split, one)) == [{%{"job" => "x"}, [2.0, 3.0, 4.0]}]
    end

    test "fill_left and fill_right stand in for a missing side, not for two missing points" do
      left = [series(%{"k" => "a"}, [1.0, nil, 1.0])]
      right = [series(%{"k" => "b"}, [5.0, 5.0, 5.0])]

      assert shape(apply_op("a + fill(0) b", left, right)) == [
               {%{"k" => "a"}, [1.0, nil, 1.0]},
               {%{"k" => "b"}, [5.0, 5.0, 5.0]}
             ]
    end
  end

  describe "comparisons" do
    test "without bool they filter and keep the name; with bool they answer 1 or 0" do
      left = [series(%{"__name__" => "up", "job" => "a"}, [1.0, 0.0, nil])]

      assert shape(apply_op("up == 0", left, scalar(0.0))) == [
               {%{"__name__" => "up", "job" => "a"}, [nil, 0.0, nil]}
             ]

      assert shape(apply_op("up == bool 0", left, scalar(0.0))) == [
               {%{"job" => "a"}, [0.0, 1.0, nil]}
             ]
    end

    test "q == (1, 2) keeps the points equal to a member of the union" do
      left = [series(%{"a" => "1"}, [1.0, 2.0, 3.0])]

      assert shape(apply_op("q == (1, 2)", left, scalar(1.0) ++ scalar(2.0))) == [
               {%{"a" => "1"}, [1.0, 2.0, nil]}
             ]

      assert shape(apply_op("q != (1, 2)", left, scalar(1.0) ++ scalar(2.0))) == [
               {%{"a" => "1"}, [nil, nil, 3.0]}
             ]
    end
  end

  describe "set operators" do
    setup do
      %{
        a: [series(%{"k" => "1"}, [1.0, nil, 1.0]), series(%{"k" => "2"}, [2.0, 2.0, 2.0])],
        b: [series(%{"k" => "1"}, [nil, 9.0, 9.0]), series(%{"k" => "3"}, [3.0, 3.0, 3.0])]
      }
    end

    test "and, unless, or", %{a: a, b: b} do
      assert shape(apply_op("a and b", a, b)) == [{%{"k" => "1"}, [nil, nil, 1.0]}]

      assert shape(apply_op("a unless b", a, b)) == [
               {%{"k" => "1"}, [1.0, nil, nil]},
               {%{"k" => "2"}, [2.0, 2.0, 2.0]}
             ]

      assert shape(apply_op("a or b", a, b)) == [
               {%{"k" => "1"}, [1.0, 9.0, 1.0]},
               {%{"k" => "2"}, [2.0, 2.0, 2.0]},
               {%{"k" => "3"}, [3.0, 3.0, 3.0]}
             ]
    end

    test "default, if and ifnot; a lone scalar matches every key", %{a: a, b: b} do
      assert shape(apply_op("a default b", a, b)) == [
               {%{"k" => "1"}, [1.0, 9.0, 1.0]},
               {%{"k" => "2"}, [2.0, 2.0, 2.0]}
             ]

      assert shape(apply_op("a default 0", a, scalar(0.0))) |> hd() ==
               {%{"k" => "1"}, [1.0, 0.0, 1.0]}

      assert shape(apply_op("a if b", a, b)) == [{%{"k" => "1"}, [nil, nil, 1.0]}]

      assert shape(apply_op("a ifnot b", a, b)) == [
               {%{"k" => "1"}, [1.0, nil, nil]},
               {%{"k" => "2"}, [2.0, 2.0, 2.0]}
             ]
    end
  end

  test "merge/2 merges series overlapping at two points at most" do
    a = series(%{}, [1.0, nil, nil])
    b = series(%{}, [nil, 2.0, 3.0])
    assert {:ok, merged} = Binary.merge(a, b)
    assert Series.values(merged) == [1.0, 2.0, 3.0]
    assert Binary.merge(series(%{}, [1.0, 1.0, 1.0]), series(%{}, [1.0, 1.0, 1.0])) == :error
  end
end
