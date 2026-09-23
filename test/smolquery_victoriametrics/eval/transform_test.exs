defmodule SmolqueryVictoriaMetrics.Eval.TransformTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Transform
  alias SmolqueryVictoriaMetrics.MetricsQL

  @inf 1.797_693_134_862_315_7e308
  @timestamps [0, 1000, 2000]
  @grid %{timestamps: @timestamps, start_ms: 0, end_ms: 2000, step_ms: 1000}

  defp series(labels, values), do: %Series{labels: labels, values: Enum.zip(@timestamps, values)}
  defp scalar(v), do: [Series.constant(@timestamps, v)]
  defp text(s), do: [Series.string(@timestamps, s)]

  defp call(query) do
    {:ok, expr} = MetricsQL.parse(query)
    expr
  end

  defp transform(query, args) do
    {:ok, list} = Transform.apply(call(query), args, @grid)
    Enum.map(list, &{&1.labels, Series.values(&1)})
  end

  test "functions/0 spans this module and those it hands to" do
    for name <- ~w(abs histogram_quantile label_replace running_sum time) do
      assert name in Transform.functions()
    end

    refute "rand" in Transform.functions()
  end

  test "math/2 has Go's edges" do
    assert Transform.math("ln", 0.0) == -@inf
    assert Transform.math("ln", -1.0) == nil
    assert Transform.math("log10", 100.0) == 2.0
    assert Transform.math("exp", 1000.0) == @inf
    assert Transform.math("sqrt", -4.0) == nil
    assert Transform.math("asin", 2.0) == nil
    assert Transform.math("sin", @inf) == nil
    assert Transform.math("atanh", 1.0) == @inf
    assert Transform.math("deg", :math.pi()) == 180.0
    assert Transform.math("abs", nil) == nil
  end

  test "one-value functions drop the name unless they keep it" do
    input = [series(%{"__name__" => "m", "a" => "1"}, [-1.5, 2.5, nil])]
    assert transform("abs(m)", [input]) == [{%{"a" => "1"}, [1.5, 2.5, nil]}]

    assert transform("abs(m) keep_metric_names", [input]) == [
             {%{"__name__" => "m", "a" => "1"}, [1.5, 2.5, nil]}
           ]

    assert transform("ceil(m)", [input]) == [{%{"__name__" => "m", "a" => "1"}, [-1.0, 3.0, nil]}]
    assert transform("sgn(m)", [input]) == [{%{"a" => "1"}, [-1.0, 1.0, 0.0]}]
  end

  test "round, clamp and bitmaps take a scalar per point" do
    input = [series(%{}, [1.24, -1.36, 17.0])]
    assert transform("round(m, 0.1)", [input, scalar(0.1)]) == [{%{}, [1.2, -1.4, 17.0]}]
    assert transform("round(m)", [input]) == [{%{}, [1.0, -1.0, 17.0]}]

    assert transform("clamp(m, 0, 2)", [input, scalar(0.0), scalar(2.0)]) == [
             {%{}, [1.24, 0.0, 2.0]}
           ]

    assert transform("clamp_min(m, 0)", [input, scalar(0.0)]) == [{%{}, [1.24, 0.0, 17.0]}]
    assert transform("clamp_max(m, 2)", [input, scalar(2.0)]) == [{%{}, [1.24, -1.36, 2.0]}]

    assert transform("bitmap_and(m, 1)", [input, scalar(1.0)]) |> hd() |> elem(1) |> List.last() ==
             1.0
  end

  test "round_to/2 and decimal_exponent/1" do
    assert Transform.round_to(2.5, 1.0) == 3.0
    assert Transform.round_to(123.456, 10.0) == 120.0
    assert Transform.round_to(1.0, 0.0) == nil
    assert Transform.round_to(5.0, 1.0e-320) == nil
    assert Transform.round_to(0.0, 1.0e-320) == nil
    assert Transform.decimal_exponent(0.01) == -2
    assert Transform.decimal_exponent(1.0) == 0
    assert Transform.decimal_exponent(100.0) == 2
    assert Transform.decimal_exponent(1.5) == -1
  end

  test "calendar functions read seconds in UTC, time() by default" do
    input = [series(%{}, [1_700_000_000.0, 0.0, nil])]
    assert transform("hour(m)", [input]) == [{%{}, [22.0, 0.0, nil]}]
    assert transform("day_of_week(m)", [input]) == [{%{}, [2.0, 4.0, nil]}]
    assert transform("year()", []) == [{%{}, [1970.0, 1970.0, 1970.0]}]
    assert transform("days_in_month(m)", [input]) |> hd() |> elem(1) |> hd() == 30.0
  end

  test "scalar makers" do
    assert transform("time()", []) == [{%{}, [0.0, 1.0, 2.0]}]
    assert transform("step()", []) == [{%{}, [1.0, 1.0, 1.0]}]
    assert transform("end()", []) == [{%{}, [2.0, 2.0, 2.0]}]
    assert [{%{}, [pi | _rest]}] = transform("pi()", [])
    assert pi == :math.pi()
    assert transform(~s|scalar("-1.5")|, [text("-1.5")]) == [{%{}, [-1.5, -1.5, -1.5]}]

    assert transform("scalar(m)", [[series(%{"a" => "1"}, [1.0, 2.0, 3.0])]]) == [
             {%{}, [1.0, 2.0, 3.0]}
           ]

    assert transform("scalar(m)", [[series(%{}, [1.0, 1.0, 1.0]), series(%{}, [2.0, 2.0, 2.0])]]) ==
             [{%{}, [nil, nil, nil]}]

    assert transform("vector(m)", [scalar(1.0)]) == [{%{}, [1.0, 1.0, 1.0]}]
  end

  test "absent/3 and absent_labels/1" do
    assert transform(~s|absent(m{job="x", i=~"."})|, [[]]) == [{%{"job" => "x"}, [1.0, 1.0, 1.0]}]
    assert transform("absent(m)", [[series(%{}, [nil, 1.0, nil])]]) == [{%{}, [1.0, nil, 1.0]}]
    assert Transform.absent_labels(call("sum(m)")) == %{}
  end

  test "union/2 dedupes by labels, keeping scalars as they are" do
    a = series(%{"a" => "1"}, [1.0, 1.0, 1.0])
    b = series(%{"a" => "1"}, [2.0, 2.0, 2.0])
    assert Transform.union([[a], [b]], @timestamps) == [a]
    assert [_one, _two] = Transform.union([scalar(1.0), scalar(2.0)], @timestamps)
    assert [%Series{values: [{0, nil} | _]}] = Transform.union([], @timestamps)
  end

  test "sorts" do
    input = [
      series(%{"n" => "10"}, [1.0, 1.0, 3.0]),
      series(%{"n" => "9"}, [9.0, 9.0, 2.0]),
      series(%{"n" => "a"}, [0.0, 0.0, nil])
    ]

    assert transform("sort(m)", [input]) |> Enum.map(&elem(&1, 0)) == [
             %{"n" => "a"},
             %{"n" => "9"},
             %{"n" => "10"}
           ]

    assert transform("sort_desc(m)", [input]) |> Enum.map(&elem(&1, 0)) == [
             %{"n" => "a"},
             %{"n" => "10"},
             %{"n" => "9"}
           ]

    assert transform(~s|sort_by_label(m, "n")|, [input, text("n")]) |> Enum.map(&elem(&1, 0)) == [
             %{"n" => "10"},
             %{"n" => "9"},
             %{"n" => "a"}
           ]

    assert transform(~s|sort_by_label_numeric(m, "n")|, [input, text("n")])
           |> Enum.map(&elem(&1, 0)) == [%{"n" => "9"}, %{"n" => "10"}, %{"n" => "a"}]

    assert Transform.numeric_less?("host2", "host10")
    refute Transform.numeric_less?("b", "a")
  end

  test "limit_offset, drop_empty_series and drop_common_labels" do
    input = for i <- 1..4, do: series(%{"i" => "#{i}", "c" => "x"}, [1.0, 1.0, 1.0])

    assert transform("limit_offset(2, 1, m)", [scalar(2.0), scalar(1.0), input])
           |> Enum.map(&elem(&1, 0)) == [%{"i" => "2", "c" => "x"}, %{"i" => "3", "c" => "x"}]

    assert transform("drop_empty_series(m)", [[series(%{}, [nil, nil, nil])]]) == []

    assert transform("drop_common_labels(m)", [Enum.take(input, 2)]) |> Enum.map(&elem(&1, 0)) ==
             [%{"i" => "1"}, %{"i" => "2"}]
  end

  test "functions not ported" do
    assert {:error, {:unsupported, "transform function rand()"}} =
             Transform.apply(call("rand()"), [], @grid)
  end
end
