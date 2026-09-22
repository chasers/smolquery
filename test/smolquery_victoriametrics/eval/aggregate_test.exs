defmodule SmolqueryVictoriaMetrics.Eval.AggregateTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Aggregate
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier

  @grid [0, 1000]

  defp series(labels, values), do: %Series{labels: labels, values: Enum.zip(@grid, values)}
  defp scalar(v), do: [Series.constant(@grid, v)]
  defp text(s), do: [Series.string(@grid, s)]

  defp expression(query) do
    {:ok, expr} = MetricsQL.parse(query)
    expr
  end

  defp aggregate(query, args) do
    {:ok, list} = Aggregate.apply(expression(query), args, @grid)
    Enum.map(list, &{&1.labels, Series.values(&1)})
  end

  defp input,
    do: [
      series(%{"__name__" => "m", "job" => "a", "i" => "1"}, [1.0, nil]),
      series(%{"__name__" => "m", "job" => "a", "i" => "2"}, [3.0, 4.0]),
      series(%{"__name__" => "m", "job" => "b", "i" => "1"}, [5.0, nil]),
      series(%{"__name__" => "m", "job" => "b", "i" => "2"}, [nil, nil])
    ]

  test "functions/0 lists the ported aggregates" do
    assert "sum" in Aggregate.functions() and "topk_last" in Aggregate.functions()
    refute "histogram" in Aggregate.functions()
  end

  test "group_labels/2 is removeGroupTags" do
    labels = %{"__name__" => "m", "a" => "1", "b" => "2"}
    assert Aggregate.group_labels(labels, nil) == %{}

    assert Aggregate.group_labels(labels, %Modifier{op: :by, labels: ["a", "__name__"]}) == %{
             "__name__" => "m",
             "a" => "1"
           }

    assert Aggregate.group_labels(labels, %Modifier{op: :without, labels: ["a"]}) == %{"b" => "2"}
  end

  test "reducers group by labels and skip missing points" do
    assert aggregate("sum(m) by (job)", [input()]) == [
             {%{"job" => "a"}, [4.0, 4.0]},
             {%{"job" => "b"}, [5.0, nil]}
           ]

    assert aggregate("sum(m)", [input()]) == [{%{}, [9.0, 4.0]}]

    assert aggregate("sum(m) without (i)", [input()]) == [
             {%{"job" => "a"}, [4.0, 4.0]},
             {%{"job" => "b"}, [5.0, nil]}
           ]

    assert aggregate("avg(m)", [input()]) == [{%{}, [3.0, 4.0]}]
    assert aggregate("min(m)", [input()]) == [{%{}, [1.0, 4.0]}]
    assert aggregate("max(m)", [input()]) == [{%{}, [5.0, 4.0]}]
    assert aggregate("count(m)", [input()]) == [{%{}, [3.0, 1.0]}]
    assert aggregate("group(m)", [input()]) == [{%{}, [1.0, 1.0]}]
    assert aggregate("sum2(m)", [input()]) == [{%{}, [35.0, 16.0]}]
    assert aggregate("distinct(m)", [input()]) == [{%{}, [3.0, 1.0]}]
    assert aggregate("median(m)", [input()]) == [{%{}, [3.0, 4.0]}]

    assert aggregate("mode(m)", [[series(%{}, [1.0, 2.0]), series(%{}, [1.0, 3.0])]])
           |> hd()
           |> elem(1) == [1.0, 2.0]

    assert [{%{}, [stdvar, +0.0]}] = aggregate("stdvar(m)", [input()])
    assert_in_delta stdvar, 8 / 3, 1.0e-12
    assert [{%{}, [geomean, 4.0]}] = aggregate("geomean(m)", [input()])
    assert_in_delta geomean, 15 ** (1 / 3), 1.0e-12
    assert aggregate("mad(m)", [input()]) == [{%{}, [2.0, 0.0]}]
  end

  test "by (__name__) keeps the name, and limit keeps the first groups" do
    assert [{%{"__name__" => "m"}, _values}] = aggregate("sum(m) by (__name__)", [input()])
    assert aggregate("sum(m) by (job) limit 1", [input()]) == [{%{"job" => "a"}, [4.0, 4.0]}]
  end

  test "any, share and zscore keep the series' own labels" do
    assert [{%{"__name__" => "m", "job" => "a", "i" => "1"}, _v}, {%{"job" => "b"} = _b, _w}] =
             aggregate("any(m) by (job)", [input()])

    assert aggregate("share(m) by (job)", [Enum.take(input(), 2)]) |> Enum.map(&elem(&1, 1)) == [
             [0.25, nil],
             [0.75, 1.0]
           ]

    assert aggregate("zscore(m)", [Enum.take(input(), 2)]) |> Enum.map(&elem(&1, 1)) == [
             [-1.0, nil],
             [1.0, nil]
           ]
  end

  test "quantile, quantiles and count_values" do
    assert aggregate("quantile(0.5, m)", [scalar(0.5), input()]) == [{%{}, [3.0, 4.0]}]

    assert aggregate(~s|quantiles("q", 0, 1, m)|, [text("q"), scalar(0.0), scalar(1.0), input()]) ==
             [
               {%{"q" => "0"}, [1.0, 4.0]},
               {%{"q" => "1"}, [5.0, 4.0]}
             ]

    counts = [series(%{"i" => "1"}, [1.0, 2.0]), series(%{"i" => "2"}, [1.0, 2.5])]

    assert aggregate(~s|count_values("v", m)|, [text("v"), counts]) == [
             {%{"v" => "1"}, [2.0, nil]},
             {%{"v" => "2"}, [nil, 1.0]},
             {%{"v" => "2.5"}, [nil, 1.0]}
           ]
  end

  test "topk and bottomk choose at each point" do
    input = [series(%{"i" => "1"}, [1.0, 9.0]), series(%{"i" => "2"}, [5.0, 2.0])]

    assert aggregate("topk(1, m)", [scalar(1.0), input]) == [
             {%{"i" => "1"}, [nil, 9.0]},
             {%{"i" => "2"}, [5.0, nil]}
           ]

    assert aggregate("bottomk(1, m)", [scalar(1.0), input]) == [
             {%{"i" => "2"}, [nil, 2.0]},
             {%{"i" => "1"}, [1.0, nil]}
           ]
  end

  test "topk_max ranks whole series, with a series summing the rest" do
    input = [
      series(%{"i" => "1"}, [1.0, 9.0]),
      series(%{"i" => "2"}, [5.0, 2.0]),
      series(%{"i" => "3"}, [1.0, 1.0])
    ]

    assert aggregate(~s|topk_max(1, m, "i=rest")|, [scalar(1.0), input, text("i=rest")]) == [
             {%{"i" => "rest"}, [6.0, 3.0]},
             {%{"i" => "1"}, [1.0, 9.0]}
           ]

    assert aggregate("bottomk_avg(1, m)", [scalar(1.0), input]) == [{%{"i" => "3"}, [1.0, 1.0]}]
  end

  test "limitk keeps k series of a group, the same ones each time" do
    input = for i <- 1..5, do: series(%{"i" => "#{i}"}, [1.0, 1.0])
    first = aggregate("limitk(2, m)", [scalar(2.0), input])
    assert [_one, _two] = first
    assert aggregate("limitk(2, m)", [scalar(2.0), Enum.reverse(input)]) == first
  end

  test "the outlier aggregates" do
    input = [
      series(%{"i" => "1"}, [1.0, 1.0]),
      series(%{"i" => "2"}, [1.1, 1.0]),
      series(%{"i" => "3"}, [1.0, 1.0]),
      series(%{"i" => "4"}, [50.0, 1.0])
    ]

    assert [{%{"i" => "4"}, _v}] = aggregate("outliers_iqr(m)", [input])
    assert [{%{"i" => "4"}, _v}] = aggregate("outliers_mad(3, m)", [scalar(3.0), input])
    assert [{%{"i" => "4"}, _v}] = aggregate("outliersk(1, m)", [scalar(1.0), input])
  end

  test "wrong arguments and aggregates not ported" do
    assert {:error, {:invalid_argument, "arg #1 must be a scalar"}} =
             Aggregate.apply(expression("topk(k, m)"), [[], input()], @grid)

    assert {:error, {:unsupported, "aggregate function histogram()"}} =
             Aggregate.apply(expression("histogram(m)"), [input()], @grid)
  end
end
