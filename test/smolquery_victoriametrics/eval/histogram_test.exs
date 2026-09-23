defmodule SmolqueryVictoriaMetrics.Eval.HistogramTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Histogram
  alias SmolqueryVictoriaMetrics.Eval.Series

  @inf 1.797_693_134_862_315_7e308
  @grid [0, 1000]

  defp series(labels, values), do: %Series{labels: labels, values: Enum.zip(@grid, values)}
  defp scalar(v), do: [Series.constant(@grid, v)]
  defp text(s), do: [Series.string(@grid, s)]

  defp buckets do
    for {le, count} <- [{"0.1", 2.0}, {"0.5", 6.0}, {"1", 8.0}, {"+Inf", 10.0}] do
      series(%{"__name__" => "h_bucket", "le" => le, "job" => "a"}, [count, 0.0])
    end
  end

  defp run(name, args) do
    {:ok, list} = Histogram.apply(name, args)
    Enum.map(list, &{&1.labels, Series.values(&1)})
  end

  test "functions/0" do
    assert "histogram_quantile" in Histogram.functions()
  end

  test "histogram_quantile interpolates inside the bucket, drops the name and le" do
    assert [{%{"job" => "a"}, [q, nil]}] = run("histogram_quantile", [scalar(0.5), buckets()])
    assert_in_delta q, 0.1 + 0.4 * (5 - 2) / 4, 1.0e-12
    assert [{_labels, [top, nil]}] = run("histogram_quantile", [scalar(0.99), buckets()])
    assert top == 1.0
    assert [{_labels, [low, nil]}] = run("histogram_quantile", [scalar(-1.0), buckets()])
    assert low == -@inf
  end

  test "histogram_quantile with a bounds label" do
    assert [
             {%{"job" => "a"}, [_q, nil]},
             {%{"job" => "a", "b" => "lower"}, [0.1, nil]},
             {%{"job" => "a", "b" => "upper"}, [0.5, nil]}
           ] = run("histogram_quantile", [scalar(0.5), buckets(), text("b")])
  end

  test "histogram_quantiles labels each phi" do
    assert [{%{"job" => "a", "q" => "0.5"}, _v}, {%{"job" => "a", "q" => "0.9"}, _w}] =
             run("histogram_quantiles", [text("q"), scalar(0.5), scalar(0.9), buckets()])
  end

  test "fix_broken/1 makes a point's counts non-decreasing" do
    assert Histogram.fix_broken([nil, 3.0, 2.0, nil, 5.0]) == [0.0, 3.0, 3.0, 3.0, 5.0]
    assert Histogram.fix_broken([nil]) == [nil]
  end

  test "quantile_at/3 and share_at/3 over one point" do
    les = [1.0, 2.0, @inf]
    assert Histogram.quantile_at(les, [0.0, 0.0, 0.0], 0.5) == {nil, nil, nil}
    assert Histogram.quantile_at(les, [1.0, 2.0, 4.0], 0.9) == {2.0, 2.0, @inf}
    assert Histogram.share_at(les, [1.0, 2.0, 4.0], 1.5) == {0.375, 0.25, 0.5}
    assert Histogram.share_at(les, [1.0, 2.0, 4.0], -1.0) == {0.0, 0.0, 0.0}
  end

  test "histogram_share and histogram_fraction" do
    assert [{%{"job" => "a"}, [0.6, nil]}] = run("histogram_share", [scalar(0.5), buckets()])

    assert [{%{"job" => "a"}, [fraction, nil]}] =
             run("histogram_fraction", [scalar(0.1), scalar(1.0), buckets()])

    assert_in_delta fraction, 0.6, 1.0e-12

    assert {:error, {:invalid_argument, _message}} =
             Histogram.apply("histogram_fraction", [scalar(2.0), scalar(1.0), buckets()])
  end

  test "histogram_avg, histogram_stddev and histogram_stdvar" do
    assert [{_labels, [avg, nil]}] = run("histogram_avg", [buckets()])
    assert_in_delta avg, (0.05 * 2 + 0.3 * 4 + 0.75 * 2) / 8, 1.0e-12
    assert [{_labels, [stdvar, nil]}] = run("histogram_stdvar", [buckets()])
    assert [{_labels, [stddev, nil]}] = run("histogram_stddev", [buckets()])
    assert_in_delta stddev * stddev, stdvar, 1.0e-12
  end

  test "a bucket bound past what n * n holds answers Go's infinity or NaN, not a raise" do
    wide = fn pairs ->
      for {le, count} <- pairs, do: series(%{"le" => le}, [count, count])
    end

    assert [{_labels, [@inf, @inf]}] =
             run("histogram_stdvar", [
               wide.([{"0.1", 1.0e6}, {"1e160", 1_000_001.0}, {"+Inf", 1_000_001.0}])
             ])

    assert [{_labels, [nil, nil]}] =
             run("histogram_stdvar", [wide.([{"0.1", 2.0}, {"1e200", 6.0}, {"+Inf", 10.0}])])

    assert [{_labels, [avg, avg]}] =
             run("histogram_avg", [wide.([{"1e308", 2.0}, {"1.7e308", 4.0}, {"+Inf", 4.0}])])

    assert is_float(avg)
  end

  test "a bucket bound written with a trailing point, 1., is read as Go reads it" do
    input =
      for {le, count} <- [{"1.", 2.0}, {"2.", 4.0}, {"+Inf", 4.0}],
          do: series(%{"le" => le}, [count, count])

    assert [{_labels, [q, q]}] = run("histogram_quantile", [scalar(0.5), input])
    assert q == 1.0
  end

  test "to_le/1 turns vmrange buckets into cumulative le buckets" do
    input = [
      series(%{"vmrange" => "0.1...0.2", "x" => "y"}, [1.0, 0.0]),
      series(%{"vmrange" => "0.2...0.4", "x" => "y"}, [2.0, 1.0])
    ]

    assert run("prometheus_buckets", [input]) |> Enum.sort_by(&elem(&1, 0)["le"]) == [
             {%{"le" => "+Inf", "x" => "y"}, [3.0, 1.0]},
             {%{"le" => "0.1", "x" => "y"}, [0.0, 0.0]},
             {%{"le" => "0.2", "x" => "y"}, [1.0, 0.0]},
             {%{"le" => "0.4", "x" => "y"}, [3.0, 1.0]}
           ]
  end

  test "buckets_limit keeps the first and last bucket and merges the smallest" do
    input =
      for {le, count} <- [{"1", 1.0}, {"2", 2.0}, {"3", 10.0}, {"4", 11.0}, {"+Inf", 20.0}] do
        series(%{"le" => le}, [count, count])
      end

    assert run("buckets_limit", [scalar(3.0), input]) |> Enum.map(&elem(&1, 0)["le"]) == [
             "1",
             "4",
             "+Inf"
           ]

    assert {:error, {:invalid_argument, _message}} =
             Histogram.apply("buckets_limit", [scalar(0.0), input])
  end
end
