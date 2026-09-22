defmodule SmolqueryVictoriaMetrics.Eval.RangesTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Ranges
  alias SmolqueryVictoriaMetrics.Eval.Series

  @grid [0, 1000, 2000, 3000]

  defp series(values, labels \\ %{"__name__" => "m"}),
    do: %Series{labels: labels, values: Enum.zip(@grid, values)}

  defp scalar(v), do: [Series.constant(@grid, v)]

  defp ranges(name, args) do
    {:ok, list} = Ranges.apply(name, args)
    Enum.map(list, &{&1.labels, Series.values(&1)})
  end

  defp values(name, args), do: name |> ranges(args) |> hd() |> elem(1)

  test "functions/0" do
    assert "running_avg" in Ranges.functions() and "smooth_exponential" in Ranges.functions()
  end

  test "running_* from the first value, repeating across gaps, name dropped" do
    input = [series([nil, 1.0, nil, 3.0])]
    assert ranges("running_sum", [input]) == [{%{}, [nil, 1.0, 1.0, 4.0]}]
    assert values("running_max", [input]) == [nil, 1.0, 1.0, 3.0]
    assert values("running_min", [input]) == [nil, 1.0, 1.0, 1.0]

    assert values("running_avg", [[series([1.0, nil, 3.0, 5.0])]]) == [
             1.0,
             1.0,
             1.0 + (3.0 - 1.0) / 3,
             1.0 + (3.0 - 1.0) / 3 + (5.0 - (1.0 + (3.0 - 1.0) / 3)) / 4
           ]
  end

  test "range_* put one statistic at every point" do
    input = [series([1.0, nil, 3.0, 2.0])]
    assert ranges("range_sum", [input]) == [{%{}, [6.0, 6.0, 6.0, 6.0]}]
    assert values("range_max", [input]) == [3.0, 3.0, 3.0, 3.0]
    assert values("range_min", [input]) == [1.0, 1.0, 1.0, 1.0]
    assert ranges("range_first", [input]) == [{%{"__name__" => "m"}, [1.0, 1.0, 1.0, 1.0]}]
    assert values("range_last", [input]) == [2.0, 2.0, 2.0, 2.0]
    assert values("range_quantile", [scalar(0.5), input]) == [2.0, 2.0, 2.0, 2.0]
    assert values("range_stdvar", [[series([1.0, 3.0, 1.0, 3.0])]]) == [1.0, 1.0, 1.0, 1.0]
    assert values("range_stddev", [[series([1.0, 3.0, 1.0, 3.0])]]) == [1.0, 1.0, 1.0, 1.0]
    assert values("range_mad", [[series([1.0, 2.0, 3.0, 10.0])]]) == [1.0, 1.0, 1.0, 1.0]
  end

  test "rescaling and fitting" do
    assert values("range_zscore", [[series([1.0, 3.0, 1.0, 3.0])]]) == [-1.0, 1.0, -1.0, 1.0]
    assert values("range_normalize", [[series([1.0, 3.0, 2.0, nil])]]) == [0.0, 1.0, 0.5, nil]

    assert ranges("range_normalize", [[series([1.0, 1.0e308 * 1.7976931348623157, 2.0, nil])]]) ==
             []

    assert values("range_linear_regression", [[series([1.0, nil, 3.0, 4.0])]])
           |> Enum.map(&Float.round(&1, 9)) == [1.0, 2.0, 3.0, 4.0]
  end

  test "trimming" do
    assert values("range_trim_outliers", [scalar(1.0), [series([1.0, 2.0, 3.0, 100.0])]]) == [
             nil,
             2.0,
             3.0,
             nil
           ]

    assert values("range_trim_spikes", [scalar(0.5), [series([1.0, 2.0, 3.0, 100.0])]]) == [
             nil,
             2.0,
             3.0,
             nil
           ]

    assert values("range_trim_zscore", [scalar(1.0), [series([1.0, 1.0, 1.0, 9.0])]]) == [
             1.0,
             1.0,
             1.0,
             nil
           ]
  end

  test "gap filling" do
    input = [series([nil, 1.0, nil, 3.0])]
    assert values("keep_last_value", [input]) == [nil, 1.0, 1.0, 3.0]
    assert values("keep_next_value", [input]) == [1.0, 1.0, 3.0, 3.0]
    assert values("interpolate", [input]) == [nil, 1.0, 2.0, 3.0]
    assert Ranges.interpolate([1.0, nil, nil, 4.0, nil]) == [1.0, 2.0, 3.0, 4.0, nil]
  end

  test "remove_resets/1 adds back what resets took" do
    assert values("remove_resets", [[series([5.0, 6.0, 1.0, 2.0])]]) == [5.0, 6.0, 7.0, 8.0]
    assert Ranges.remove_resets([nil, 10.0, 9.0, nil, 11.0]) == [nil, 10.0, 10.0, nil, 12.0]
  end

  test "smooth_exponential/2 blends with its factor, clamped to [0, 1]" do
    assert values("smooth_exponential", [[series([1.0, 3.0, nil, 5.0])], scalar(0.5)]) == [
             1.0,
             2.0,
             nil,
             3.5
           ]

    assert values("smooth_exponential", [[series([1.0, 3.0, 5.0, 7.0])], scalar(2.0)]) == [
             1.0,
             3.0,
             5.0,
             7.0
           ]
  end

  test "stdvar/1 as VictoriaMetrics' stdvar" do
    assert Ranges.stdvar([]) == nil
    assert Ranges.stdvar([nil]) == 0.0
    assert Ranges.stdvar([1.0, 3.0]) == 1.0
  end
end
