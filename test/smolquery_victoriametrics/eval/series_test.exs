defmodule SmolqueryVictoriaMetrics.Eval.SeriesTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Series

  @grid [0, 1000, 2000]

  test "constant/2, generate/2 and string/2 build series on a grid" do
    assert Series.constant(@grid, 1.0).values == [{0, 1.0}, {1000, 1.0}, {2000, 1.0}]
    assert Series.generate(@grid, &(&1 / 1000)).values == [{0, 0.0}, {1000, 1.0}, {2000, 2.0}]

    assert %Series{labels: %{"__name__" => "text"}, values: [{0, nil} | _rest]} =
             Series.string(@grid, "text")

    assert Series.string(@grid, "").labels == %{}
  end

  test "values/1, timestamps/1, put_values/2 and map_values/2" do
    series = Series.constant(@grid, 1.0)
    assert Series.values(series) == [1.0, 1.0, 1.0]
    assert Series.timestamps(series) == @grid
    assert Series.values(Series.put_values(series, [1.0, nil, 3.0])) == [1.0, nil, 3.0]
    assert Series.values(Series.map_values(series, &(&1 * 2))) == [2.0, 2.0, 2.0]
  end

  test "empty?/1, drop_empty/1 and scalar?/1" do
    empty = Series.constant(@grid, nil)
    full = Series.constant(@grid, 1.0)
    assert Series.empty?(empty)
    assert Series.drop_empty([empty, full]) == [full]
    assert Series.scalar?([full])
    refute Series.scalar?([full, full])
    refute Series.scalar?([%{full | labels: %{"a" => "b"}}])
  end

  test "label helpers are MetricName's" do
    labels = %{"__name__" => "m", "a" => "1", "b" => "2"}
    assert Series.label(labels, "a") == "1"
    assert Series.label(labels, "zzz") == ""
    assert Series.put_label(labels, "a", "") == %{"__name__" => "m", "b" => "2"}
    assert Series.put_label(labels, "c", "3")["c"] == "3"
    assert Series.on(labels, ["a"]) == %{"a" => "1"}
    assert Series.on(labels, ["__name__", "a"]) == %{"__name__" => "m", "a" => "1"}
    assert Series.ignoring(labels, ["a"]) == %{"__name__" => "m", "b" => "2"}
    assert Series.ignoring(labels, []) == labels
    assert Series.drop_name(labels) == %{"a" => "1", "b" => "2"}
  end

  test "describe/1 and describe_tags/1 write a series as VictoriaMetrics does" do
    labels = %{"__name__" => "up", "b" => "2", "a" => "1"}
    assert Series.describe(labels) == ~s|up{a="1", b="2"}|
    assert Series.describe_tags(labels) == ~s|{a="1", b="2"}|
  end

  test "sort/1 and sort_key/1 order by name, then labels" do
    a = %Series{labels: %{"__name__" => "b"}, values: []}
    b = %Series{labels: %{"__name__" => "a", "x" => "2"}, values: []}
    c = %Series{labels: %{"__name__" => "a", "x" => "1"}, values: []}
    assert Series.sort([a, b, c]) == [c, b, a]
    assert Series.sort_key(c) == {"a", [{"x", "1"}]}
  end
end
