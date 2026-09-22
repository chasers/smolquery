defmodule SmolqueryVictoriaMetrics.EvalTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.MetricsQL

  @inf 1.797_693_134_862_315_7e308

  defp context(grid, series) do
    test = self()

    Map.merge(
      %{
        lookback_ms: 300_000,
        max_points: 30_000,
        fetch: fn selector, range ->
          send(test, {:fetch, selector, range})
          {:ok, series}
        end
      },
      grid
    )
  end

  defp run(query, grid, series) do
    {:ok, expr} = MetricsQL.parse(query)
    Eval.run(expr, context(grid, series))
  end

  defp counter(labels, every_ms, count, step) do
    timestamps = for i <- 0..(count - 1), do: i * every_ms

    %{
      labels: labels,
      timestamps: timestamps,
      values: Enum.map(0..(count - 1), &(&1 * step * 1.0))
    }
  end

  @up %{
    labels: %{"__name__" => "up", "job" => "a"},
    timestamps: [0, 15_000, 30_000],
    values: [1.0, 1.0, 0.0]
  }

  describe "a bare selector" do
    test "is default_rollup, the last sample within max(step, lookback), __name__ kept" do
      assert {:ok, [%Series{labels: labels, values: values}], %{series: 1, samples: 3}} =
               run("up", %{start_ms: 0, end_ms: 60_000, step_ms: 30_000}, [@up])

      assert labels == %{"__name__" => "up", "job" => "a"}
      assert values == [{0, 1.0}, {30_000, 0.0}, {60_000, 0.0}]
    end

    test "reads its samples once, from far enough back to hold every window" do
      run("up", %{start_ms: 600_000, end_ms: 660_000, step_ms: 60_000}, [])
      assert_received {:fetch, _selector, range}
      assert range == {600_000 - 300_000 - 300_000, 660_000}
      refute_received {:fetch, _selector, _range}
    end

    test "a sample older than the lookback is gone" do
      assert {:ok, [], _stats} =
               run("up", %{start_ms: 400_000, end_ms: 400_000, step_ms: 1_000}, [@up])
    end
  end

  describe "offset and @" do
    test "offset evaluates on the grid moved back and answers at the grid's points" do
      series = counter(%{"__name__" => "m"}, 15_000, 20, 6)

      assert {:ok, [%Series{values: [{120_000, 24.0}]}], _stats} =
               run("m offset 1m", %{start_ms: 120_000, end_ms: 120_000, step_ms: 15_000}, [series])

      assert {:ok, [%Series{values: [{60_000, 48.0}]}], _stats} =
               run("m offset -1m", %{start_ms: 60_000, end_ms: 60_000, step_ms: 15_000}, [series])
    end

    test "@ evaluates at one time and answers it at every point" do
      series = counter(%{"__name__" => "m"}, 15_000, 20, 6)

      assert {:ok, [%Series{values: values}], _stats} =
               run("m @ 60", %{start_ms: 0, end_ms: 30_000, step_ms: 15_000}, [series])

      assert values == [{0, 24.0}, {15_000, 24.0}, {30_000, 24.0}]

      assert {:ok, [%Series{values: [{0, 48.0}, {60_000, 48.0}, {120_000, 48.0}]}], _stats} =
               run("m @ end()", %{start_ms: 0, end_ms: 120_000, step_ms: 60_000}, [series])
    end
  end

  describe "rollup functions" do
    test "rate drops __name__, keep_metric_names keeps it, max_over_time keeps it anyway" do
      series = counter(%{"__name__" => "m", "job" => "a"}, 15_000, 20, 6)
      grid = %{start_ms: 60_000, end_ms: 120_000, step_ms: 60_000}

      assert {:ok, [%Series{labels: %{"job" => "a"} = labels, values: values}], _stats} =
               run("rate(m[1m])", grid, [series])

      refute Map.has_key?(labels, "__name__")
      assert values == [{60_000, 0.4}, {120_000, 0.4}]

      assert {:ok, [%Series{labels: %{"__name__" => "m"}}], _stats} =
               run("rate(m[1m]) keep_metric_names", grid, [series])

      assert {:ok, [%Series{labels: %{"__name__" => "m"}, values: [{60_000, 24.0}, _next]}], _} =
               run("max_over_time(m[1m])", grid, [series])
    end

    test "scalar parameters, before or after the series" do
      series = counter(%{"__name__" => "m"}, 15_000, 20, 6)
      grid = %{start_ms: 60_000, end_ms: 60_000, step_ms: 60_000}

      assert {:ok, [%Series{values: [{60_000, value}]}], _stats} =
               run("quantile_over_time(0.5, m[1m])", grid, [series])

      assert value == 15.0

      assert {:ok, [%Series{values: [{60_000, 3.0}]}], _stats} =
               run("count_gt_over_time(m[1m], 10)", grid, [series])
    end

    test "an omitted window is the step, widened to the scrape interval where allowed" do
      series = counter(%{"__name__" => "m"}, 60_000, 11, 60)

      assert {:ok, [%Series{values: values}], _stats} =
               run("rate(m)", %{start_ms: 300_000, end_ms: 600_000, step_ms: 15_000}, [series])

      assert Enum.all?(values, fn {_t, v} -> v == 1.0 end)
    end

    test "absent_over_time answers one series from the selector's = matchers" do
      grid = %{start_ms: 0, end_ms: 30_000, step_ms: 15_000}

      assert {:ok, [%Series{labels: %{"job" => "x"}, values: values}], _stats} =
               run(~s|absent_over_time(nothing{job="x", i=~"."}[10s])|, grid, [])

      assert values == [{0, 1.0}, {15_000, 1.0}, {30_000, 1.0}]

      assert {:ok, [], _stats} = run("absent_over_time(up[10s])", grid, [@up])
    end
  end

  describe "scalars" do
    test "a number, a duration, infinities and NaN" do
      grid = %{start_ms: 0, end_ms: 0, step_ms: 15_000}
      assert run("42", grid, []) == {:ok, 42.0, %{series: 0, samples: 0}}
      assert run("(42)", grid, []) == {:ok, 42.0, %{series: 0, samples: 0}}
      assert run("5m", grid, []) == {:ok, 300.0, %{series: 0, samples: 0}}
      assert run("Inf", grid, []) == {:ok, @inf, %{series: 0, samples: 0}}
      assert run("NaN", grid, []) == {:ok, nil, %{series: 0, samples: 0}}
    end
  end

  describe "the answer" do
    test "series left with the same labels are an error" do
      a = %{@up | labels: %{"__name__" => "a", "job" => "x"}}
      b = %{@up | labels: %{"__name__" => "b", "job" => "x"}}

      assert {:error, {:duplicate_series, ~s|duplicate output timeseries: {job="x"}|}} =
               run(
                 "rate({__name__=~\"a|b\"}[1m])",
                 %{start_ms: 30_000, end_ms: 30_000, step_ms: 1_000},
                 [a, b]
               )
    end

    test "series are sorted by name, then labels, and empty ones dropped" do
      b = %{@up | labels: %{"__name__" => "up", "job" => "b"}}
      z = %{@up | labels: %{"__name__" => "up", "job" => "z"}, timestamps: [900_000]}

      assert {:ok, series, _stats} =
               run("up", %{start_ms: 30_000, end_ms: 30_000, step_ms: 1_000}, [z, b, @up])

      assert Enum.map(series, & &1.labels["job"]) == ["a", "b"]
    end

    test "a grid past max_points is refused before anything is read" do
      {:ok, expr} = MetricsQL.parse("up")

      assert {:error, {:too_many_points, message}} =
               Eval.run(expr, %{
                 context(%{start_ms: 0, end_ms: 100, step_ms: 1}, [])
                 | max_points: 10
               })

      assert message =~ "the maximum number of points is 10"
      refute_received {:fetch, _selector, _range}
    end
  end

  describe "what is not evaluated yet" do
    for {query, what} <- [
          {"sum(up)", "aggregate function sum()"},
          {"abs(up)", "transform function abs()"},
          {"up + 1", "binary operator `+`"},
          {"rate(up[5m:1m])", "rate() over anything but a series selector (a subquery)"},
          {"up[5m:1m]", "subqueries, `q[window:step]`"},
          {"holt_winters(up[5m], 0.5, 0.5)", "rollup function holt_winters()"},
          {"(up, down)", "a union of several expressions, `(a, b)`"},
          {~s|"text"|, "a string literal as a result"},
          {"quantile_over_time(up, up[5m])", "a series as a parameter of quantile_over_time()"}
        ] do
      test query do
        assert run(unquote(query), %{start_ms: 0, end_ms: 0, step_ms: 1_000}, [@up]) ==
                 {:error, {:unsupported, unquote(what)}}
      end
    end
  end

  describe "raw/3" do
    test "an instant range vector answers the window's raw samples, lower bound excluded" do
      {:ok, expr} = MetricsQL.parse("up[30s]")
      assert Eval.raw?(expr)

      assert {:ok, [%Series{labels: %{"__name__" => "up"}, values: values}], %{series: 1}} =
               Eval.raw(
                 expr,
                 30_000,
                 context(%{start_ms: 30_000, end_ms: 30_000, step_ms: 1}, [@up])
               )

      assert values == [{15_000, 1.0}, {30_000, 0.0}]
      assert_received {:fetch, _selector, {1, 30_000}}
    end

    test "only a selector with a window is raw" do
      for query <- ["up", "rate(up[1m])", "up[5m:1m]", "up[5m] @ 10"] do
        {:ok, expr} = MetricsQL.parse(query)
        refute Eval.raw?(expr), query
      end
    end
  end

  test "describe/1 writes labels as a selector" do
    assert Eval.describe(%{"__name__" => "up", "b" => "2", "a" => "1"}) == ~s|up{a="1", b="2"}|
  end
end
