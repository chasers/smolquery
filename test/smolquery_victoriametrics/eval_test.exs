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
    test "a number, a duration and infinities are one series with no labels; NaN is none" do
      grid = %{start_ms: 0, end_ms: 15_000, step_ms: 15_000}

      scalar = fn v ->
        {:ok, [%Series{labels: %{}, values: [{0, v}, {15_000, v}]}], %{series: 0, samples: 0}}
      end

      assert run("42", grid, []) == scalar.(42.0)
      assert run("(42)", grid, []) == scalar.(42.0)
      assert run("5m", grid, []) == scalar.(300.0)
      assert run("Inf", grid, []) == scalar.(@inf)
      assert run("-1 + 2 * 3", grid, []) == scalar.(5.0)
      assert run("NaN", grid, []) == {:ok, [], %{series: 0, samples: 0}}
    end

    test "scalar?/1 is true of what always evaluates to a scalar" do
      for query <- ["1+1", "time()", "scalar(up)", "-1", "5m", "pi() * 2", "step()"] do
        {:ok, expr} = MetricsQL.parse(query)
        assert Eval.scalar?(expr), query
      end

      for query <- ["up", "vector(1)", "sum(up)", "up + 1", ~s|"text"|] do
        {:ok, expr} = MetricsQL.parse(query)
        refute Eval.scalar?(expr), query
      end
    end
  end

  describe "the evaluator above the rollups" do
    test "an aggregate over a rollup, grouped by a label" do
      a = counter(%{"__name__" => "m", "job" => "x", "i" => "1"}, 15_000, 20, 6)
      b = counter(%{"__name__" => "m", "job" => "x", "i" => "2"}, 15_000, 20, 3)
      grid = %{start_ms: 60_000, end_ms: 60_000, step_ms: 60_000}

      assert {:ok, [%Series{labels: %{"job" => "x"}, values: [{60_000, sum}]}], %{series: 2}} =
               run("sum by (job) (rate(m[1m]))", grid, [a, b])

      assert_in_delta sum, 0.6, 1.0e-12
    end

    test "a comparison filters points and keeps the name; bool answers 0 or 1" do
      grid = %{start_ms: 0, end_ms: 30_000, step_ms: 15_000}

      assert {:ok, [%Series{labels: %{"__name__" => "up"}, values: values}], _stats} =
               run("up == 0", grid, [@up])

      assert values == [{0, nil}, {15_000, nil}, {30_000, 0.0}]

      assert {:ok, [%Series{labels: %{"job" => "a"}, values: bools}], _stats} =
               run("up == bool 0", grid, [@up])

      assert bools == [{0, 0.0}, {15_000, 0.0}, {30_000, 1.0}]
    end

    test "a rollup over a subquery evaluates the inner query on its own aligned grid" do
      series = counter(%{"__name__" => "m"}, 15_000, 80, 6)
      grid = %{start_ms: 600_000, end_ms: 600_000, step_ms: 60_000}

      assert {:ok, [%Series{labels: %{}, values: [{600_000, value}]}], _stats} =
               run("max_over_time(rate(m[1m])[10m:1m])", grid, [series])

      assert_in_delta value, 0.4, 1.0e-12
      assert_received {:fetch, _selector, {-720_000, 660_000}}
    end

    test "a window on an expression is a subquery at the step, and offset applies to it" do
      grid = %{start_ms: 1_000_000, end_ms: 1_000_000, step_ms: 200_000}

      assert {:ok, [%Series{values: [{1_000_000, 800.0}]}], _stats} =
               run("time() offset 200s", grid, [])

      assert {:ok, [%Series{values: [{1_000_000, 2.0}]}], _stats} =
               run("count_over_time(time()[400s])", grid, [])
    end

    test "@ takes any expression with one series" do
      series = counter(%{"__name__" => "m"}, 15_000, 20, 6)
      grid = %{start_ms: 0, end_ms: 15_000, step_ms: 15_000}

      assert {:ok, [%Series{values: [{0, 24.0}, {15_000, 24.0}]}], _stats} =
               run("m @ (30 + 30)", grid, [series])

      assert {:error, {:invalid_at, "`@` modifier must return a non-NaN value"}} =
               run("m @ NaN", grid, [series])
    end

    test "instant_range/3 turns an instant window on anything but a selector into a range query" do
      {:ok, expr} = MetricsQL.parse("rate(m[1m])[5m:30s]")

      assert {:ok, %{name: "rate"}, {400_000, 700_000, 30_000}} =
               Eval.instant_range(expr, 700_000, 15_000)

      {:ok, expr} = MetricsQL.parse("m[5m:] offset 1m")
      assert {:ok, %{}, {340_000, 640_000, 15_000}} = Eval.instant_range(expr, 700_000, 15_000)

      for query <- ["m[5m]", "rate(m[5m])", "m"] do
        {:ok, expr} = MetricsQL.parse(query)
        assert Eval.instant_range(expr, 700_000, 15_000) == :none, query
      end
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

  describe "what is not ported" do
    for {query, what} <- [
          {"holt_winters(up[5m], 0.5, 0.5)", "rollup function holt_winters()"},
          {"sum(holt_winters(up[5m], 0.5, 0.5))", "rollup function holt_winters()"},
          {"histogram(up)", "aggregate function histogram()"},
          {"rand()", "transform function rand()"},
          {~s|timezone_offset("UTC")|, "transform function timezone_offset()"}
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

  test "a window below zero is refused where VictoriaMetrics refuses it" do
    grid = %{start_ms: 60_000, end_ms: 60_000, step_ms: 15_000}

    for query <- ["rate(up[5m-10m])", "max_over_time(up[5m-10m:1m])"] do
      assert {:error, {:invalid_argument, "duration cannot be negative; got 5m-10m"}} =
               run(query, grid, [@up]),
             query
    end

    {:ok, raw} = MetricsQL.parse("up[5m-10m]")
    assert {:error, {:invalid_argument, _message}} = Eval.raw(raw, 60_000, context(grid, [@up]))
    assert Eval.instant_range(raw, 60_000, 15_000) == :none
  end
end
