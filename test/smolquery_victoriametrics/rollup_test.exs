defmodule SmolqueryVictoriaMetrics.RollupTest do
  @moduledoc """
  The cases of VictoriaMetrics v1.152.0's `app/vmselect/promql/rollup_test.go`,
  for the functions this port computes, with its fixtures: NaN is `nil` and
  `±Inf` is the largest double.
  """

  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Rollup
  alias SmolqueryVictoriaMetrics.Rollup.Window

  @inf 1.797_693_134_862_315_7e308

  @test_values [123, 34, 44, 21, 54, 34, 99, 12, 44, 32, 34, 34] |> Enum.map(&(&1 * 1.0))
  @test_timestamps [5, 15, 24, 36, 49, 60, 78, 80, 97, 115, 120, 130]

  defp fixture_window(name) do
    values =
      if String.downcase(name) in ~w(rate increase increase_pure irate),
        do: Rollup.remove_counter_resets(@test_values, @test_timestamps, 0),
        else: @test_values

    Window.new(values, @test_timestamps, window: 130 - 5)
  end

  defp rollup_func(name, args \\ []) do
    {:ok, value} = Rollup.evaluate(name, args, fixture_window(name))
    value
  end

  defp assert_close(nil, nil, _context), do: :ok

  defp assert_close(actual, expected, context) when is_float(actual) and is_number(expected) do
    tolerance = if expected == 0, do: 1.0e-14, else: abs(expected) * 1.0e-13
    assert abs(actual - expected) <= tolerance, "#{context}: #{actual} != #{expected}"
  end

  defp assert_close(actual, expected, context),
    do: flunk("#{context}: #{inspect(actual)} != #{inspect(expected)}")

  defp rows(name, values, timestamps, config) do
    config = Map.merge(%{may_adjust_window: false}, config)

    {:ok, points} =
      Rollup.apply(
        name,
        [],
        %{timestamps: timestamps, values: Enum.map(values, &(&1 * 1.0))},
        config
      )

    points
  end

  defp assert_rows(points, expected_values, expected_timestamps) do
    assert Enum.map(points, &elem(&1, 0)) == expected_timestamps

    for {{_ts, actual}, expected} <- Enum.zip(points, expected_values) do
      assert_close(actual, expected, inspect(points))
    end
  end

  describe "TestRollupNewRollupFuncSuccess" do
    test "every ported function over the fixture" do
      for {name, expected} <- [
            {"default_rollup", 34},
            {"changes", 11},
            {"delta", 34},
            {"deriv", -266.85860231406093},
            {"deriv_fast", -712},
            {"idelta", 0},
            {"increase", 398},
            {"irate", 0},
            {"rate", 2200},
            {"resets", 5},
            {"range_over_time", 111},
            {"avg_over_time", 47.083333333333336},
            {"min_over_time", 12},
            {"max_over_time", 123},
            {"tmin_over_time", 0.08},
            {"tmax_over_time", 0.005},
            {"sum_over_time", 565},
            {"sum2_over_time", 37_951},
            {"geomean_over_time", 39.33466603189148},
            {"count_over_time", 12},
            {"stddev_over_time", 30.752935722554287},
            {"stdvar_over_time", 945.7430555555555},
            {"first_over_time", 123},
            {"last_over_time", 34},
            {"distinct_over_time", 8},
            {"ideriv", 0},
            {"increase_pure", 398},
            {"timestamp", 0.13},
            {"rate_over_sum", 4520},
            {"present_over_time", 1},
            {"absent_over_time", nil}
          ] do
        assert_close(rollup_func(name), expected, name)
      end
    end

    test "names are case-insensitive" do
      assert_close(rollup_func("RATE"), 2200, "RATE")
    end
  end

  test "TestRollupCountLEOverTime" do
    for {le, expected} <- [
          {-123, 0},
          {0, 0},
          {10, 0},
          {12, 1},
          {30, 2},
          {50, 9},
          {100, 11},
          {123, 12},
          {1000, 12}
        ] do
      assert_close(rollup_func("count_le_over_time", [le * 1.0]), expected, "le #{le}")
    end
  end

  test "TestRollupCountGTOverTime" do
    for {gt, expected} <- [
          {-123, 12},
          {0, 12},
          {10, 12},
          {12, 11},
          {30, 10},
          {50, 3},
          {100, 1},
          {123, 0},
          {1000, 0}
        ] do
      assert_close(rollup_func("count_gt_over_time", [gt * 1.0]), expected, "gt #{gt}")
    end
  end

  test "TestRollupCountEQOverTime" do
    for {eq, expected} <- [{-123, 0}, {0, 0}, {34, 4}, {123, 1}, {12, 1}] do
      assert_close(rollup_func("count_eq_over_time", [eq * 1.0]), expected, "eq #{eq}")
    end
  end

  test "TestRollupCountNEOverTime" do
    for {ne, expected} <- [{-123, 12}, {0, 12}, {34, 8}, {123, 11}, {12, 11}] do
      assert_close(rollup_func("count_ne_over_time", [ne * 1.0]), expected, "ne #{ne}")
    end
  end

  test "a NaN limit counts nothing, or everything for count_ne_over_time" do
    assert rollup_func("count_gt_over_time", [nil]) == 0.0
    assert rollup_func("count_ne_over_time", [nil]) == 12.0
  end

  test "TestRollupQuantileOverTime" do
    for {phi, expected} <- [
          {-123, -@inf},
          {-0.5, -@inf},
          {0, 12},
          {0.1, 22.1},
          {0.5, 34},
          {0.9, 94.50000000000001},
          {1, 123},
          {234, @inf}
        ] do
      assert_close(rollup_func("quantile_over_time", [phi * 1.0]), expected, "phi #{phi}")
    end

    assert rollup_func("quantile_over_time", [nil]) == nil
  end

  test "TestLinearRegression" do
    for {values, timestamps, {value, slope}} <- [
          {[1.0, 2.0], [100, 300], {1.5, 5}},
          {[2.0, 4.0, 6.0, 8.0, 10.0], [100, 200, 300, 400, 500], {4, 20}}
        ] do
      {actual_value, actual_slope} =
        Rollup.linear_regression(values, timestamps, hd(timestamps) + 100)

      assert_close(actual_value, value, "value")
      assert_close(actual_slope, slope, "slope")
    end

    assert Rollup.linear_regression([], [], 0) == {nil, nil}
  end

  test "TestRollupNewRollupFuncError: a wrong count of args, or a function not ported" do
    window = fixture_window("default_rollup")

    assert {:error, {:arity, _message}} = Rollup.evaluate("default_rollup", [1.0], window)
    assert {:error, {:arity, _message}} = Rollup.evaluate("quantile_over_time", [], window)

    assert {:error, {:unsupported, "rollup function holt_winters()"}} =
             Rollup.evaluate("holt_winters", [], window)
  end

  test "TestRollupIderivDuplicateTimestamps" do
    ideriv = fn values, timestamps, opts ->
      {:ok, value} = Rollup.evaluate("irate", [], Window.new(values, timestamps, opts))
      value
    end

    assert ideriv.([1.0, 2.0, 3.0, 4.0, 5.0], [100, 100, 200, 300, 300], []) == 20.0
    assert ideriv.([1.0, 2.0, 3.0, 4.0, 5.0], [100, 100, 300, 300, 300], []) == 15.0
    assert ideriv.([], [], []) == nil
    assert ideriv.([15.0], [100], []) == nil
    assert ideriv.([15.0], [100], prev_value: 10.0, prev_timestamp: 90) == 500.0
    assert ideriv.([15.0], [100], prev_value: 10.0, prev_timestamp: 100) == @inf
    assert ideriv.([15.0, 20.0], [100, 100], prev_value: 10.0, prev_timestamp: 100) == @inf
  end

  describe "TestRemoveCounterResets" do
    test "the fixture" do
      assert Rollup.remove_counter_resets([], [], 0) == []

      assert Rollup.remove_counter_resets(@test_values, @test_timestamps, 0) ==
               Enum.map([123, 157, 167, 188, 221, 255, 320, 332, 364, 396, 398, 398], &(&1 * 1.0))
    end

    test "negative values, which it does not expect" do
      assert Rollup.remove_counter_resets([-100.0, -200.0, -300.0, -400.0], [0, 1, 2, 3], 0) ==
               [-100.0, -100.0, -100.0, -100.0]
    end

    test "a partial reset adds only the fall" do
      assert Rollup.remove_counter_resets(
               [100.0, 95.0, 120.0, 119.0, 139.0, 50.0],
               [0, 1, 2, 3, 4, 5],
               0
             ) == [100.0, 100.0, 125.0, 125.0, 145.0, 195.0]
    end

    test "a gap past the staleness interval starts over" do
      values = Enum.map([10, 12, 14, 4, 6, 8, 6, 8, 4, 6], &(&1 * 1.0))
      timestamps = [10, 20, 30, 60, 70, 80, 90, 100, 120, 130]

      assert Rollup.remove_counter_resets(values, timestamps, 10) ==
               Enum.map([10, 12, 14, 4, 6, 8, 14, 16, 4, 6], &(&1 * 1.0))

      assert Rollup.remove_counter_resets([10.0, 12.0, 2.0, 4.0], [10, 20, 30, 60], 10) ==
               [10.0, 12.0, 14.0, 4.0]
    end

    test "the result never decreases, whatever the float error" do
      values = [
        34.094223,
        2.7518,
        2.140669,
        0.044878,
        1.887095,
        2.546569,
        2.490149,
        0.045,
        0.035684,
        0.062454,
        0.058296
      ]

      result = Rollup.remove_counter_resets(values, Enum.to_list(0..10), 0)
      assert result == Enum.sort(result)
    end
  end

  test "TestRollupDelta" do
    delta = fn prev, real_prev, real_next, values ->
      {:ok, value} =
        Rollup.evaluate(
          "delta",
          [],
          Window.new(Enum.map(values, &(&1 * 1.0)), Enum.to_list(1..length(values)//1),
            prev_value: prev,
            real_prev_value: real_prev,
            real_next_value: real_next
          )
        )

      value
    end

    for {prev, real_prev, real_next, values, expected} <- [
          {nil, nil, nil, [], nil},
          {nil, nil, nil, [1], 1},
          {nil, nil, nil, [10], 0},
          {nil, nil, nil, [100], 0},
          {nil, nil, nil, [1, 2, 3], 3},
          {1.0, nil, nil, [1, 2, 3], 2},
          {nil, nil, nil, [5, 6, 8], 8},
          {2.0, nil, nil, [5, 6, 8], 6},
          {nil, nil, nil, [100, 100], 0},
          {nil, nil, nil, [1000], 0},
          {nil, nil, nil, [1000, 1000], 0},
          {nil, nil, nil, [1000, 1001, 1002], 2},
          {nil, 900.0, nil, [1000], 100},
          {nil, 1000.0, nil, [1000], 0},
          {nil, 1100.0, nil, [1000], -100},
          {nil, 900.0, nil, [1000, 1001, 1002], 102},
          {nil, nil, 990.0, [1000], 0},
          {nil, nil, 1005.0, [1000], 0},
          {nil, nil, 800.0, [1000], 1000},
          {nil, nil, 1300.0, [1000], 1000},
          {1.0, nil, nil, [], 0},
          {100.0, nil, nil, [], 0}
        ] do
      assert_close(delta.(prev, real_prev, real_next, values), expected, inspect(values))
    end
  end

  describe "TestRollupNoWindowNoPoints" do
    test "beforeStart" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 4, step_ms: 1})
      |> assert_rows([nil, nil, nil, nil, nil], [0, 1, 2, 3, 4])
    end

    test "afterEnd" do
      "delta"
      |> rows(@test_values, @test_timestamps, %{start_ms: 120, end_ms: 148, step_ms: 4})
      |> assert_rows(
        [2, 0, 0, 0, nil, nil, nil, nil],
        [120, 124, 128, 132, 136, 140, 144, 148]
      )
    end
  end

  describe "TestRollupWindowNoPoints" do
    test "beforeStart" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 4, step_ms: 1, window_ms: 3})
      |> assert_rows([nil, nil, nil, nil, nil], [0, 1, 2, 3, 4])
    end

    test "afterEnd" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{
        start_ms: 161,
        end_ms: 191,
        step_ms: 10,
        window_ms: 3
      })
      |> assert_rows([nil, nil, nil, nil], [161, 171, 181, 191])
    end
  end

  describe "TestRollupNoWindowPartialPoints" do
    test "beforeStart" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 25, step_ms: 5})
      |> assert_rows([nil, 123, nil, 34, nil, 44], [0, 5, 10, 15, 20, 25])
    end

    test "afterEnd" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: 100, end_ms: 160, step_ms: 20})
      |> assert_rows([44, 32, 34, nil], [100, 120, 140, 160])
    end

    test "middle" do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: -50, end_ms: 150, step_ms: 50})
      |> assert_rows([nil, nil, 123, 34, 32], [-50, 0, 50, 100, 150])
    end
  end

  describe "TestRollupWindowPartialPoints" do
    test "beforeStart" do
      "last_over_time"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 20, step_ms: 5, window_ms: 8})
      |> assert_rows([nil, 123, 123, 34, 34], [0, 5, 10, 15, 20])
    end

    test "afterEnd" do
      "last_over_time"
      |> rows(@test_values, @test_timestamps, %{
        start_ms: 100,
        end_ms: 160,
        step_ms: 20,
        window_ms: 18
      })
      |> assert_rows([44, 34, 34, nil], [100, 120, 140, 160])
    end

    test "middle" do
      "last_over_time"
      |> rows(@test_values, @test_timestamps, %{
        start_ms: 0,
        end_ms: 150,
        step_ms: 50,
        window_ms: 19
      })
      |> assert_rows([nil, 54, 44, nil], [0, 50, 100, 150])
    end
  end

  test "TestRollupFuncsLookbackDelta" do
    for lookback <- [1, 7, 0] do
      "first_over_time"
      |> rows(@test_values, @test_timestamps, %{
        start_ms: 80,
        end_ms: 140,
        step_ms: 10,
        lookback_delta_ms: lookback
      })
      |> assert_rows([99, nil, 44, nil, 32, 34, nil], [80, 90, 100, 110, 120, 130, 140])
    end
  end

  describe "TestRollupFuncsNoWindow" do
    @grid [0, 40, 80, 120, 160]

    for {name, function, window, expected} <- [
          {"first", "first_over_time", 0, [nil, 123, 54, 44, 34]},
          {"count", "count_over_time", 0, [nil, 4, 4, 3, 1]},
          {"min", "min_over_time", 0, [nil, 21, 12, 32, 34]},
          {"max", "max_over_time", 0, [nil, 123, 99, 44, 34]},
          {"sum", "sum_over_time", 0, [nil, 222, 199, 110, 34]},
          {"delta", "delta", 0, [nil, 21, -9, 22, 0]},
          {"lag", "lag", 0, [nil, 0.004, 0, 0, 0.03]},
          {"lifetime_1", "lifetime", 0, [nil, 0.031, 0.044, 0.04, 0.01]},
          {"lifetime_2", "lifetime", 200, [nil, 0.031, 0.075, 0.115, 0.125]},
          {"scrape_interval_1", "scrape_interval", 0,
           [nil, 0.010333333333333333, 0.011, 0.013333333333333334, 0.01]},
          {"scrape_interval_2", "scrape_interval", 80,
           [nil, 0.010333333333333333, 0.010714285714285714, 0.012, 0.0125]},
          {"changes", "changes", 0, [nil, 4, 4, 3, 0]},
          {"resets", "resets", 0, [nil, 2, 2, 1, 0]},
          {"avg", "avg_over_time", 0, [nil, 55.5, 49.75, 36.666666666666664, 34]},
          {"deriv", "deriv", 0,
           [nil, -2879.310344827588, 127.87627310448904, -496.5831435079728, 0]},
          {"ideriv", "ideriv", 0, [nil, -1916.6666666666665, -43_500, 400, 0]},
          {"stddev", "stddev_over_time", 0,
           [nil, 39.81519810323691, 32.080952292598795, 5.2493385826745405, 0]},
          {"distinct_over_time_1", "distinct_over_time", 0, [nil, 4, 4, 3, 1]},
          {"distinct_over_time_2", "distinct_over_time", 80, [nil, 4, 7, 6, 3]},
          {"rate_over_sum", "rate_over_sum", 80, [nil, 2775, 5262.5, 3862.5, 1800]}
        ] do
      test name do
        unquote(function)
        |> rows(@test_values, @test_timestamps, %{
          start_ms: 0,
          end_ms: 160,
          step_ms: 40,
          window_ms: unquote(window)
        })
        |> assert_rows(unquote(expected), @grid)
      end
    end

    test "idelta" do
      "idelta"
      |> rows(@test_values, @test_timestamps, %{start_ms: 10, end_ms: 130, step_ms: 40})
      |> assert_rows([123, 33, -87, 0], [10, 50, 90, 130])
    end

    test "changes_small_window" do
      "changes"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 45, step_ms: 9, window_ms: 9})
      |> assert_rows([nil, 1, 1, 1, 1, 0], [0, 9, 18, 27, 36, 45])
    end

    test "deriv_fast" do
      "deriv_fast"
      |> rows(@test_values, @test_timestamps, %{start_ms: 0, end_ms: 20, step_ms: 4})
      |> assert_rows([nil, nil, nil, 0, -8900, 0], [0, 4, 8, 12, 16, 20])
    end
  end

  test "TestRollupBigNumberOfValues" do
    count = 10_000
    values = Enum.map(0..(count - 1), &(&1 * 1.0))
    timestamps = Enum.map(0..(count - 1), &div(&1, 2))

    "default_rollup"
    |> rows(values, timestamps, %{
      start_ms: 0,
      end_ms: count,
      step_ms: div(count, 5),
      window_ms: div(count, 4)
    })
    |> assert_rows([1, 4001, 8001, 9999, nil, nil], [0, 2000, 4000, 6000, 8000, 10_000])
  end

  describe "a gap between samples (TestRollupDeltaWithStaleness, TestRollupIncreasePureWithStaleness, TestRollupChangesWithStaleness)" do
    @gap_values [1, 1, 1, 1]
    @gap_timestamps [0, 15_000, 30_000, 70_000]

    for function <- ~w(delta increase_pure changes) do
      test "#{function}: a step longer than the gap keeps the value before it" do
        for lookback <- [0, 10_000] do
          unquote(function)
          |> rows(@gap_values, @gap_timestamps, %{
            start_ms: 0,
            end_ms: 70_000,
            step_ms: 45_000,
            lookback_delta_ms: lookback
          })
          |> assert_rows([1, 0], [0, 45_000])
        end
      end

      test "#{function}: a shorter step keeps it only within the lookback delta" do
        "#{unquote(function)}"
        |> rows(@gap_values, @gap_timestamps, %{
          start_ms: 0,
          end_ms: 70_000,
          step_ms: 10_000,
          lookback_delta_ms: 30_000
        })
        |> assert_rows([1, 0, 0, 0, 0, 0, 0, 1], Enum.to_list(0..70_000//10_000))
      end
    end

    for function <- ~w(delta increase_pure) do
      test "#{function}: no lookback delta ignores staleness" do
        unquote(function)
        |> rows(@gap_values, @gap_timestamps, %{start_ms: 0, end_ms: 70_000, step_ms: 10_000})
        |> assert_rows([1, 0, 0, 0, 0, 0, 0, 0], Enum.to_list(0..70_000//10_000))
      end
    end

    test "delta: issue-8935, the lookback delta counts from the window's first sample" do
      "delta"
      |> rows([50, 50, 1, 1], [0, 10_000, 70_000, 80_000], %{
        start_ms: 0,
        end_ms: 90_000,
        step_ms: 30_000,
        lookback_delta_ms: 55_000
      })
      |> assert_rows([0, 0, 0, 1], [0, 30_000, 60_000, 90_000])
    end

    test "changes: issue-10280, a gap past the scrape interval uses the real previous value" do
      "changes"
      |> rows([1, 1, 1, 1, 1, 1, 2], [0, 30_000, 40_000, 50_000, 60_000, 70_000, 100_000], %{
        start_ms: 0,
        end_ms: 100_000,
        step_ms: 10_000
      })
      |> assert_rows([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], Enum.to_list(0..100_000//10_000))
    end
  end

  describe "rate" do
    test "is the rise over the time since the sample before the window, not extrapolated" do
      points =
        rows("rate", [0, 60, 120, 180, 240], [0, 15_000, 30_000, 45_000, 60_000], %{
          start_ms: 30_000,
          end_ms: 60_000,
          step_ms: 30_000,
          window_ms: 30_000
        })

      assert points == [{30_000, 4.0}, {60_000, 4.0}]
    end

    test "adds back a counter reset" do
      points =
        rows("rate", [100, 110, 5, 15], [0, 10_000, 20_000, 30_000], %{
          start_ms: 30_000,
          end_ms: 30_000,
          step_ms: 30_000,
          window_ms: 30_000
        })

      assert points == [{30_000, (125.0 - 100.0) / 30}]
    end

    test "with no window written widens to the scrape interval for a range query" do
      timestamps = Enum.to_list(0..600_000//60_000)
      values = Enum.map(timestamps, &(&1 / 1000))

      points =
        "rate"
        |> Rollup.apply([], %{timestamps: timestamps, values: values}, %{
          start_ms: 300_000,
          end_ms: 600_000,
          step_ms: 15_000
        })
        |> elem(1)

      assert Enum.all?(points, fn {_t, value} -> value == 1.0 end)
    end
  end

  describe "grid/4" do
    test "runs from start by step up to end" do
      assert Rollup.grid(0, 10, 4, nil) == {:ok, [0, 4, 8]}
      assert Rollup.grid(5, 5, 4, nil) == {:ok, [5]}
    end

    test "refuses more points than the limit, a bad step and an inverted range" do
      assert {:error, {:too_many_points, message}} = Rollup.grid(0, 100, 1, 50)
      assert message =~ "101; the maximum number of points is 50"
      assert {:error, {:invalid_grid, _message}} = Rollup.grid(0, 100, 0, 50)
      assert {:error, {:invalid_grid, _message}} = Rollup.grid(10, 0, 1, 50)
    end
  end

  test "scrape_interval/3 and inflate/1 port getScrapeInterval and getMaxPrevInterval" do
    assert Rollup.scrape_interval({}, 0, 7) == 7

    assert Rollup.scrape_interval(List.to_tuple(Enum.to_list(0..100_000//10_000)), 11, 7) ==
             10_000

    assert Rollup.inflate(1_000) == 5_000
    assert Rollup.inflate(15_000) == 22_500
    assert Rollup.inflate(60_000) == 67_500
  end

  test "the function table" do
    assert "rate" in Rollup.functions()
    assert Rollup.supported?("Rate")
    refute Rollup.supported?("holt_winters")
    assert Rollup.keeps_metric_name?("max_over_time")
    refute Rollup.keeps_metric_name?("rate")
    assert Rollup.may_adjust_window?("rate")
    refute Rollup.may_adjust_window?("sum_over_time")
    assert Rollup.series_arg_index("quantile_over_time") == 1
    assert Rollup.series_arg_index("count_gt_over_time") == 0
    assert Rollup.quantile(0.5, [3.0, 1.0, 2.0]) == 2.0
  end

  test "a sum past the largest double answers +Inf" do
    points =
      rows("sum_over_time", [@inf, @inf], [1, 2], %{
        start_ms: 2,
        end_ms: 2,
        step_ms: 1,
        window_ms: 5
      })

    assert points == [{2, @inf}]
  end

  describe "±Inf is sticky, as a float64 infinity is in VictoriaMetrics" do
    @one %{start_ms: 2, end_ms: 2, step_ms: 1, window_ms: 5}

    test "avg, sum, sum2, range and rate over an infinity" do
      assert rows("avg_over_time", [@inf, 5], [1, 2], @one) == [{2, @inf}]
      assert rows("avg_over_time", [-@inf, 5], [1, 2], @one) == [{2, -@inf}]
      assert rows("sum_over_time", [@inf, -@inf], [1, 2], @one) == [{2, nil}]
      assert rows("sum2_over_time", [@inf, 1], [1, 2], @one) == [{2, @inf}]
      assert rows("range_over_time", [-@inf, @inf], [1, 2], @one) == [{2, @inf}]
      assert rows("delta", [@inf, @inf], [1, 2], @one) == [{2, nil}]
      assert rows("idelta", [5, @inf], [1, 2], @one) == [{2, @inf}]
    end

    test "quantile interpolates an infinity as one" do
      assert Rollup.quantile(0.5, [@inf, 5.0]) == @inf
      assert Rollup.quantile(0.5, [-@inf, @inf]) == nil
    end

    test "stdvar, stddev and deriv over an infinity are NaN" do
      assert rows("stdvar_over_time", [@inf, 5], [1, 2], @one) == [{2, nil}]
      assert rows("stddev_over_time", [@inf, 5], [1, 2], @one) == [{2, nil}]
      assert rows("deriv", [@inf, 5], [1, 2], @one) == [{2, nil}]
      assert rows("stdvar_over_time", [1, 3], [1, 2], @one) == [{2, 1.0}]
    end

    test "geomean over an infinity is the infinity" do
      assert rows("geomean_over_time", [@inf, 4], [1, 2], @one) == [{2, @inf}]
    end
  end

  describe "scalar arguments are read at each point" do
    test "a list of one value per point is read at that point's index" do
      samples = %{timestamps: [0, 10, 20, 30], values: [1.0, 5.0, 9.0, 13.0]}
      config = %{start_ms: 10, end_ms: 30, step_ms: 10, window_ms: 30, may_adjust_window: false}

      assert Rollup.apply("count_gt_over_time", [[0.0, 6.0, 12.0]], samples, config) ==
               {:ok, [{10, 2.0}, {20, 1.0}, {30, 1.0}]}

      assert Rollup.apply("count_gt_over_time", [4.0], samples, config) ==
               {:ok, [{10, 1.0}, {20, 2.0}, {30, 3.0}]}

      assert Rollup.apply("quantile_over_time", [[0.0, 1.0, nil]], samples, config) ==
               {:ok, [{10, 1.0}, {20, 9.0}, {30, nil}]}
    end
  end
end
