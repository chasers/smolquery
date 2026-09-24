defmodule SmolqueryVictoriaMetrics.PushdownTest do
  @moduledoc """
  `SmolqueryVictoriaMetrics.Pushdown`: which aggregates are planned in SQL,
  the SQL they become, how a frame reads back, and, over the stack, that a
  pushed aggregate answers byte for byte what the Elixir evaluator answers.
  """
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.Pushdown
  alias SmolqueryVictoriaMetrics.Runtime

  @moduletag :capture_log
  @inf 1.797_693_134_862_315_7e308
  @t0 1_789_812_000
  @t0_ms @t0 * 1000

  @context %{
    start_ms: 0,
    end_ms: 60_000,
    step_ms: 30_000,
    lookback_ms: 300_000,
    timestamps: [0, 30_000, 60_000]
  }

  defp plan(query) do
    {:ok, expr} = MetricsQL.parse(query)
    Pushdown.plan(expr, @context)
  end

  defp plan!(query) do
    {:ok, plan} = plan(query)
    plan
  end

  describe "plan/2" do
    test "an aggregate by labels over a rollup of a selector" do
      assert %Pushdown{
               rollup: "default_rollup",
               aggregate: "sum",
               window_ms: 300_000,
               labels: ["job"],
               name_in_key: false,
               timestamps: [0, 30_000, 60_000]
             } = plan!(~s|sum by (job) (m{a="1"}[5m])|)

      assert %Pushdown{rollup: "avg_over_time", aggregate: "count", labels: []} =
               plan!("count(avg_over_time(m[1m]))")
    end

    test "a window not written is max(step, lookback) for default_rollup, the step otherwise" do
      assert %Pushdown{window_ms: 300_000} = plan!("sum(m)")
      assert %Pushdown{window_ms: 300_000} = plan!("sum(default_rollup(m))")
      assert %Pushdown{window_ms: 30_000} = plan!("sum(sum_over_time(m))")
      assert %Pushdown{window_ms: 60_000} = plan!("min(last_over_time(m[2i]))")
    end

    test "__name__ is a key only when by lists it and the rollup keeps it" do
      assert %Pushdown{name_in_key: true, labels: ["job"]} = plan!("max by (__name__, job) (m)")
      assert %Pushdown{name_in_key: false} = plan!("sum by (__name__) (sum_over_time(m[1m]))")

      assert %Pushdown{name_in_key: true} =
               plan!("sum by (__name__) (sum_over_time(m[1m]) keep_metric_names)")

      assert %Pushdown{labels: ["a", "b"]} = plan!("sum by (a, b, a) (m)")
    end

    test "the rate family reads the fetch's range, and widens an unwritten window per series" do
      assert %Pushdown{
               rollup: "rate",
               window_ms: 300_000,
               adjust_window: false,
               read_from_ms: -600_000
             } = plan!("sum by (job) (rate(m[5m]))")

      assert %Pushdown{
               rollup: "rate",
               window_ms: 30_000,
               adjust_window: true,
               read_from_ms: -330_000
             } =
               plan!("sum(rate(m))")

      assert %Pushdown{rollup: "increase", window_ms: 30_000, adjust_window: false} =
               plan!("sum(increase(m))")

      assert %Pushdown{rollup: "irate", adjust_window: true} = plan!("max(irate(m))")

      assert %Pushdown{rollup: "first_over_time", window_ms: 60_000} =
               plan!("count(first_over_time(m[1m]))")

      assert %Pushdown{rollup: "present_over_time", window_ms: 900_000} =
               plan!("sum(present_over_time(m[15m]))")

      assert plan("sum(present_over_time(m[1d]))") == :none
      assert plan("sum(rate(m[1h]))") == :none
    end

    test "offset moves the grid and the read back, and the answer forward again" do
      assert %Pushdown{
               offset_ms: 60_000,
               start_ms: -60_000,
               end_ms: 0,
               timestamps: [-60_000, -30_000, 0],
               read_from_ms: -360_000
             } = plan!("sum(m offset 1m)")

      assert %Pushdown{offset_ms: -30_000, start_ms: 30_000, read_from_ms: -570_000} =
               plan!("sum(rate(m[5m] offset -30s))")

      assert %Pushdown{offset_ms: 60_000} = plan!("sum(last_over_time(m[5m] offset 1m))")
    end

    test "a rollup other than the last sample is pushed only up to 32 steps of window" do
      assert %Pushdown{window_ms: 960_000} = plan!("sum(sum_over_time(m[16m]))")
      assert plan("sum(sum_over_time(m[16m1s]))") == :none
      assert plan("avg(avg_over_time(m[1d]))") == :none
      assert %Pushdown{window_ms: 86_400_000} = plan!("sum(last_over_time(m[1d]))")
      assert %Pushdown{window_ms: 86_400_000} = plan!("sum(m[1d])")
    end

    test "what stays in Elixir" do
      for query <- [
            "sum without (job) (m)",
            "topk(3, m)",
            "sum(m @ 100)",
            "sum(m[5m:1m])",
            "sum(m[5m:])",
            "sum(m) limit 2",
            "sum(m + 1)",
            "quantile(0.9, m)",
            "sum(m[5m-10m])",
            "sum(rate(m[5m:1m]))",
            "sum(changes(m[5m]))",
            "sum(m, m)"
          ] do
        assert plan(query) == :none, query
      end
    end
  end

  describe "sql/2" do
    test "binds the labels, then the grid and the window, after the predicate's parameters" do
      runtime = Runtime.new(name: :pushdown_sql_test, password: "pw")
      plan = plan!(~s|sum by (job, __name__) (m{a="1"})|)

      assert {:ok, sql, params} = Pushdown.sql(plan, runtime)

      assert ["m", %NaiveDateTime{}, %NaiveDateTime{}, "a", "1", "job", 0, 30_000, 300_000, 3] =
               params

      assert sql =~
               "SELECT series, name, labels[$6] AS k0, epoch_ms(ts) AS ts_ms, CASE WHEN value"

      assert sql =~
               ~s|FROM "metrics"."samples" WHERE name = $1 AND ts BETWEEN $2 AND $3 AND labels[$4] = $5|

      assert sql =~ "e AS (SELECT series, name, k0, value, unnest(generate_series("
      assert sql =~ "greatest(0, CAST(ceil((ts_ms - $7) / $8) AS BIGINT))"

      assert sql =~
               "least($10 - 1, CAST(floor((ts_ms + $9 - 1 - $7) / $8) AS BIGINT), " <>
                 "CAST(floor((coalesce(next_ts, ts_ms + $9) - 1 - $7) / $8) AS BIGINT))"

      assert sql =~
               "FROM (SELECT *, lead(ts_ms) OVER (PARTITION BY series ORDER BY ts_ms, value) AS next_ts FROM s))"

      assert sql =~
               "a AS (SELECT k, name, k0, sum(value) FILTER (WHERE NOT isnan(value)) AS value FROM e GROUP BY k, name, k0)"

      assert sql =~ "c AS (SELECT count(*) AS samples FROM s)"
      assert sql =~ "SELECT $7 + a.k * $8 AS t, a.*, c.samples FROM c LEFT JOIN a ON true"
      refute sql =~ "r AS"
    end

    test "a rollup other than the last sample is unnested to its window alone" do
      runtime = Runtime.new(name: :pushdown_sql_window_test, password: "pw")
      assert {:ok, sql, _params} = Pushdown.sql(plan!("avg(avg_over_time(m[1m]))"), runtime)

      assert sql =~
               "least($7 - 1, CAST(floor((ts_ms + $6 - 1 - $4) / $5) AS BIGINT)))) AS k FROM s)"

      refute sql =~ "lead("
    end

    test "pairs that compose are one stage; the rest group by series first" do
      runtime = Runtime.new(name: :pushdown_sql_stages_test, password: "pw")

      one = fn query ->
        {:ok, sql, _params} = Pushdown.sql(plan!(query), runtime)
        refute sql =~ "r AS", query
        sql
      end

      assert one.("count(count_over_time(m[1m]))") =~
               "a AS (SELECT k, count(DISTINCT series)::DOUBLE AS value FROM e GROUP BY k)"

      assert one.("count(min_over_time(m[1m]))") =~ "count(DISTINCT series)::DOUBLE AS value"
      assert one.("sum(count_over_time(m[1m]))") =~ "a AS (SELECT k, count(*)::DOUBLE AS value"
      assert one.("min(min_over_time(m[1m]))") =~ "a AS (SELECT k, min(value) AS value"
      assert one.("max by (job) (max_over_time(m[1m]))") =~ "SELECT k, k0, max(value) AS value"

      assert one.("count(last_over_time(m[1m]))") =~
               "nullif(count(value) FILTER (WHERE NOT isnan(value)), 0)::DOUBLE AS value"

      two = fn query ->
        {:ok, sql, _params} = Pushdown.sql(plan!(query), runtime)
        sql
      end

      assert two.("sum(sum_over_time(m[1m]))") =~
               "r AS (SELECT k, series, sum(value) AS v FROM e GROUP BY k, series), " <>
                 "a AS (SELECT k, sum(v) FILTER (WHERE NOT isnan(v)) AS value FROM r GROUP BY k)"

      assert two.("count by (job) (sum_over_time(m[1m]))") =~
               "a AS (SELECT k, k0, nullif(count(v) FILTER (WHERE NOT isnan(v)), 0)::DOUBLE AS value FROM r GROUP BY k, k0)"

      assert two.("min(count_over_time(m[1m]))") =~ "count(value)::DOUBLE AS v"
      assert two.("avg(max_over_time(m[1m]))") =~ "max(value) AS v"
    end
  end

  describe "series/3" do
    test "one series per group, nil where a point has no value, infinities and NaN read back" do
      frame =
        DataFrame.new(
          t: [0, 30_000, 0, 60_000, nil],
          name: ["m", "m", "m", nil, nil],
          k0: ["a", "a", nil, nil, nil],
          value: [1.0, :infinity, :nan, :neg_infinity, nil],
          samples: [5, 5, 5, 5, 5]
        )

      plan = %{plan!("sum by (__name__, job) (m)") | labels: ["job"], name_in_key: true}

      assert {:ok, series, %{series: 3, samples: 5}} = Pushdown.series(frame, plan, 10)

      assert Series.sort(series) == [
               %Series{labels: %{}, values: [{0, nil}, {30_000, nil}, {60_000, -@inf}]},
               %Series{
                 labels: %{"__name__" => "m"},
                 values: [{0, nil}, {30_000, nil}, {60_000, nil}]
               },
               %Series{
                 labels: %{"__name__" => "m", "job" => "a"},
                 values: [{0, 1.0}, {30_000, @inf}, {60_000, nil}]
               }
             ]

      assert Pushdown.series(frame, plan, 2) == {:error, {:too_many_series, 2}}
      assert Pushdown.series(nil, plan, 2) == {:ok, [], %{series: 0, samples: 0}}
    end
  end

  describe "over the stack" do
    @describetag :tmp_dir
    @describetag timeout: 600_000

    setup context do
      stack = VictoriaMetricsStack.start(context)
      every_15s = for i <- 0..20, do: {@t0_ms + i * 15_000, 1}
      gappy = for i <- 0..20, rem(i, 3) != 0, do: {@t0_ms + i * 15_000, i * 1.0}

      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "up", "job" => "a"}, every_15s},
          {%{"__name__" => "up", "job" => "b"}, gappy},
          {%{"__name__" => "up"}, for(i <- 0..6, do: {@t0_ms + i * 45_000, 2.0})},
          {%{"__name__" => "gauge", "job" => "a"},
           [
             {@t0_ms, 0.1},
             {@t0_ms + 15_000, 1.0e21},
             {@t0_ms + 30_000, @inf},
             {@t0_ms + 60_000, -@inf},
             {@t0_ms + 75_000, 3.0}
           ]},
          {%{"__name__" => "gauge", "job" => "b"},
           [{@t0_ms + 30_000, 5.0}, {@t0_ms + 30_000, 7.0}]},
          {%{"__name__" => "up", "job" => "c"},
           for(i <- 0..20, do: {@t0_ms + 7_000 + i * 15_000, 3.0})},
          {%{"__name__" => "up", "job" => "d"},
           for(i <- 0..8, do: {@t0_ms + 4_000 + i * 37_000, i * 1.0})},
          {%{"__name__" => "c", "job" => "a"},
           for(
             i <- 0..20,
             do: {@t0_ms + i * 15_000, if(i < 10, do: 6.0 * i, else: 3.0 + 6 * (i - 10))}
           )},
          {%{"__name__" => "c", "job" => "b"},
           Enum.with_index(
             [100.0, 110.0, 120.0, 118.0, 130.0, 145.0, 150.0, 20.0, 25.0, 40.0],
             fn v, i ->
               {@t0_ms + i * 15_000, v}
             end
           )},
          {%{"__name__" => "c", "job" => "c"},
           [
             {@t0_ms, 1.0},
             {@t0_ms + 60_000, 8.0},
             {@t0_ms + 61_000, 9.0},
             {@t0_ms + 200_000, 30.0},
             {@t0_ms + 290_000, 31.0}
           ]},
          {%{"__name__" => "c", "job" => "d"}, [{@t0_ms + 100_000, 42.0}]},
          {%{"__name__" => "c", "job" => "e"},
           [
             {@t0_ms + 30_000, 5.0},
             {@t0_ms + 30_000, 7.0},
             {@t0_ms + 45_000, 9.0},
             {@t0_ms + 45_000, 9.0}
           ]},
          {%{"__name__" => "c", "job" => "f"}, for(i <- 0..20, do: {@t0_ms + i * 15_000, 4.0})},
          {%{"__name__" => "c", "job" => "g"},
           [
             {@t0_ms, 1.0},
             {@t0_ms + 15_000, @inf},
             {@t0_ms + 30_000, 5.0},
             {@t0_ms + 60_000, 6.0}
           ]},
          {%{"__name__" => "c", "job" => "h"},
           for(i <- 0..12, do: {@t0_ms + 2_000 + i * 23_000, 1000.0 + i * i * 1.0})},
          {%{"__name__" => "c", "job" => "i"},
           [{@t0_ms + 50_000, @inf}, {@t0_ms + 200_000, 3.0}]},
          {%{"__name__" => "c", "job" => "j"},
           [{@t0_ms + 50_000, 1.0e300}, {@t0_ms + 65_000, 2.0}]}
        ])

      %{stack: stack, elixir: limited(stack, pushdown: false)}
    end

    @queries [
      "sum(up)",
      "sum by (job) (up)",
      "count by (job) (up[1m])",
      "max by (__name__, job) (last_over_time(up[30s]))",
      "avg(avg_over_time(gauge[1m]))",
      "sum(sum_over_time(gauge[1m]))",
      "min by (job) (min_over_time(up[45s]))",
      "count(count_over_time(up[1m]))",
      "sum by (job, __name__) (sum_over_time(up[1m]) keep_metric_names)",
      ~s|sum by (job) (up{job="a" or job="b"})|,
      "sum(nothing)",
      "sum by (job) (max_over_time(up[2m]))",
      "avg by (job) (gauge)",
      "max(gauge[1m])",
      "sum by (job) (rate(c[1m]))",
      "sum by (job) (rate(c))",
      "max by (job) (increase(c[2m]))",
      "sum by (job) (increase(c))",
      "sum by (job) (increase_pure(c[1m]))",
      "sum by (job) (delta(c[1m]))",
      "sum by (job) (delta(c))",
      "sum by (job) (idelta(c[1m]))",
      "sum by (job) (irate(c[1m]))",
      "sum by (job) (irate(c))",
      "avg by (job) (ideriv(c[45s]))",
      "avg by (job) (deriv_fast(c[1m]))",
      "count by (job) (first_over_time(c[1m]))",
      "sum by (job) (present_over_time(c[30s]))",
      "sum by (job) (rate(c[1m] offset 30s))",
      "sum by (job) (rate(c[2m] offset -15s))",
      "sum(up offset 1m)",
      "sum(rate(c[5m]))",
      "count(rate(c[1m]))"
    ]

    test "a pushed aggregate answers what the evaluator answers, over a range and at an instant",
         %{stack: stack, elixir: elixir} do
      for query <- @queries do
        assert {:ok, _plan} = plan(query), query

        range = %{"query" => query, "start" => "#{@t0}", "end" => "#{@t0 + 300}", "step" => "15s"}

        off_grid = %{
          "query" => query,
          "start" => "#{@t0 + 11}",
          "end" => "#{@t0 + 250}",
          "step" => "13s"
        }

        instant = %{"query" => query, "time" => "#{@t0 + 100}"}
        odd_instant = %{"query" => query, "time" => "#{@t0 + 101}"}

        for params <- [range, off_grid] do
          assert {query, params, answer(stack, "/api/v1/query_range", params)} ==
                   {query, params, answer(elixir, "/api/v1/query_range", params)}
        end

        for params <- [instant, odd_instant] do
          assert {query, params, answer(stack, "/api/v1/query", params)} ==
                   {query, params, answer(elixir, "/api/v1/query", params)}
        end
      end
    end

    test "past max_series is 422", %{stack: stack} do
      one = limited(stack, max_series: 1)

      response =
        get(one, "/api/v1/query", %{"query" => "sum by (job) (up)", "time" => "#{@t0 + 100}"})

      assert response.status == 422
      assert body(response)["error"] =~ "more than 1 series"
    end

    test "max_samples does not apply, and what was scanned is reported", %{stack: stack} do
      handler = "pushdown-test-#{stack.name}"
      test = self()

      :telemetry.attach(
        handler,
        [:smolquery, :victoriametrics, :query],
        fn _event, measurements, _meta, _config -> send(test, {:query, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      small = limited(stack, max_samples: 1, max_samples_per_query: 1)

      response =
        get(small, "/api/v1/query", %{"query" => "sum by (job) (up)", "time" => "#{@t0 + 100}"})

      assert response.status == 200
      assert_receive {:query, %{series: 5, samples: samples, fetch_us: fetch}}
      assert samples > 5 and fetch > 0

      none = get(small, "/api/v1/query", %{"query" => "sum(up)", "time" => "#{@t0 - 1_000}"})
      assert body(none)["data"]["result"] == []
      assert_receive {:query, %{series: 0, samples: 0}}

      before =
        get(small, "/api/v1/query", %{"query" => ~s|sum(up{job="a"})|, "time" => "#{@t0 + 600}"})

      assert body(before)["data"]["result"] == []
      assert_receive {:query, %{series: 0, samples: 1}}
    end
  end

  defp answer(stack, path, params) do
    response = get(stack, path, params)
    assert response.status == 200, response.resp_body
    response |> body() |> Map.delete("stats")
  end

  defp get(stack, path, params),
    do: VictoriaMetricsStack.request(stack, :get, path <> "?" <> URI.encode_query(params))

  defp body(response), do: JSON.decode!(response.resp_body)

  defp limited(stack, limits) do
    name = :"#{stack.name}_limited_#{:erlang.unique_integer([:positive])}"
    Runtime.put(struct!(%{stack.runtime | name: name}, limits))
    on_exit(fn -> Runtime.delete(name) end)
    %{stack | name: name}
  end
end
