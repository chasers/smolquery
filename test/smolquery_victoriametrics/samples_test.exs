defmodule SmolqueryVictoriaMetrics.SamplesTest do
  use ExUnit.Case, async: false

  alias Smolquery.QueryService.Client
  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.Samples

  @moduletag :capture_log

  @from 1_789_812_000_000
  @to 1_789_815_600_000
  @from_time ~N[2026-09-19 10:00:00.000000]
  @to_time ~N[2026-09-19 11:00:00.000000]

  defp selector(text) do
    {:ok, expr} = MetricsQL.parse(text)
    expr
  end

  defp where(text), do: Samples.where(selector(text), @from, @to)

  describe "where/3: one filter set" do
    test "a metric name is `name = $1`, first, then the time range" do
      assert where("up") ==
               {:ok, "name = $1 AND ts BETWEEN $2 AND $3", ["up", @from_time, @to_time]}
    end

    test "each label matcher, with the label's name bound too" do
      assert where(~s|up{job="api"}|) ==
               {:ok, "name = $1 AND ts BETWEEN $2 AND $3 AND labels[$4] = $5",
                ["up", @from_time, @to_time, "job", "api"]}

      assert where(~s|up{job=""}|) ==
               {:ok, "name = $1 AND ts BETWEEN $2 AND $3 AND coalesce(labels[$4], '') = $5",
                ["up", @from_time, @to_time, "job", ""]}

      assert where(~s|up{job!="api"}|) ==
               {:ok, "name = $1 AND ts BETWEEN $2 AND $3 AND coalesce(labels[$4], '') <> $5",
                ["up", @from_time, @to_time, "job", "api"]}

      assert where(~s|up{job=~"a.*"}|) ==
               {:ok,
                "name = $1 AND ts BETWEEN $2 AND $3 AND " <>
                  "regexp_full_match(coalesce(labels[$4], ''), $5)",
                ["up", @from_time, @to_time, "job", "a.*"]}

      assert where(~s|up{job!~"a.*"}|) ==
               {:ok,
                "name = $1 AND ts BETWEEN $2 AND $3 AND " <>
                  "NOT regexp_full_match(coalesce(labels[$4], ''), $5)",
                ["up", @from_time, @to_time, "job", "a.*"]}
    end

    test "a name matched otherwise than by `=` is no pruning conjunct" do
      assert where(~s({__name__=~"up|down"})) ==
               {:ok, "ts BETWEEN $1 AND $2 AND regexp_full_match(name, $3)",
                [@from_time, @to_time, "up|down"]}

      assert where(~s|{__name__!="up", job="a"}|) ==
               {:ok, "ts BETWEEN $1 AND $2 AND name <> $3 AND labels[$4] = $5",
                [@from_time, @to_time, "up", "job", "a"]}

      assert where(~s|{__name__!~"up", job="a"}|) ==
               {:ok,
                "ts BETWEEN $1 AND $2 AND NOT regexp_full_match(name, $3) AND labels[$4] = $5",
                [@from_time, @to_time, "up", "job", "a"]}
    end

    test "a time before the epoch is the epoch" do
      assert {:ok, _sql, [_name, ~N[1970-01-01 00:00:00.000000], _to]} =
               Samples.where(selector("up"), -5_000, @to)
    end
  end

  describe "where/3: `or` filter sets" do
    test "one shared name stays a top-level conjunct, the rest is an OR" do
      assert where(~s|up{job="a" or instance="b"}|) ==
               {:ok,
                "name = $1 AND ts BETWEEN $2 AND $3 AND " <>
                  "((labels[$4] = $5) OR (labels[$6] = $7))",
                ["up", @from_time, @to_time, "job", "a", "instance", "b"]}
    end

    test "a set left with no matcher but the name makes the OR always true" do
      assert where(~s|{__name__="up" or __name__="up", job="a"}|) ==
               {:ok, "name = $1 AND ts BETWEEN $2 AND $3", ["up", @from_time, @to_time]}
    end

    test "different names sit inside the OR" do
      assert where(~s|{__name__="up", job="a" or __name__="down"}|) ==
               {:ok, "ts BETWEEN $1 AND $2 AND ((name = $3 AND labels[$4] = $5) OR (name = $6))",
                [@from_time, @to_time, "up", "job", "a", "down"]}
    end
  end

  test "a selector with no non-empty matcher is refused before any SQL" do
    for text <- [
          ~s|{job=""}|,
          ~s|{job=~".*"}|,
          ~s|{job!="x"}|,
          ~s|{job!~"x"}|,
          ~s|{a="1" or job=""}|
        ] do
      assert {:error, {:empty_selector, message}} = where(text), text
      assert message == "vector selector must contain at least one non-empty matcher"
    end

    assert {:error, {:empty_selector, _message}} =
             Samples.where(
               %SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr{filter_sets: []},
               0,
               1
             )

    assert {:ok, _sql, _params} = where(~s|{job=~".+"}|)
  end

  describe "against a buffer, an ingest service and a query service" do
    @describetag :tmp_dir

    @day 86_400_000
    @hour 3_600_000

    setup context do
      stack = VictoriaMetricsStack.start(context)

      at = fn offset -> for s <- 0..3, do: {@from + offset + s * 15_000, s * 1.0} end

      :ok = VictoriaMetricsStack.write(stack, [{%{"__name__" => "up", "job" => "a"}, at.(0)}])

      :ok =
        VictoriaMetricsStack.write(stack, [{%{"__name__" => "up", "job" => "b"}, at.(60_000)}])

      :ok = VictoriaMetricsStack.write(stack, [{%{"__name__" => "down", "job" => "a"}, at.(0)}])
      :ok = VictoriaMetricsStack.write(stack, [{%{"__name__" => "up", "job" => "a"}, at.(@day)}])

      %{stack: stack}
    end

    test "series/4 names each matching series once, labels folded", %{stack: stack} do
      assert {:ok, series} = Samples.series(stack.runtime, selector("up"), {@from, @from + @hour})

      assert series |> Map.values() |> Enum.sort_by(& &1.labels) == [
               %{name: "up", labels: %{"job" => "a"}},
               %{name: "up", labels: %{"job" => "b"}}
             ]
    end

    test "fetch/4 groups the samples by series, in time order", %{stack: stack} do
      assert {:ok, samples} =
               Samples.fetch(stack.runtime, selector(~s|up{job="b"}|), {@from, @from + @hour})

      assert Map.values(samples) == [
               {[@from + 60_000, @from + 75_000, @from + 90_000, @from + 105_000],
                [0.0, 1.0, 2.0, 3.0]}
             ]
    end

    test "select/4 joins them, __name__ among the labels", %{stack: stack} do
      assert {:ok, [%{labels: labels, timestamps: timestamps, values: values}]} =
               Samples.select(
                 stack.runtime,
                 selector(~s|{__name__=~"do.*"}|),
                 {@from, @from + @hour}
               )

      assert labels == %{"__name__" => "down", "job" => "a"}
      assert [_first, _second, _third, _fourth] = timestamps
      assert values == [0.0, 1.0, 2.0, 3.0]
    end

    test "a query for one metric over one hour opens only that metric's hour", %{stack: stack} do
      for build <- [&Samples.series_query/3, &Samples.samples_query/3] do
        {:ok, sql, params} =
          build.(stack.runtime, selector(~s|up{job="a"}|), {@from, @from + @hour})

        assert {:ok, %{state: :done} = job, frame} =
                 Client.query(stack.query, sql, params: params)

        assert Explorer.DataFrame.n_rows(frame) >= 1
        assert job.statistics.hot.files_total == 4
        assert job.statistics.hot.files_scanned == 2
      end
    end

    test "the ceilings refuse one row past them", context do
      stack = VictoriaMetricsStack.start(context, max_series: 1, max_samples: 7)

      :ok =
        VictoriaMetricsStack.write(stack, [{%{"__name__" => "up", "job" => "a"}, [{@from, 1.0}]}])

      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "up", "job" => "b"}, for(s <- 1..7, do: {@from + s, 1.0})}
        ])

      range = {@from, @from + @hour}

      assert Samples.series(stack.runtime, selector("up"), range) ==
               {:error, {:too_many_series, 1}}

      assert {:ok, _one} = Samples.series(stack.runtime, selector(~s|up{job="a"}|), range)

      assert Samples.fetch(stack.runtime, selector("up"), range) ==
               {:error, {:too_many_samples, 7}}

      assert {:ok, _seven} = Samples.fetch(stack.runtime, selector(~s|up{job="b"}|), range)
    end

    test "a table that does not exist yet selects nothing", context do
      stack = VictoriaMetricsStack.start(context, table: "empty.samples")

      assert Samples.select(stack.runtime, selector("up"), {@from, @to}) == {:ok, []}
      assert Samples.fetch(stack.runtime, selector("up"), {@from, @to}) == {:ok, %{}}
    end
  end
end
