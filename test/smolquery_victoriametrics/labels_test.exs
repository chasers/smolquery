defmodule SmolqueryVictoriaMetrics.LabelsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.Labels
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.Runtime

  doctest Labels

  @t0_ms 1_789_812_000_000
  @range {@t0_ms, @t0_ms + 60_000}

  defp runtime, do: Runtime.new(name: :labels_test, password: "p")

  defp selector(text) do
    {:ok, expr} = MetricsQL.parse(text)
    expr
  end

  defp ts(ms), do: (ms * 1_000) |> DateTime.from_unix!(:microsecond) |> DateTime.to_naive()

  describe "limit/1, as -search.maxTagKeys and -search.maxTagValues bound it" do
    test "a positive limit up to 100,000 is kept, anything else is 100,000" do
      assert Labels.limit(1) == 1
      assert Labels.limit(100_000) == 100_000

      for requested <- [0, -1, 100_001] do
        assert Labels.limit(requested) == 100_000
      end
    end
  end

  describe "where/2" do
    test "without a selector, the time range alone" do
      assert {:ok, "ts BETWEEN $1 AND $2", [from, to]} = Labels.where(nil, @range)
      assert from == ts(@t0_ms)
      assert to == ts(@t0_ms + 60_000)
    end

    test "a range starting before the epoch starts at it" do
      assert {:ok, _sql, [from, _to]} = Labels.where(nil, {-300_000, 0})
      assert from == ts(0)
    end

    test "with a selector, the samples predicate, metric name first" do
      assert {:ok, "name = $1 AND ts BETWEEN $2 AND $3 AND labels[$4] = $5",
              ["up", _from, _to, "job", "a"]} =
               Labels.where(selector(~s(up{job="a"})), @range)
    end

    test "a selector matching every series is refused" do
      assert {:error, {:empty_selector, _message}} =
               Labels.where(selector(~s({job=~".*"})), @range)
    end
  end

  describe "names_query/4" do
    test "the distinct keys of every row's labels, and __name__, in order" do
      assert {:ok, sql, [_from, _to]} = Labels.names_query(runtime(), nil, @range, 7)

      assert sql ==
               "SELECT DISTINCT unnest(list_append(map_keys(labels), '__name__')) AS label " <>
                 ~s(FROM "metrics"."samples" WHERE ts BETWEEN $1 AND $2 ORDER BY label LIMIT 7)
    end

    test "narrowed by a selector" do
      assert {:ok, sql, ["up", _from, _to]} =
               Labels.names_query(runtime(), selector("up"), @range, 100_000)

      assert sql =~ "WHERE name = $1 AND ts BETWEEN $2 AND $3 ORDER BY label LIMIT 100000"
    end
  end

  describe "values_query/5" do
    test "__name__ reads the name column" do
      assert {:ok, sql, [_from, _to]} = Labels.values_query(runtime(), "__name__", nil, @range, 3)

      assert sql ==
               ~s(SELECT DISTINCT name AS value FROM "metrics"."samples" ) <>
                 "WHERE ts BETWEEN $1 AND $2 ORDER BY value LIMIT 3"
    end

    test "another label reads its map entry where the series has it, bound last" do
      assert {:ok, sql, ["up", _from, _to, "job", "a", "instance"]} =
               Labels.values_query(runtime(), "instance", selector(~s(up{job="a"})), @range, 9)

      assert sql ==
               ~s(SELECT DISTINCT labels[$6] AS value FROM "metrics"."samples" ) <>
                 "WHERE name = $1 AND ts BETWEEN $2 AND $3 AND labels[$4] = $5 " <>
                 "AND labels[$6] IS NOT NULL ORDER BY value LIMIT 9"
    end
  end

  describe "over a real query service" do
    @describetag :tmp_dir
    @describetag :capture_log

    setup context do
      stack = VictoriaMetricsStack.start(context)

      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "up", "job" => "b", "instance" => "i1"}, [{@t0_ms, 1}]},
          {%{"__name__" => "up", "job" => "a"}, [{@t0_ms + 1_000, 1}]},
          {%{"__name__" => "load", "zone" => "z"}, [{@t0_ms + 2_000, 0.5}]},
          {%{"__name__" => "bare"}, [{@t0_ms + 3_000, 2}]},
          {%{"__name__" => "late", "region" => "r"}, [{@t0_ms + 3_600_000, 1}]}
        ])

      %{stack: stack}
    end

    test "names/5: sorted, __name__ included, the range and the selector applied",
         %{stack: stack} do
      assert Labels.names(stack.runtime, nil, @range, 100) ==
               {:ok, ["__name__", "instance", "job", "zone"]}

      assert Labels.names(stack.runtime, selector("up"), @range, 100) ==
               {:ok, ["__name__", "instance", "job"]}

      assert Labels.names(stack.runtime, selector("bare"), @range, 100) == {:ok, ["__name__"]}
      assert Labels.names(stack.runtime, nil, @range, 2) == {:ok, ["__name__", "instance"]}
      assert Labels.names(stack.runtime, nil, {0, 1_000}, 100) == {:ok, []}
    end

    test "values/6: __name__ values and a label's, missing labels left out", %{stack: stack} do
      assert Labels.values(stack.runtime, "__name__", nil, @range, 100) ==
               {:ok, ["bare", "load", "up"]}

      assert Labels.values(stack.runtime, "job", nil, @range, 100) == {:ok, ["a", "b"]}
      assert Labels.values(stack.runtime, "job", nil, @range, 1) == {:ok, ["a"]}

      assert Labels.values(stack.runtime, "job", selector(~s({instance="i1"})), @range, 100) ==
               {:ok, ["b"]}

      assert Labels.values(stack.runtime, "nope", nil, @range, 100) == {:ok, []}
    end

    test "a table not created yet answers nothing", %{stack: stack} do
      runtime = %{stack.runtime | table: {"metrics", "never_written"}}

      assert Labels.names(runtime, nil, @range, 100) == {:ok, []}
      assert Labels.values(runtime, "job", nil, @range, 100) == {:ok, []}
    end
  end
end
