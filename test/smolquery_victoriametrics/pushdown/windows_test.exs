defmodule SmolqueryVictoriaMetrics.Pushdown.WindowsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Pushdown.Windows

  @refs %{start: "$4", step: "$5", window: "$6", points: "$7"}

  test "functions/0 names the rollups read off a window's edges" do
    assert "rate" in Windows.functions()
    assert "present_over_time" in Windows.functions()
    refute "sum_over_time" in Windows.functions()
  end

  describe "stages/5" do
    test "orders each series by (ts, value), removes counter resets, and carries each window as a list" do
      sql = IO.iodata_to_binary(Windows.stages("rate", @refs, ["k0"], false, false))

      assert sql =~ "o AS (SELECT *, row_number() OVER win AS idx"
      assert sql =~ "WINDOW win AS (PARTITION BY series ORDER BY ts_ms, value)"
      assert sql =~ "r1 AS (SELECT *, value + sum(CASE WHEN value < lag_raw THEN"

      assert sql =~
               "r AS (SELECT *, max(c_sum) OVER (PARTITION BY series ORDER BY idx ROWS UNBOUNDED PRECEDING) AS cv FROM r1)"

      assert sql =~ "FROM (SELECT (CASE WHEN max(n) < 2 THEN $5 ELSE"
      assert sql =~ "0.6 * (len(q) - 1) AS rank"
      assert sql =~ "WHEN i <= 2000 THEN i + 4 * i"
      assert sql =~ "mw AS (SELECT series, max_prev, $6 AS win FROM m)"
      assert sql =~ "RANGE BETWEEN mw.win PRECEDING AND CURRENT ROW"
      assert sql =~ "max({'idx': r.idx, 'ts': r.ts_ms, 'v': r.cv}) OVER bf AS before"
      assert sql =~ "RANGE BETWEEN UNBOUNDED PRECEDING AND mw.win + 1 PRECEDING"
      assert sql =~ "UNION ALL SELECT w.*, false AS held, unnest(generate_series("
      assert sql =~ "d0 AS (SELECT series, k0, k, $4 + k * $5 AS t"
      assert sql =~ "frame[1:len(frame) - (group_last - idx)] AS mine"
      assert sql =~ "list_filter(mine, x -> x.ts <= t - win) AS older"
      assert sql =~ "mine[len(older) + 1:] AS items"
      assert sql =~ "prev_ts IS NOT NULL AND prev_ts > t - win - max_prev AS has_prev"
      refute sql =~ "JOIN l"
      assert sql =~ "e AS (SELECT *, " <> Windows.value("rate") <> " AS v FROM dp), "
    end

    test "a gauge rollup keeps the raw values; an instant query's max_prev is the step; an unwritten window widens" do
      sql = IO.iodata_to_binary(Windows.stages("delta", @refs, [], true, true))

      assert sql =~ "r AS (SELECT *, value AS cv FROM o)"
      refute sql =~ "r1 AS"
      assert sql =~ "m AS (SELECT series, $5 AS max_prev FROM o GROUP BY series)"
      assert sql =~ "mw AS (SELECT series, max_prev, greatest($6, max_prev) AS win FROM m)"
      assert sql =~ "d0 AS (SELECT series, k, $4 + k * $5 AS t"
    end

    test "a rollup that reads no previous sample skips the sample before the frame and the empty windows" do
      sql = IO.iodata_to_binary(Windows.stages("first_over_time", @refs, [], false, false))

      refute sql =~ "OVER bf"
      refute sql =~ "UNION ALL"
      assert sql =~ "NULL::STRUCT(idx BIGINT, ts BIGINT, v DOUBLE) AS before"
    end
  end

  describe "value/1" do
    test "rate divides by the IEEE rule and answers 0 over an empty window with a previous sample" do
      assert Windows.value("rate") =~ "WHEN cnt = 0 THEN 0.0::DOUBLE ELSE"
      assert Windows.value("rate") =~ "CASE WHEN ((l_ts - prev_ts) / 1000) = 0 THEN"
      assert Windows.value("rate") == Windows.value("deriv_fast")
    end

    test "each function has its expression" do
      for name <- Windows.functions(), do: assert(is_binary(Windows.value(name)), name)

      assert Windows.value("increase") == Windows.value("delta")
      assert Windows.value("delta") =~ "WHEN cnt = 1 THEN 0.0::DOUBLE ELSE l_v - f_v END"
      assert Windows.value("irate") == Windows.value("ideriv")
      assert Windows.value("irate") =~ "WHEN e_ts IS NOT NULL THEN"
      assert Windows.value("present_over_time") == "CASE WHEN cnt > 0 THEN 1.0::DOUBLE END"
      assert Windows.value("first_over_time") == "CASE WHEN cnt > 0 THEN f_v END"
      assert Windows.value("idelta") =~ "ELSE l_v - l_lag_v END"
      assert Windows.value("increase_pure") =~ "l_v - coalesce(prev_v, 0.0)"
    end
  end
end
