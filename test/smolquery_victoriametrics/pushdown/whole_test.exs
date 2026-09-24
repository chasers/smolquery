defmodule SmolqueryVictoriaMetrics.Pushdown.WholeTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Pushdown.Whole

  test "functions/0, scalar?/1 and reads_prev?/1 describe the rollups read whole" do
    assert "changes" in Whole.functions()
    assert "quantile_over_time" in Whole.functions()
    refute "rate" in Whole.functions()
    assert Whole.scalar?("count_gt_over_time")
    refute Whole.scalar?("sum2_over_time")
    assert Whole.reads_prev?("resets")
    refute Whole.reads_prev?("tmin_over_time")
  end

  test "welford/1 folds from the first value with Value.stdvar's steps" do
    sql = Whole.welford("vals")
    assert sql =~ "list_transform(vals, x -> {'n': 1, 'avg': x, 'q': 0.0::DOUBLE})"
    assert sql =~ "'avg': a.avg + (b.avg - a.avg) / (a.n + 1)"
    assert sql =~ "'q': a.q + (b.avg - a.avg) * (b.avg - (a.avg + (b.avg - a.avg) / (a.n + 1)))"
  end

  test "quantile/2 interpolates between the two nearest ranks" do
    sql = Whole.quantile("list_sort(vals)", "$9")
    assert sql =~ "q[lower + 1] * (1 - weight) + q[least(len(q) - 1, lower + 1) + 1] * weight"
    assert sql =~ "$9 * (len(q) - 1) AS rank"
  end

  describe "value/2" do
    test "every function has an expression, over items and the descriptor" do
      for name <- Whole.functions() do
        scalar = if Whole.scalar?(name), do: "$9", else: nil
        assert is_binary(Whole.value(name, scalar)), name
      end
    end

    test "sums start at 0.0 and fold in order; the timestamp of a tie is the later one" do
      assert Whole.value("sum2_over_time", nil) =~
               "list_reduce(list_prepend(0.0::DOUBLE, list_transform(items, x -> x.v * x.v)), (a, b) -> a + b)"

      assert Whole.value("tmin_over_time", nil) =~
               "CASE WHEN b.v <= a.v THEN b ELSE a END).ts / 1000"

      assert Whole.value("tmax_over_time", nil) =~
               "CASE WHEN b.v >= a.v THEN b ELSE a END).ts / 1000"

      assert Whole.value("rate_over_sum", nil) =~ "/ (win / 1000)"

      assert Whole.value("count_gt_over_time", "$9") =~
               "list_filter(list_transform(items, x -> x.v), x -> x > $9)"

      assert Whole.value("quantile_over_time", "$9") =~ "WHEN $9 < 0 THEN '-infinity'::DOUBLE"
    end

    test "changes and resets start from the sample before the window when there is one" do
      changes = Whole.value("changes", nil)
      assert changes =~ "CASE WHEN cnt = 0 AND NOT has_prev THEN NULL ELSE"

      assert changes =~
               "CASE WHEN prev_v IS NULL THEN {'p': items[1].v, 'n': 1.0::DOUBLE} ELSE {'p': prev_v, 'n': 0.0::DOUBLE} END"

      assert Whole.value("resets", nil) =~
               "CASE WHEN NOT has_prev THEN {'p': items[1].v, 'n': 0.0::DOUBLE} ELSE {'p': prev_v, 'n': 0.0::DOUBLE} END"

      assert changes =~ "CASE WHEN prev_v IS NULL THEN items[2:] ELSE items END"
      assert changes =~ "NOT isnan(b.p - a.p) AND abs(b.p - a.p) < 1e-12 * abs(b.p)"
      assert Whole.value("resets", nil) =~ "CASE WHEN b.p < a.p AND NOT"
    end

    test "a variance of one sample is 0 and of none is NULL" do
      assert Whole.value("stdvar_over_time", nil) =~
               "CASE WHEN cnt = 0 THEN NULL WHEN cnt = 1 THEN 0.0::DOUBLE ELSE"

      assert Whole.value("stddev_over_time", nil) =~ "ELSE sqrt("
    end
  end
end
