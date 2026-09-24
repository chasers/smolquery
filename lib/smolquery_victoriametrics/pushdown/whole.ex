defmodule SmolqueryVictoriaMetrics.Pushdown.Whole do
  @moduledoc """
  The SQL for the rollups that read a window whole (PL-70, T-586):
  `sum2_over_time`, `range_over_time`, `distinct_over_time`,
  `tmin_over_time`, `tmax_over_time`, `timestamp`, `stddev_over_time`,
  `stdvar_over_time`, `quantile_over_time`, `geomean_over_time`,
  `rate_over_sum`, `count_eq_over_time`, `count_ne_over_time`,
  `count_gt_over_time`, `count_le_over_time`, `changes`, `resets`, `lag`,
  `lifetime` and `scrape_interval`, each a fold over `items`, the window's
  samples in `(ts, value)` order that `SmolqueryVictoriaMetrics.Pushdown.Windows`
  carries on every row of its descriptor, with `cnt`, `prev_v`, `prev_ts`,
  `has_prev`, `f_ts`, `l_ts`, `t` and `win` beside it.

  Each fold adds, multiplies or compares in the order and from the start
  `SmolqueryVictoriaMetrics.Rollup` does, so the doubles come out the same:
  a sum starts at `0.0` and adds each value in turn; the variance is
  Welford's, seeded from the first value; a quantile interpolates between
  the two nearest ranks of the sorted values as `Value.quantile_sorted/2`
  does; `changes` and `resets` compare each value with the one before it,
  starting from the sample before the window when there is one; the
  timestamp of the smallest or largest value is the last one on a tie. A
  NaN is never in `items`, since the write path drops it, but a fold can
  make one from two infinities, and DuckDB orders NaN above every number,
  so a comparison that could see one tests `isnan` first.

  A scalar argument (`quantile_over_time(0.9, m[5m])`, `count_gt_over_time(m[5m], 10)`)
  is pushed only as a number literal, bound as `$scalar`; VictoriaMetrics
  reads it per point, which a literal makes the same everywhere.
  """

  @functions ~w(
    sum2_over_time range_over_time distinct_over_time tmin_over_time tmax_over_time timestamp
    stddev_over_time stdvar_over_time quantile_over_time geomean_over_time rate_over_sum
    count_eq_over_time count_ne_over_time count_gt_over_time count_le_over_time changes resets
    lag lifetime scrape_interval
  )
  @with_scalar ~w(quantile_over_time count_eq_over_time count_ne_over_time count_gt_over_time count_le_over_time)
  @reads_prev ~w(changes resets lag lifetime scrape_interval)

  @doc "The rollups computed here."
  @spec functions() :: [String.t()]
  def functions, do: @functions

  @doc "Whether `name` takes a scalar argument, the one bound as `scalar`."
  @spec scalar?(String.t()) :: boolean()
  def scalar?(name), do: name in @with_scalar

  @doc "Whether `name` reads the sample before the window."
  @spec reads_prev?(String.t()) :: boolean()
  def reads_prev?(name), do: name in @reads_prev

  @vals "list_transform(items, x -> x.v)"
  @sum "list_reduce(list_prepend(0.0::DOUBLE, #{@vals}), (a, b) -> a + b)"

  @doc """
  Welford's mean and sum of squared deviations over the doubles of the
  list `values`, folded from its first element as `Value.stdvar/1` folds
  them: a struct with `n`, `avg` and `q`.
  """
  @spec welford(String.t()) :: String.t()
  def welford(values) do
    "list_reduce(list_transform(#{values}, x -> {'n': 1, 'avg': x, 'q': 0.0::DOUBLE}), " <>
      "(a, b) -> {'n': a.n + 1, 'avg': a.avg + (b.avg - a.avg) / (a.n + 1), " <>
      "'q': a.q + (b.avg - a.avg) * (b.avg - (a.avg + (b.avg - a.avg) / (a.n + 1)))})"
  end

  @doc """
  The `phi` quantile of `sorted`, a sorted list of doubles, interpolated
  between the two nearest ranks as `Value.quantile_sorted/2` does; `phi`
  is SQL, a bound parameter.
  """
  @spec quantile(String.t(), String.t()) :: String.t()
  def quantile(sorted, phi) do
    "(SELECT q[lower + 1] * (1 - weight) + q[least(len(q) - 1, lower + 1) + 1] * weight " <>
      "FROM (SELECT q, CAST(floor(rank) AS BIGINT) AS lower, rank - floor(rank) AS weight " <>
      "FROM (SELECT q, #{phi} * (len(q) - 1) AS rank FROM (SELECT #{sorted} AS q))))"
  end

  @doc """
  The `v` expression for `rollup` over a row of the descriptor; `scalar`
  names the bound scalar argument, when the function takes one.
  """
  @spec value(String.t(), String.t() | nil) :: String.t()
  def value("sum2_over_time", _scalar),
    do:
      held(
        "list_reduce(list_prepend(0.0::DOUBLE, list_transform(items, x -> x.v * x.v)), (a, b) -> a + b)"
      )

  def value("range_over_time", _scalar),
    do: held("list_aggregate(#{@vals}, 'max') - list_aggregate(#{@vals}, 'min')")

  def value("distinct_over_time", _scalar),
    do:
      held(
        "CAST(len(list_distinct(list_transform(items, x -> CASE WHEN x.v = 0 THEN 0.0::DOUBLE ELSE x.v END))) AS DOUBLE)"
      )

  def value("tmin_over_time", _scalar),
    do: held("list_reduce(items, (a, b) -> CASE WHEN b.v <= a.v THEN b ELSE a END).ts / 1000")

  def value("tmax_over_time", _scalar),
    do: held("list_reduce(items, (a, b) -> CASE WHEN b.v >= a.v THEN b ELSE a END).ts / 1000")

  def value("timestamp", _scalar), do: held("l_ts / 1000")

  def value("stdvar_over_time", _scalar),
    do: "CASE WHEN cnt = 0 THEN NULL WHEN cnt = 1 THEN 0.0::DOUBLE ELSE #{stdvar()} END"

  def value("stddev_over_time", _scalar),
    do: "CASE WHEN cnt = 0 THEN NULL WHEN cnt = 1 THEN 0.0::DOUBLE ELSE sqrt(#{stdvar()}) END"

  def value("quantile_over_time", scalar) do
    "CASE WHEN cnt = 0 THEN NULL WHEN #{scalar} < 0 THEN '-infinity'::DOUBLE " <>
      "WHEN #{scalar} > 1 THEN 'infinity'::DOUBLE ELSE " <>
      quantile("list_sort(#{@vals})", scalar) <> " END"
  end

  def value("geomean_over_time", _scalar),
    do: held("pow(list_reduce(list_prepend(1.0::DOUBLE, #{@vals}), (a, b) -> a * b), 1.0 / cnt)")

  def value("rate_over_sum", _scalar), do: held("#{@sum} / (win / 1000)")

  def value("count_eq_over_time", scalar), do: counted("x = #{scalar}")
  def value("count_ne_over_time", scalar), do: counted("x <> #{scalar}")
  def value("count_gt_over_time", scalar), do: counted("x > #{scalar}")
  def value("count_le_over_time", scalar), do: counted("x <= #{scalar}")

  def value("changes", _scalar) do
    "CASE WHEN cnt = 0 AND NOT has_prev THEN NULL ELSE " <>
      fold(
        "prev_v IS NULL",
        "1.0::DOUBLE",
        "{'p': CASE WHEN #{same("b.p", "a.p")} THEN a.p ELSE b.p END, " <>
          "'n': a.n + CASE WHEN #{same("b.p", "a.p")} THEN 0 ELSE 1 END}"
      ) <> ".n END"
  end

  def value("resets", _scalar) do
    "CASE WHEN cnt = 0 AND NOT has_prev THEN NULL ELSE " <>
      fold(
        "NOT has_prev",
        "0.0::DOUBLE",
        "{'p': b.p, 'n': a.n + CASE WHEN b.p < a.p AND NOT " <>
          "#{negligible("b.p", "a.p")} THEN 1 ELSE 0 END}"
      ) <> ".n END"
  end

  def value("lag", _scalar) do
    "CASE WHEN cnt = 0 THEN (CASE WHEN has_prev THEN (t - prev_ts) / 1000 END) ELSE (t - l_ts) / 1000 END"
  end

  def value("lifetime", _scalar) do
    "CASE WHEN NOT has_prev THEN (CASE WHEN cnt >= 2 THEN (l_ts - f_ts) / 1000 END) " <>
      "WHEN cnt = 0 THEN NULL ELSE (l_ts - prev_ts) / 1000 END"
  end

  def value("scrape_interval", _scalar) do
    "CASE WHEN NOT has_prev THEN (CASE WHEN cnt >= 2 THEN (l_ts - f_ts) / 1000 / (cnt - 1) END) " <>
      "WHEN cnt = 0 THEN NULL ELSE (l_ts - prev_ts) / 1000 / cnt END"
  end

  defp held(expression), do: "CASE WHEN cnt > 0 THEN #{expression} END"

  defp counted(predicate),
    do: held("CAST(len(list_filter(#{@vals}, x -> #{predicate})) AS DOUBLE)")

  defp stdvar, do: "#{welford(@vals)}.q / cnt"

  defp fold(unseeded, first_n, step) do
    "list_reduce(list_prepend(" <>
      "CASE WHEN #{unseeded} THEN #{counter("items[1].v", first_n)} " <>
      "ELSE #{counter("prev_v", "0.0::DOUBLE")} END, " <>
      "list_transform(CASE WHEN #{unseeded} THEN items[2:] ELSE items END, x -> " <>
      "#{counter("x.v", "0.0::DOUBLE")})), (a, b) -> #{step})"
  end

  defp counter(previous, count), do: "{'p': #{previous}, 'n': #{count}}"

  defp same(value, previous),
    do: "(#{value} = #{previous} OR #{negligible(value, previous)})"

  defp negligible(value, previous),
    do:
      "(NOT isnan(#{value} - #{previous}) AND abs(#{value} - #{previous}) < 1e-12 * abs(#{value}))"
end
