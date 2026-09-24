defmodule SmolqueryVictoriaMetrics.Pushdown.Windows do
  @moduledoc """
  The SQL for the rollups that read a window's edges rather than its whole
  (PL-70, T-585): `rate`, `deriv_fast`, `increase`, `increase_pure`, `delta`,
  `idelta`, `irate`, `ideriv`, `first_over_time` and `present_over_time`,
  each computed from what `SmolqueryVictoriaMetrics.Rollup.Window` holds at a
  point of the grid: the window's first and last samples, the sample before
  the last, the sample before the window, the one after it, and how many
  it holds.

  ## The descriptor

  Everything is per series, ordered by `(ts, value)` as the fetch orders it,
  in window functions over the samples read for
  `[start - max(window, step) - lookback_ms, end]`, the range a fetch reads,
  since the first two rules below look at the whole of it:

    * counter resets (`Rollup.remove_counter_resets/3`), for the functions
      `Rollup.removes_counter_resets?/1` names: a fall of less than an
      eighth of the previous value adds the fall, any other adds the
      previous value, as a running sum, and the result never decreases, as
      a running max;
    * `max_prev` (`Rollup.scrape_interval/3` and `inflate/1`): the 0.6
      quantile of the gaps between the series' last 21 samples, inflated,
      for a range query; the step for an instant query. It is the furthest
      before its window a sample still counts as the window's `prev_value`,
      and, for the functions `Rollup.may_adjust_window?/1` names with no
      window written, the window itself when it is longer than the step;
    * each sample carries `frame`, the samples within one window before it
      (a `RANGE` frame of the per-series window, sorted by index), and
      `before`, the last sample older than that. A sample is the window's
      last at the grid points `[ts, min(next_ts, ts + w))`, and is unnested
      into them (`held`); at such a point the window holds the samples of
      `frame` up to this one with `ts > t - w` (`items`), the first of them
      is the window's first, the one before it in `frame` (or `before`) is
      the sample before the window, and the one before the last's timestamp
      group is `irate`'s `earlier`;
    * a sample is also the sample before an empty window at the points
      `[ts + w, min(next_ts, ts + w + max_prev))`, and is unnested into
      them too (not `held`) for the functions that read `prev_value`, which
      is how `rate` answers `0` there.

  One row per series and point, no join. A rollup here is pushed only while
  the window is at most 32 steps (`SmolqueryVictoriaMetrics.Pushdown`),
  since a sample's `frame` holds a window of samples and the sample unnests
  into up to a window of points.

  A division by zero answers the IEEE result whatever the engine's
  `ieee_floating_point_ops` setting, since `SmolqueryVictoriaMetrics.Eval.Value`
  answers it.
  """

  alias SmolqueryVictoriaMetrics.Rollup

  @functions ~w(
    rate deriv_fast increase increase_pure delta idelta irate ideriv first_over_time
    present_over_time
  )
  @reads_prev ~w(rate deriv_fast increase increase_pure delta idelta irate ideriv)

  @doc "The rollups computed here."
  @spec functions() :: [String.t()]
  def functions, do: @functions

  @doc """
  The CTEs from `s` (the samples read) to `e` (the descriptor with `v`, the
  rollup's value). `refs` names the bound parameters (`start`, `step`,
  `window`, `points`), `keys` the group key columns of `s`, `instant?`
  whether `max_prev` is the step, and `adjust_window?` whether the window
  is `max(step, max_prev)` rather than the one bound.
  """
  @spec stages(String.t(), map(), [String.t()], boolean(), boolean()) :: iodata()
  def stages(rollup, refs, keys, instant?, adjust_window?) do
    columns = Enum.map(keys, &[", ", &1])

    [
      "o AS (SELECT *, row_number() OVER win AS idx, count(*) OVER (PARTITION BY series) AS n, ",
      "lag(ts_ms) OVER win AS prev_ts, lead(ts_ms) OVER win AS next_ts, ",
      "lag(value) OVER win AS lag_raw FROM s WINDOW win AS (PARTITION BY series ORDER BY ts_ms, value)), ",
      corrected(rollup),
      "m AS (SELECT series, ",
      max_prev(refs, instant?),
      " AS max_prev FROM o GROUP BY series), ",
      "mw AS (SELECT series, max_prev, #{window(refs, adjust_window?)} AS win FROM m), ",
      "w AS (SELECT r.*, mw.max_prev, mw.win, lead(r.cv) OVER (PARTITION BY r.series ORDER BY r.idx) AS next_v, ",
      "max(r.idx) OVER (PARTITION BY r.series, r.ts_ms) AS group_last, ",
      "min(r.idx) OVER (PARTITION BY r.series, r.ts_ms) AS group_first, ",
      "list_sort(list(#{sample()}) OVER fr) AS frame",
      before(rollup),
      " FROM r JOIN mw USING (series) ",
      "WINDOW fr AS (PARTITION BY r.series ORDER BY r.ts_ms RANGE BETWEEN mw.win PRECEDING AND CURRENT ROW)",
      before_window(rollup),
      "), ",
      "u AS (SELECT w.*, true AS held, unnest(generate_series(",
      "greatest(0, CAST(ceil((w.ts_ms - #{refs.start}) / #{refs.step}) AS BIGINT)), ",
      "least(#{refs.points} - 1, CAST(floor((w.ts_ms + w.win - 1 - #{refs.start}) / #{refs.step}) AS BIGINT), ",
      "CAST(floor((coalesce(w.next_ts, w.ts_ms + w.win) - 1 - #{refs.start}) / #{refs.step}) AS BIGINT)))) AS k FROM w",
      empty_windows(rollup, refs),
      "), ",
      "d0 AS (SELECT series",
      columns,
      ", k, #{refs.start} + k * #{refs.step} AS t, held, idx, ts_ms, cv, next_v, ",
      "frame[1:len(frame) - (group_last - idx)] AS mine, group_first, ",
      "#{before_column(rollup)} AS before, max_prev, win FROM u), ",
      "d1 AS (SELECT *, list_filter(mine, x -> x.ts <= t - win) AS older FROM d0), ",
      "d2 AS (SELECT *, mine[len(older) + 1:] AS items FROM d1), ",
      "d AS (SELECT series",
      columns,
      ", k, t, max_prev, win, CASE WHEN held THEN len(items) ELSE 0 END AS cnt, ",
      "CASE WHEN NOT held THEN ts_ms WHEN len(older) > 0 THEN older[-1].ts ELSE before.ts END AS prev_ts, ",
      "CASE WHEN NOT held THEN cv WHEN len(older) > 0 THEN older[-1].v ELSE before.v END AS prev_v, ",
      "items[1].ts AS f_ts, items[1].v AS f_v, ",
      "CASE WHEN len(items) > 1 THEN items[2].v ELSE next_v END AS f_next_v, ",
      "ts_ms AS l_ts, cv AS l_v, items[-2].v AS l_lag_v, ",
      "items[group_first - items[1].idx].v AS e_v, items[group_first - items[1].idx].ts AS e_ts FROM d2), ",
      "dp AS (SELECT *, prev_ts IS NOT NULL AND prev_ts > t - win - max_prev AS has_prev FROM d), ",
      "e AS (SELECT *, #{value(rollup)} AS v FROM dp), "
    ]
  end

  defp sample, do: "{'idx': r.idx, 'ts': r.ts_ms, 'v': r.cv}"

  defp corrected(rollup) do
    if Rollup.removes_counter_resets?(rollup) do
      [
        "r1 AS (SELECT *, value + sum(CASE WHEN value < lag_raw THEN ",
        "(CASE WHEN lag_raw - value < lag_raw / 8 THEN lag_raw - value ELSE lag_raw END) ",
        "ELSE 0 END) OVER (PARTITION BY series ORDER BY idx ROWS UNBOUNDED PRECEDING) AS c_sum FROM o), ",
        "r AS (SELECT *, max(c_sum) OVER (PARTITION BY series ORDER BY idx ROWS UNBOUNDED PRECEDING) AS cv FROM r1), "
      ]
    else
      "r AS (SELECT *, value AS cv FROM o), "
    end
  end

  defp before(rollup) when rollup in @reads_prev, do: ", max(#{sample()}) OVER bf AS before"
  defp before(_rollup), do: ""

  defp before_window(rollup) when rollup in @reads_prev,
    do:
      ", bf AS (PARTITION BY r.series ORDER BY r.ts_ms RANGE BETWEEN UNBOUNDED PRECEDING AND mw.win + 1 PRECEDING)"

  defp before_window(_rollup), do: ""

  defp before_column(rollup) when rollup in @reads_prev, do: "before"
  defp before_column(_rollup), do: "NULL::STRUCT(idx BIGINT, ts BIGINT, v DOUBLE)"

  defp empty_windows(rollup, refs) when rollup in @reads_prev do
    [
      " UNION ALL SELECT w.*, false AS held, unnest(generate_series(",
      "greatest(0, CAST(ceil((w.ts_ms + w.win - #{refs.start}) / #{refs.step}) AS BIGINT)), ",
      "least(#{refs.points} - 1, CAST(floor((w.next_ts - 1 - #{refs.start}) / #{refs.step}) AS BIGINT), ",
      "CAST(floor((w.ts_ms + w.win + w.max_prev - 1 - #{refs.start}) / #{refs.step}) AS BIGINT)))) AS k ",
      "FROM w WHERE w.next_ts IS NOT NULL"
    ]
  end

  defp empty_windows(_rollup, _refs), do: ""

  defp max_prev(refs, true), do: refs.step

  defp max_prev(refs, false),
    do: ["CASE WHEN max(n) < 2 THEN #{refs.step} ELSE ", inflate(quantile(refs)), " END"]

  defp quantile(refs) do
    gaps =
      "list_sort(list(CAST(ts_ms - prev_ts AS DOUBLE)) " <>
        "FILTER (WHERE prev_ts IS NOT NULL AND idx >= n - 19))"

    "(SELECT CASE WHEN trunc(est) > 0 THEN CAST(trunc(est) AS BIGINT) ELSE #{refs.step} END " <>
      "FROM (SELECT q[lower + 1] * (1 - weight) + q[least(len(q) - 1, lower + 1) + 1] * weight AS est " <>
      "FROM (SELECT q, CAST(floor(rank) AS BIGINT) AS lower, rank - floor(rank) AS weight " <>
      "FROM (SELECT q, 0.6 * (len(q) - 1) AS rank FROM (SELECT #{gaps} AS q)))))"
  end

  defp inflate(interval) do
    "(SELECT CASE WHEN i <= 2000 THEN i + 4 * i WHEN i <= 4000 THEN i + 2 * i " <>
      "WHEN i <= 8000 THEN i + i WHEN i <= 16000 THEN i + i // 2 " <>
      "WHEN i <= 32000 THEN i + i // 4 ELSE i + i // 8 END FROM (SELECT #{interval} AS i))"
  end

  defp window(refs, true), do: "greatest(#{refs.window}, max_prev)"
  defp window(refs, false), do: refs.window

  @doc "The `v` expression for `rollup` over a row of the descriptor `d`."
  @spec value(String.t()) :: String.t()
  def value(rollup) when rollup in ["rate", "deriv_fast"] do
    "CASE WHEN NOT has_prev THEN (CASE WHEN cnt >= 2 THEN " <>
      divide("l_v - f_v", "(l_ts - f_ts) / 1000") <>
      " END) WHEN cnt = 0 THEN 0.0::DOUBLE ELSE " <>
      divide("l_v - prev_v", "(l_ts - prev_ts) / 1000") <> " END"
  end

  def value(rollup) when rollup in ["irate", "ideriv"] do
    "CASE WHEN cnt = 0 THEN NULL WHEN cnt = 1 THEN (CASE WHEN has_prev THEN " <>
      divide("l_v - prev_v", "(l_ts - prev_ts) / 1000") <>
      " END) WHEN e_ts IS NOT NULL THEN " <>
      divide("l_v - e_v", "(l_ts - e_ts) / 1000") <>
      " WHEN has_prev THEN " <>
      divide("l_v - prev_v", "(l_ts - prev_ts) / 1000") <> " ELSE 0.0::DOUBLE END"
  end

  def value("idelta") do
    "CASE WHEN cnt = 0 THEN (CASE WHEN has_prev THEN 0.0::DOUBLE END) " <>
      "WHEN cnt = 1 THEN (CASE WHEN has_prev THEN l_v - prev_v ELSE l_v END) ELSE l_v - l_lag_v END"
  end

  def value(rollup) when rollup in ["delta", "increase"] do
    "CASE WHEN NOT has_prev THEN (CASE WHEN cnt = 0 THEN NULL WHEN prev_v IS NOT NULL THEN l_v - prev_v " <>
      "WHEN NOT isnan(coalesce(f_next_v, f_v) - f_v) AND " <>
      "abs(f_v) < 10 * (abs(coalesce(f_next_v, f_v) - f_v) + 1) THEN l_v " <>
      "WHEN cnt = 1 THEN 0.0::DOUBLE ELSE l_v - f_v END) " <>
      "WHEN cnt = 0 THEN 0.0::DOUBLE ELSE l_v - prev_v END"
  end

  def value("increase_pure") do
    "CASE WHEN NOT has_prev THEN (CASE WHEN cnt = 0 THEN NULL ELSE l_v - coalesce(prev_v, 0.0) END) " <>
      "WHEN cnt = 0 THEN 0.0::DOUBLE ELSE l_v - prev_v END"
  end

  def value("first_over_time"), do: "CASE WHEN cnt > 0 THEN f_v END"
  def value("present_over_time"), do: "CASE WHEN cnt > 0 THEN 1.0::DOUBLE END"

  defp divide(x, dt) do
    "(CASE WHEN (#{dt}) = 0 THEN (CASE WHEN isnan(#{x}) OR (#{x}) = 0 THEN 'nan'::DOUBLE " <>
      "WHEN (#{x}) > 0 THEN 'infinity'::DOUBLE ELSE '-infinity'::DOUBLE END) ELSE (#{x}) / (#{dt}) END)"
  end
end
