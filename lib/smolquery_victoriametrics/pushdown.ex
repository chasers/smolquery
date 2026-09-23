defmodule SmolqueryVictoriaMetrics.Pushdown do
  @moduledoc """
  An aggregate over one rollup of a selector, computed in SQL (PL-70, T-568).

  `count(m{job="job-1"})` at ten million series matched a million and took
  124 s through `SmolqueryVictoriaMetrics.Eval`: the scan was 0.4 s, grouping
  a million series and copying their lists into the node 29 s, and the
  aggregate in Elixir 94 s (`bench/results/victoriametrics_cardinality.md`).
  The database had already grouped the rows; here the aggregate runs there
  too, and the node receives one row per output series per point of the
  grid.

  ## What is pushed

  `agg by (labels) (rollup(selector[window]))`, and no more:

    * `agg` is `sum`, `min`, `max`, `avg` or `count`, with `by (...)` or no
      modifier, and no `limit`;
    * `rollup` is `default_rollup` (a bare selector, or `m[5m]`),
      `last_over_time`, `sum_over_time`, `count_over_time`, `min_over_time`,
      `max_over_time` or `avg_over_time`, over a selector with at most a
      window: no `offset`, no `@`, no subquery.

  Anything else, `without`, `rate`, `topk`, an aggregate over an
  expression, evaluates in Elixir as before. `rate` and `increase` stay
  there because their window, when none is written, and the sample they
  read before it depend on the series' own scrape interval
  (`SmolqueryVictoriaMetrics.Rollup`), which SQL would have to estimate the
  same way; the functions here read exactly the window written.

  ## The SQL

  Each rollup here reads the samples in `(t - window, t]` at a point `t` of
  the grid, so a sample at `ts` counts at the grid points `k` from
  `ceil((ts - start) / step)` to `floor((ts + window - 1 - start) / step)`,
  and, for `default_rollup` and `last_over_time`, no further than the point
  before the series' next sample. Each sample is unnested into those points,
  no join and no more rows than samples x window / step, and the aggregate
  is grouped by the `by` labels and point:

      WITH s AS (SELECT series, labels[$4] AS k0, epoch_ms(ts) AS ts_ms, value
                 FROM metrics.samples WHERE <predicate>),
           e AS (SELECT series, k0, value,
                        unnest(generate_series(<first point>, <last point>)) AS k
                 FROM (SELECT *, lead(ts_ms) OVER (PARTITION BY series ORDER BY ts_ms, value)
                              AS next_ts FROM s)),
           a AS (SELECT k, k0, sum(value) FILTER (WHERE NOT isnan(value)) AS value
                 FROM e GROUP BY k, k0),
           c AS (SELECT count(*) AS samples, count(DISTINCT series) AS series FROM s)
      SELECT $5 + a.k * $6 AS t, a.*, c.samples, c.series FROM a, c

  For the last sample the rows of `e` are already one per series and point,
  so the aggregate is one stage, as it is for the pairs that compose
  exactly: `count` over `count_over_time`, `min_over_time` or
  `max_over_time` is `count(DISTINCT series)`, `sum(count_over_time(...))`
  is `count(*)`, `min(min_over_time(...))` is `min`, and
  `max(max_over_time(...))` is `max`. Any other pair,
  `avg(avg_over_time(...))` or `sum(sum_over_time(...))`, groups by series
  and point first, one row per series per point in the job engine's memory,
  which is what a million series over a thousand points costs there.
  `sum(sum_over_time(...))` is not composed because `+Inf` and `-Inf` in
  one series' window are NaN for that series and skipped, as VictoriaMetrics
  skips them, where one stage would make the whole point NaN.

  `<predicate>` is `SmolqueryVictoriaMetrics.Samples.where/3` over
  `[start - window, end]`, so the query prunes by name and time as a fetch
  does, and every label name and grid value is a bound parameter. A window
  not written is the step, or `max(step, lookback_ms)` for `default_rollup`,
  as `SmolqueryVictoriaMetrics.Eval` resolves it.

  A stored infinity (`±1.7976931348623157e308`, PL-70 D6) is read as the
  IEEE infinity before any arithmetic, so `sum_over_time` of `+Inf` and
  `-Inf` is NaN in DuckDB as `SmolqueryVictoriaMetrics.Eval.Value` makes it
  `nil`; the aggregate skips NaN as VictoriaMetrics does and answers NULL,
  read back as `nil`, when nothing is left; an infinity in the answer reads
  back as the stored bound. `__name__` is a group key only when `by` lists
  it and the rollup keeps it (`keep_metric_names`, or one of
  `SmolqueryVictoriaMetrics.Rollup.keeps_metric_name?/1`); a label a series
  lacks is NULL, and absent from that group's labels, as `by` reads it.

  ## Ceilings

  `max_series` bounds the output series: the frame may hold `max_series x
  points` rows, and one group past `max_series` is `{:too_many_series, max}`.
  `max_samples` does not apply, since the samples never leave the database.
  What the query read is still reported in its telemetry and as
  `seriesFetched`: `samples` is every sample of `s`, counted in `c` and
  carried on every row (`c LEFT JOIN a`, so it comes back when the answer
  is empty), and `series` is the groups answered, not the series scanned,
  which would cost a distinct count over every sample for a number nothing
  acts on.

  A rollup other than the last sample is pushed only while the window is at
  most 32 steps: each sample unnests into `window / step` rows, where the
  sweep in `SmolqueryVictoriaMetrics.Rollup` passes over each sample once
  however many points the grid has, so `sum_over_time(m[1d])` at a 15 s
  step stays in Elixir. The last-sample rollups are bounded by the next
  sample and stay at one row per series and point.
  """

  alias Explorer.DataFrame
  alias Explorer.Series, as: ExplorerSeries
  alias SmolqueryVictoriaMetrics.Eval
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.Rollup
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Samples

  @aggregates ~w(sum min max avg count)
  @last ~w(default_rollup last_over_time)
  @inner %{
    "sum_over_time" => "sum(value)",
    "count_over_time" => "count(value)::DOUBLE",
    "min_over_time" => "min(value)",
    "max_over_time" => "max(value)",
    "avg_over_time" => "avg(value)"
  }
  @rollups @last ++ Map.keys(@inner)
  @max_window_steps 32
  @inf "1.7976931348623157e308"

  @enforce_keys [
    :selector,
    :rollup,
    :aggregate,
    :window_ms,
    :start_ms,
    :end_ms,
    :step_ms,
    :timestamps,
    :labels
  ]
  defstruct [
    :selector,
    :rollup,
    :aggregate,
    :window_ms,
    :start_ms,
    :end_ms,
    :step_ms,
    :timestamps,
    :labels,
    name_in_key: false
  ]

  @typedoc """
  A pushed aggregate: the selector, the rollup and aggregate names, the
  window and grid in milliseconds, the `by` labels other than `__name__`,
  and whether `__name__` is a group key too.
  """
  @type t :: %__MODULE__{
          selector: MetricExpr.t(),
          rollup: String.t(),
          aggregate: String.t(),
          window_ms: non_neg_integer(),
          start_ms: integer(),
          end_ms: integer(),
          step_ms: pos_integer(),
          timestamps: [integer()],
          labels: [String.t()],
          name_in_key: boolean()
        }

  @typedoc "What the query answered and read: the groups, and the samples scanned."
  @type stats :: Eval.stats()

  @doc """
  The plan for `node` over the grid of `context` (`SmolqueryVictoriaMetrics.Eval`'s),
  or `:none` when its shape is not one that is pushed.
  """
  @spec plan(AggrFuncExpr.t(), map()) :: {:ok, t()} | :none
  def plan(%AggrFuncExpr{name: name, args: [arg], modifier: modifier, limit: nil}, context)
      when name in @aggregates do
    with {:ok, by} <- grouping(modifier),
         {:ok, rollup, selector, window, keeps} <- rollup(arg, context),
         :ok <- bounded(rollup, window, context) do
      {:ok,
       %__MODULE__{
         selector: selector,
         rollup: rollup,
         aggregate: name,
         window_ms: window,
         start_ms: context.start_ms,
         end_ms: context.end_ms,
         step_ms: context.step_ms,
         timestamps: context.timestamps,
         labels: List.delete(by || [], "__name__"),
         name_in_key:
           by != nil and "__name__" in by and (keeps or Rollup.keeps_metric_name?(rollup))
       }}
    end
  end

  def plan(_node, _context), do: :none

  defp bounded(rollup, _window, _context) when rollup in @last, do: :ok

  defp bounded(_rollup, window, context) do
    if window <= @max_window_steps * context.step_ms, do: :ok, else: :none
  end

  defp grouping(nil), do: {:ok, nil}

  defp grouping(%Modifier{op: :by, labels: names}) when is_list(names),
    do: {:ok, Enum.uniq(names)}

  defp grouping(_modifier), do: :none

  defp rollup(%MetricExpr{} = selector, context),
    do: {:ok, "default_rollup", selector, window(0, "default_rollup", context), false}

  defp rollup(%RollupExpr{expr: %MetricExpr{} = selector} = rollup, context) do
    with {:ok, written} <- written_window(rollup, context),
         do: {:ok, "default_rollup", selector, window(written, "default_rollup", context), false}
  end

  defp rollup(%FuncExpr{name: name, args: [arg], keep_metric_names: keep}, context) do
    name = String.downcase(name)
    if name in @rollups, do: function_rollup(name, arg, keep, context), else: :none
  end

  defp rollup(_expr, _context), do: :none

  defp function_rollup(name, %MetricExpr{} = selector, keep, context),
    do: {:ok, name, selector, window(0, name, context), keep}

  defp function_rollup(name, %RollupExpr{expr: %MetricExpr{} = selector} = rollup, keep, context) do
    with {:ok, written} <- written_window(rollup, context),
         do: {:ok, name, selector, window(written, name, context), keep}
  end

  defp function_rollup(_name, _arg, _keep, _context), do: :none

  defp written_window(
         %RollupExpr{step: nil, offset: nil, at: nil, inherit_step: false, window: window},
         context
       ) do
    case resolve(window, context.step_ms) do
      ms when ms >= 0 -> {:ok, ms}
      _negative -> :none
    end
  end

  defp written_window(_rollup, _context), do: :none

  defp window(written, name, context) do
    case Rollup.window_ms(name, written, context.step_ms, context.lookback_ms) do
      0 -> context.step_ms
      window -> window
    end
  end

  defp resolve(nil, _step), do: 0
  defp resolve(%Duration{ms: ms, steps: steps}, step), do: Durations.resolve(ms, steps, step)

  @doc """
  The SQL for `plan` against the runtime's table, and its parameters.
  """
  @spec sql(t(), Runtime.t()) :: {:ok, String.t(), [term()]} | {:error, term()}
  def sql(%__MODULE__{} = plan, %Runtime{} = runtime) do
    with {:ok, predicate, params} <-
           Samples.where(plan.selector, plan.start_ms - plan.window_ms, plan.end_ms) do
      bound = length(params)

      label_columns =
        plan.labels
        |> Enum.with_index()
        |> Enum.map(fn {_label, index} -> {"k#{index}", "labels[$#{bound + index + 1}]"} end)

      params = params ++ plan.labels
      grid = length(params)
      params = params ++ [plan.start_ms, plan.step_ms, plan.window_ms, length(plan.timestamps)]

      refs = %{
        start: "$#{grid + 1}",
        step: "$#{grid + 2}",
        window: "$#{grid + 3}",
        points: "$#{grid + 4}"
      }

      keys = name_key(plan) ++ Enum.map(label_columns, &elem(&1, 0))
      by = Enum.map_join(keys, &(", " <> &1))

      read =
        ["series" | name_key(plan)] ++
          Enum.map(label_columns, fn {key, extract} -> "#{extract} AS #{key}" end) ++
          ["epoch_ms(ts) AS ts_ms", value_column()]

      sql = [
        "WITH s AS (SELECT ",
        Enum.intersperse(read, ", "),
        " FROM #{Samples.table(runtime)} WHERE #{predicate}), ",
        "e AS (SELECT series#{by}, value, ",
        "unnest(generate_series(#{first_point(refs)}, #{last_point(plan, refs)})) AS k ",
        "FROM #{expanded(plan)}), ",
        stages(plan, by),
        "c AS (SELECT count(*) AS samples FROM s) ",
        "SELECT #{refs.start} + a.k * #{refs.step} AS t, a.*, c.samples ",
        "FROM c LEFT JOIN a ON true"
      ]

      {:ok, IO.iodata_to_binary(sql), params}
    end
  end

  defp name_key(%__MODULE__{name_in_key: true}), do: ["name"]
  defp name_key(_plan), do: []

  defp value_column do
    "CASE WHEN value >= #{@inf} THEN 'infinity'::DOUBLE " <>
      "WHEN value <= -#{@inf} THEN '-infinity'::DOUBLE ELSE value END AS value"
  end

  defp first_point(refs),
    do: "greatest(0, CAST(ceil((ts_ms - #{refs.start}) / #{refs.step}) AS BIGINT))"

  defp last_point(%__MODULE__{rollup: rollup}, refs) when rollup in @last do
    "least(#{refs.points} - 1, #{point_before("ts_ms + #{refs.window}", refs)}, " <>
      "#{point_before("coalesce(next_ts, ts_ms + #{refs.window})", refs)})"
  end

  defp last_point(_plan, refs),
    do: "least(#{refs.points} - 1, #{point_before("ts_ms + #{refs.window}", refs)})"

  defp point_before(stop, refs),
    do: "CAST(floor((#{stop} - 1 - #{refs.start}) / #{refs.step}) AS BIGINT)"

  defp expanded(%__MODULE__{rollup: rollup}) when rollup in @last do
    "(SELECT *, lead(ts_ms) OVER (PARTITION BY series ORDER BY ts_ms, value) AS next_ts FROM s)"
  end

  defp expanded(_plan), do: "s"

  defp stages(%__MODULE__{rollup: rollup, aggregate: aggregate}, by) when rollup in @last,
    do: single(aggregate_column(aggregate, "value"), by)

  defp stages(%__MODULE__{rollup: "count_over_time", aggregate: "sum"}, by),
    do: single("count(*)::DOUBLE", by)

  defp stages(%__MODULE__{rollup: rollup, aggregate: "count"}, by)
       when rollup in ["count_over_time", "min_over_time", "max_over_time"],
       do: single("count(DISTINCT series)::DOUBLE", by)

  defp stages(%__MODULE__{rollup: "min_over_time", aggregate: "min"}, by),
    do: single("min(value)", by)

  defp stages(%__MODULE__{rollup: "max_over_time", aggregate: "max"}, by),
    do: single("max(value)", by)

  defp stages(%__MODULE__{rollup: rollup, aggregate: aggregate}, by) do
    "r AS (SELECT k#{by}, series, #{Map.fetch!(@inner, rollup)} AS v " <>
      "FROM e GROUP BY k#{by}, series), " <>
      "a AS (SELECT k#{by}, #{aggregate_column(aggregate, "v")} AS value FROM r GROUP BY k#{by}), "
  end

  defp single(expression, by),
    do: "a AS (SELECT k#{by}, #{expression} AS value FROM e GROUP BY k#{by}), "

  defp aggregate_column("count", column),
    do: "nullif(count(#{column}) FILTER (WHERE NOT isnan(#{column})), 0)::DOUBLE"

  defp aggregate_column(aggregate, column),
    do: "#{aggregate}(#{column}) FILTER (WHERE NOT isnan(#{column}))"

  @doc """
  Runs `plan` through the runtime's query service: the answer's series and
  what was scanned. `opts` are `Smolquery.QueryService.Client.query/3`'s.
  """
  @spec run(Runtime.t(), t(), keyword()) :: {:ok, [Series.t()], stats()} | {:error, term()}
  def run(%Runtime{} = runtime, %__MODULE__{} = plan, opts \\ []) do
    max = runtime.max_series
    rows = max * length(plan.timestamps) + 1

    with {:ok, sql, params} <- sql(plan, runtime),
         {:ok, frame} <- job(runtime, sql, params, Keyword.put(opts, :result_max_rows, rows)) do
      series(frame, plan, max)
    end
  end

  defp job(runtime, sql, params, opts) do
    case Samples.run(runtime, sql, params, opts) do
      {:error, {:job, {:result_too_large, _rows}}} ->
        {:error, {:too_many_series, runtime.max_series}}

      result ->
        result
    end
  end

  @doc """
  The answer's series from the query's frame, `nil` when the table does not
  exist: one per group, in no order, `nil` at a point with no value; or
  `{:error, {:too_many_series, max}}` past `max` groups.
  """
  @spec series(DataFrame.t() | nil, t(), pos_integer()) ::
          {:ok, [Series.t()], stats()} | {:error, {:too_many_series, pos_integer()}}
  def series(nil, _plan, _max), do: {:ok, [], %{series: 0, samples: 0}}

  def series(%DataFrame{} = frame, %__MODULE__{} = plan, max) do
    groups =
      frame
      |> points(plan)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    if map_size(groups) > max do
      {:error, {:too_many_series, max}}
    else
      series =
        for {labels, points} <- groups do
          at = Map.new(points)
          %Series{labels: labels, values: Enum.map(plan.timestamps, &{&1, Map.get(at, &1)})}
        end

      {:ok, series, %{series: map_size(groups), samples: samples(frame)}}
    end
  end

  defp points(frame, plan) do
    keys = key_columns(frame, plan)

    [column(frame, "t"), column(frame, "value") | keys]
    |> Enum.zip_with(fn
      [nil | _rest] -> nil
      [t, value | key] -> {labels(key, plan), {t, Value.from_number(value)}}
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp key_columns(frame, plan) do
    names = if plan.name_in_key, do: ["name"], else: []
    Enum.map(names ++ Enum.map(0..(length(plan.labels) - 1)//1, &"k#{&1}"), &column(frame, &1))
  end

  defp column(frame, name), do: ExplorerSeries.to_list(frame[name])

  defp labels(key, %__MODULE__{name_in_key: true} = plan) do
    [name | rest] = key
    labels(rest, %{plan | name_in_key: false}) |> put_label("__name__", name)
  end

  defp labels(key, plan) do
    plan.labels
    |> Enum.zip(key)
    |> Enum.reduce(%{}, fn {label, value}, acc -> put_label(acc, label, value) end)
  end

  defp put_label(labels, _label, nil), do: labels
  defp put_label(labels, label, value), do: Map.put(labels, label, value)

  defp samples(frame) do
    case ExplorerSeries.to_list(frame["samples"]) do
      [count | _rest] -> count
      [] -> 0
    end
  end
end
