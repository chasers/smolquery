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

    * `agg` is `sum`, `min`, `max`, `avg`, `count`, `group`, `stddev`,
      `stdvar`, `distinct`, `sum2`, `median` or `quantile(phi, ...)` with a
      number for `phi`, with `by (...)`, `without (...)` or no modifier, and
      no `limit`;
    * `rollup` is `default_rollup` (a bare selector, or `m[5m]`),
      `last_over_time`, `sum_over_time`, `count_over_time`, `min_over_time`,
      `max_over_time` or `avg_over_time`; one of the rollups read off a
      window's edges (`SmolqueryVictoriaMetrics.Pushdown.Windows`, T-585):
      `rate`, `deriv_fast`, `increase`, `increase_pure`, `delta`, `idelta`,
      `irate`, `ideriv`, `first_over_time`, `present_over_time`; or one of
      the rollups read over a window whole
      (`SmolqueryVictoriaMetrics.Pushdown.Whole`, T-586): `sum2_over_time`,
      `range_over_time`, `distinct_over_time`, `tmin_over_time`,
      `tmax_over_time`, `timestamp`, `stddev_over_time`, `stdvar_over_time`,
      `geomean_over_time`, `rate_over_sum`, `changes`, `resets`, `lag`,
      `lifetime`, `scrape_interval`, and `quantile_over_time` and the
      `count_*_over_time` family with a number for their argument; over a
      selector with at most a window and an `offset`: no `@`, no subquery.

  Anything else, `topk`, `count_values`, an argument that is not a number
  literal, an aggregate over an expression, evaluates in Elixir as before.
  A `without` key is the labels map less the listed labels; a `by` key the
  listed labels. An `offset` moves the grid
  and the read back and the answer forward again, as
  `SmolqueryVictoriaMetrics.Eval` does. A transform, a binary operator or
  `topk` over a pushed aggregate already evaluates over the pushed result:
  `histogram_quantile(0.9, sum by (le) (rate(b[5m])))` and
  `sum(rate(a[5m])) / sum(rate(b[5m]))` read no sample into the node.

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
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.Pushdown.Whole
  alias SmolqueryVictoriaMetrics.Pushdown.Windows
  alias SmolqueryVictoriaMetrics.Rollup
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Samples

  @aggregates ~w(sum min max avg count group stddev stdvar quantile median distinct sum2)
  @last ~w(default_rollup last_over_time)
  @inner %{
    "sum_over_time" => "sum(value)",
    "count_over_time" => "count(value)::DOUBLE",
    "min_over_time" => "min(value)",
    "max_over_time" => "max(value)",
    "avg_over_time" => "avg(value)"
  }
  @windows Windows.functions()
  @whole Whole.functions()
  @rollups @last ++ Map.keys(@inner) ++ @windows ++ @whole
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
    :labels,
    :read_from_ms
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
    :read_from_ms,
    :phi,
    :scalar,
    :without,
    name_in_key: false,
    adjust_window: false,
    offset_ms: 0
  ]

  @typedoc """
  A pushed aggregate: the selector, the rollup and aggregate names, the
  window and grid in milliseconds (the grid moved back by `offset_ms`, the
  selector's `offset`, and the answer moved forward again), where the read
  starts, the `by` labels other than `__name__` or the `without` labels,
  and whether `__name__` is a group key too. `adjust_window` says the
  window is `max(step, max_prev)` per series, a rate with no window
  written. `phi` is `quantile`'s (`0.5` for `median`) and `scalar` the
  rollup's number argument, when there is one.
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
          read_from_ms: integer(),
          phi: float() | nil,
          scalar: float() | nil,
          without: [String.t()] | nil,
          name_in_key: boolean(),
          adjust_window: boolean(),
          offset_ms: integer()
        }

  @typedoc "What the query answered and read: the groups, and the samples scanned."
  @type stats :: Eval.stats()

  @doc """
  The plan for `node` over the grid of `context` (`SmolqueryVictoriaMetrics.Eval`'s),
  or `:none` when its shape is not one that is pushed.
  """
  @spec plan(AggrFuncExpr.t(), map()) :: {:ok, t()} | :none
  def plan(%AggrFuncExpr{name: name, args: args, modifier: modifier, limit: nil}, context)
      when name in @aggregates do
    with {:ok, phi, arg} <- aggregate_args(name, args),
         {:ok, grouping} <- grouping(modifier),
         {:ok, rollup} <- rollup(arg, context),
         :ok <- bounded(rollup, context) do
      offset = rollup.offset_ms
      {by, without} = grouping

      {:ok,
       %__MODULE__{
         selector: rollup.selector,
         rollup: rollup.name,
         aggregate: name,
         phi: phi,
         scalar: rollup.scalar,
         window_ms: rollup.window_ms,
         adjust_window: rollup.adjust_window,
         offset_ms: offset,
         read_from_ms: context.start_ms - offset - read_back(rollup, context),
         start_ms: context.start_ms - offset,
         end_ms: context.end_ms - offset,
         step_ms: context.step_ms,
         timestamps: Enum.map(context.timestamps, &(&1 - offset)),
         labels: List.delete(by || [], "__name__"),
         without: without,
         name_in_key:
           by != nil and "__name__" in by and
             (rollup.keep or Rollup.keeps_metric_name?(rollup.name))
       }}
    end
  end

  def plan(_node, _context), do: :none

  defp aggregate_args("quantile", [%Number{value: phi}, arg]) when is_number(phi),
    do: {:ok, phi * 1.0, arg}

  defp aggregate_args("median", [arg]), do: {:ok, 0.5, arg}
  defp aggregate_args(name, [arg]) when name not in ["quantile"], do: {:ok, nil, arg}
  defp aggregate_args(_name, _args), do: :none

  defp read_back(%{name: name, written_ms: written}, context)
       when name in @windows or name in @whole,
       do: max(written, context.step_ms) + context.lookback_ms

  defp read_back(%{window_ms: window}, _context), do: window

  defp bounded(%{name: rollup}, _context) when rollup in @last, do: :ok

  defp bounded(%{window_ms: window}, context) do
    if window <= @max_window_steps * context.step_ms, do: :ok, else: :none
  end

  defp grouping(nil), do: {:ok, {nil, nil}}

  defp grouping(%Modifier{op: :by, labels: names}) when is_list(names),
    do: {:ok, {Enum.uniq(names), nil}}

  defp grouping(%Modifier{op: :without, labels: names}) when is_list(names),
    do: {:ok, {nil, Enum.uniq(names)}}

  defp grouping(_modifier), do: :none

  defp rollup(%MetricExpr{} = selector, context),
    do: {:ok, described("default_rollup", selector, 0, 0, false, nil, context)}

  defp rollup(%RollupExpr{expr: %MetricExpr{} = selector} = rollup, context) do
    with {:ok, written, offset} <- written(rollup, context),
         do: {:ok, described("default_rollup", selector, written, offset, false, nil, context)}
  end

  defp rollup(%FuncExpr{name: name, args: args, keep_metric_names: keep}, context) do
    name = String.downcase(name)

    with true <- name in @rollups,
         {:ok, arg, scalar} <- function_args(name, args) do
      function_rollup(name, arg, keep, scalar, context)
    else
      _other -> :none
    end
  end

  defp rollup(_expr, _context), do: :none

  defp function_args(name, args) do
    index = Rollup.series_arg_index(name)

    case {Enum.at(args, index), List.delete_at(args, index)} do
      {nil, _scalars} -> :none
      {arg, []} -> if Whole.scalar?(name), do: :none, else: {:ok, arg, nil}
      {arg, [%Number{value: scalar}]} when is_number(scalar) -> scalar_arg(name, arg, scalar)
      _other -> :none
    end
  end

  defp scalar_arg(name, arg, scalar) do
    if Whole.scalar?(name), do: {:ok, arg, scalar * 1.0}, else: :none
  end

  defp function_rollup(name, %MetricExpr{} = selector, keep, scalar, context),
    do: {:ok, described(name, selector, 0, 0, keep, scalar, context)}

  defp function_rollup(
         name,
         %RollupExpr{expr: %MetricExpr{} = selector} = rollup,
         keep,
         scalar,
         context
       ) do
    with {:ok, written, offset} <- written(rollup, context),
         do: {:ok, described(name, selector, written, offset, keep, scalar, context)}
  end

  defp function_rollup(_name, _arg, _keep, _scalar, _context), do: :none

  defp written(%RollupExpr{step: nil, at: nil, inherit_step: false} = rollup, context) do
    case resolve(rollup.window, context.step_ms) do
      ms when ms >= 0 -> {:ok, ms, resolve(rollup.offset, context.step_ms)}
      _negative -> :none
    end
  end

  defp written(_rollup, _context), do: :none

  defp described(name, selector, written, offset, keep, scalar, context) do
    resolved = Rollup.window_ms(name, written, context.step_ms, context.lookback_ms)

    %{
      name: name,
      selector: selector,
      written_ms: written,
      window_ms: if(resolved == 0, do: context.step_ms, else: resolved),
      adjust_window: resolved == 0 and Rollup.may_adjust_window?(name),
      offset_ms: offset,
      keep: keep,
      scalar: scalar
    }
  end

  defp resolve(nil, _step), do: 0
  defp resolve(%Duration{ms: ms, steps: steps}, step), do: Durations.resolve(ms, steps, step)

  @doc """
  The SQL for `plan` against the runtime's table, and its parameters.
  """
  @spec sql(t(), Runtime.t()) :: {:ok, String.t(), [term()]} | {:error, term()}
  def sql(%__MODULE__{} = plan, %Runtime{} = runtime) do
    with {:ok, predicate, params} <-
           Samples.where(plan.selector, plan.read_from_ms, plan.end_ms) do
      {key_columns, params} = key_columns(plan, params)
      grid = length(params)
      scalars = Enum.reject([plan.phi, plan.scalar], &is_nil/1)

      params =
        params ++
          [plan.start_ms, plan.step_ms, plan.window_ms, length(plan.timestamps)] ++ scalars

      refs =
        %{
          start: "$#{grid + 1}",
          step: "$#{grid + 2}",
          window: "$#{grid + 3}",
          points: "$#{grid + 4}"
        }
        |> put_ref(:phi, plan.phi, grid + 5)
        |> put_ref(:scalar, plan.scalar, grid + 4 + length(scalars))

      keys = name_key(plan) ++ Enum.map(key_columns, &elem(&1, 0))
      by = Enum.map_join(keys, &(", " <> &1))

      read =
        ["series" | name_key(plan)] ++
          Enum.map(key_columns, fn {key, extract} -> "#{extract} AS #{key}" end) ++
          ["epoch_ms(ts) AS ts_ms", value_column()]

      sql = [
        "WITH s AS (SELECT ",
        Enum.intersperse(read, ", "),
        " FROM #{Samples.table(runtime)} WHERE #{predicate}), ",
        unnested(plan, refs, by),
        stages(plan, keys, by, refs),
        "c AS (SELECT count(*) AS samples FROM s) ",
        "SELECT #{refs.start} + a.k * #{refs.step} AS t, a.*, c.samples ",
        "FROM c LEFT JOIN a ON true"
      ]

      {:ok, IO.iodata_to_binary(sql), params}
    end
  end

  defp put_ref(refs, _key, nil, _position), do: refs
  defp put_ref(refs, key, _value, position), do: Map.put(refs, key, "$#{position}")

  defp key_columns(%__MODULE__{without: names}, params) when is_list(names) do
    bound = length(params)

    entries =
      case names do
        [] ->
          "map_entries(labels)"

        _some ->
          dropped = Enum.map_join(1..length(names)//1, ", ", &"$#{bound + &1}")
          "list_filter(map_entries(labels), e -> e.key NOT IN (#{dropped}))"
      end

    {[{"kw", "map_from_entries(#{entries})"}], params ++ names}
  end

  defp key_columns(%__MODULE__{labels: labels}, params) do
    bound = length(params)

    columns =
      labels
      |> Enum.with_index()
      |> Enum.map(fn {_label, index} -> {"k#{index}", "labels[$#{bound + index + 1}]"} end)

    {columns, params ++ labels}
  end

  defp name_key(%__MODULE__{name_in_key: true}), do: ["name"]
  defp name_key(_plan), do: []

  defp value_column do
    "CASE WHEN value >= #{@inf} THEN 'infinity'::DOUBLE " <>
      "WHEN value <= -#{@inf} THEN '-infinity'::DOUBLE ELSE value END AS value"
  end

  defp unnested(%__MODULE__{rollup: rollup}, _refs, _by)
       when rollup in @windows or rollup in @whole,
       do: []

  defp unnested(plan, refs, by) do
    "e AS (SELECT series#{by}, value, " <>
      "unnest(generate_series(#{first_point(refs)}, #{last_point(plan, refs)})) AS k " <>
      "FROM #{expanded(plan)}), "
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

  defp stages(%__MODULE__{rollup: rollup} = plan, keys, by, refs)
       when rollup in @windows or rollup in @whole do
    instant? = plan.start_ms == plan.end_ms

    [
      Windows.stages(rollup, refs, keys, instant?, plan.adjust_window),
      single(aggregate_column(plan, "v", refs), by)
    ]
  end

  defp stages(%__MODULE__{rollup: rollup} = plan, _keys, by, refs) when rollup in @last,
    do: single(aggregate_column(plan, "value", refs), by)

  defp stages(%__MODULE__{rollup: "count_over_time", aggregate: "sum"}, _keys, by, _refs),
    do: single("count(*)::DOUBLE", by)

  defp stages(%__MODULE__{rollup: rollup, aggregate: "count"}, _keys, by, _refs)
       when rollup in ["count_over_time", "min_over_time", "max_over_time"],
       do: single("count(DISTINCT series)::DOUBLE", by)

  defp stages(%__MODULE__{rollup: "min_over_time", aggregate: "min"}, _keys, by, _refs),
    do: single("min(value)", by)

  defp stages(%__MODULE__{rollup: "max_over_time", aggregate: "max"}, _keys, by, _refs),
    do: single("max(value)", by)

  defp stages(%__MODULE__{rollup: rollup} = plan, _keys, by, refs) do
    "r AS (SELECT k#{by}, series, #{Map.fetch!(@inner, rollup)} AS v " <>
      "FROM e GROUP BY k#{by}, series), " <>
      "a AS (SELECT k#{by}, #{aggregate_column(plan, "v", refs)} AS value FROM r GROUP BY k#{by}), "
  end

  defp single(expression, by),
    do: "a AS (SELECT k#{by}, #{expression} AS value FROM e GROUP BY k#{by}), "

  defp aggregate_column(%__MODULE__{aggregate: "count"}, column, _refs),
    do: "nullif(count(#{column}) FILTER (WHERE NOT isnan(#{column})), 0)::DOUBLE"

  defp aggregate_column(%__MODULE__{aggregate: "group"}, column, _refs),
    do: "CASE WHEN count(#{column}) FILTER (WHERE NOT isnan(#{column})) > 0 THEN 1.0::DOUBLE END"

  defp aggregate_column(%__MODULE__{aggregate: "sum2"}, column, _refs),
    do: "sum(#{column} * #{column}) FILTER (WHERE NOT isnan(#{column}))"

  defp aggregate_column(%__MODULE__{aggregate: "distinct"}, column, _refs) do
    "nullif(count(DISTINCT CASE WHEN #{column} = 0 THEN 0.0::DOUBLE ELSE #{column} END) " <>
      "FILTER (WHERE NOT isnan(#{column})), 0)::DOUBLE"
  end

  defp aggregate_column(%__MODULE__{aggregate: aggregate}, column, _refs)
       when aggregate in ["stddev", "stdvar"] do
    ordered = "list(#{column} ORDER BY series) FILTER (WHERE NOT isnan(#{column}))"
    variance = "#{Whole.welford(ordered)}.q / len(#{ordered})"
    answer = if aggregate == "stddev", do: "sqrt(#{variance})", else: variance

    "CASE WHEN len(#{ordered}) = 0 THEN NULL WHEN len(#{ordered}) = 1 THEN 0.0::DOUBLE " <>
      "ELSE #{answer} END"
  end

  defp aggregate_column(%__MODULE__{aggregate: aggregate}, column, refs)
       when aggregate in ["quantile", "median"] do
    values = "list(#{column}) FILTER (WHERE NOT isnan(#{column}))"

    "CASE WHEN len(#{values}) = 0 THEN NULL WHEN #{refs.phi} < 0 THEN '-infinity'::DOUBLE " <>
      "WHEN #{refs.phi} > 1 THEN 'infinity'::DOUBLE " <>
      "ELSE #{Whole.quantile("list_sort(#{values})", refs.phi)} END"
  end

  defp aggregate_column(%__MODULE__{aggregate: aggregate}, column, _refs),
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

          %Series{
            labels: labels,
            values: Enum.map(plan.timestamps, &{&1 + plan.offset_ms, Map.get(at, &1)})
          }
        end

      {:ok, series, %{series: map_size(groups), samples: samples(frame)}}
    end
  end

  defp points(frame, plan) do
    keys = key_lists(frame, plan)

    [column(frame, "t"), column(frame, "value") | keys]
    |> Enum.zip_with(fn
      [nil | _rest] -> nil
      [t, value | key] -> {labels(key, plan), {t, Value.from_number(value)}}
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp key_lists(frame, %__MODULE__{without: names}) when is_list(names),
    do: [column(frame, "kw")]

  defp key_lists(frame, plan) do
    names = if plan.name_in_key, do: ["name"], else: []
    Enum.map(names ++ Enum.map(0..(length(plan.labels) - 1)//1, &"k#{&1}"), &column(frame, &1))
  end

  defp column(frame, name), do: ExplorerSeries.to_list(frame[name])

  defp labels([entries], %__MODULE__{without: names}) when is_list(names),
    do: Map.new(entries || [], &{&1["key"], &1["value"]})

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
