defmodule SmolqueryVictoriaMetrics.Eval do
  @moduledoc """
  Evaluates a parsed MetricsQL expression over a step grid (PL-70, T-564,
  T-565), as VictoriaMetrics v1.152.0's `app/vmselect/promql/eval.go` does.

  The grid is `start, start + step, ...` up to `end`; an instant query is a
  grid of one point. Everything evaluates to a list of
  `SmolqueryVictoriaMetrics.Eval.Series`, each with a value, possibly `nil`,
  at every point of the grid. A number, `time()` or `scalar(x)` is one
  series with no labels, as VictoriaMetrics evaluates a scalar; `scalar?/1`
  says whether an expression always is one, which an instant query answers
  as `resultType: "scalar"`.

  Before evaluating, the expression is prepared as VictoriaMetrics prepares
  it (`SmolqueryVictoriaMetrics.Eval.Constants`): constants are folded and
  `0.5 < q` is turned into `q > 0.5`.

  ## What evaluates

    * numbers, durations (their seconds) and strings;
    * a selector, `default_rollup` over it, and a rollup function over a
      selector, both run by `SmolqueryVictoriaMetrics.Rollup` over the raw
      samples `fetch` reads;
    * a rollup function over anything else, a subquery `q[5m:1m]`, `q[5m:]`
      at the query's step, or `q[5m]` and `q offset 1h` on any expression
      that is not a selector (`evalRollupFuncWithSubquery`): `q` is
      evaluated on its own grid from `start - window - step - 5m` to
      `end + step` at the subquery step, both ends aligned to that step, at
      most 100,000 points, and the rollup then runs over its points with a
      value, as over raw samples;
    * `offset d` on any of these evaluates on the grid moved back by `d`
      and answers at the grid's own points; `@ t` evaluates at `t` alone
      and answers that value at every point; `t` is any expression with one
      series, `start()` and `end()` included;
    * aggregates (`SmolqueryVictoriaMetrics.Eval.Aggregate`), binary
      operators (`SmolqueryVictoriaMetrics.Eval.Binary`), transforms
      (`SmolqueryVictoriaMetrics.Eval.Transform`) and unions `(a, b)`.

  Rollup functions `SmolqueryVictoriaMetrics.Rollup` does not compute,
  aggregate and transform functions not ported, answer
  `{:error, {:unsupported, what}}` naming them.

  ## A rollup over a selector

  As `evalRollupFuncWithMetricExpr` and `evalRollupFuncNoCache`:

    * the window is the one written, resolved at the step (`5m`, `2i`), and
      one that resolves below zero (`5m-10m`) is refused with
      `{:invalid_argument, "duration cannot be negative; ..."}`; a
      window not written is `0`, which `SmolqueryVictoriaMetrics.Rollup`
      reads as the step, widened for the functions that may widen it. For
      `default_rollup` alone, the bare selector's function, a window not
      written is `max(step, lookback_ms)`: a sample is current for
      `lookback_ms` after it was taken, as `SmolqueryVictoriaMetrics.Runtime`
      configures;
    * the samples are read once per selector, for
      `[start - offset - max(window, step) - lookback_ms, end - offset]`,
      which holds every window and the sample before the first one;
    * a rollup drops `__name__` unless it is one that keeps it
      (`Rollup.keeps_metric_name?/1`) or the call says `keep_metric_names`;
      `absent_over_time` answers one series, labelled by the selector's `=`
      matchers, `1` where no series had a sample.

  ## The answer

  As `promql.Exec`: series with no value at any point are dropped, the rest
  are sorted by name, then labels, unless the expression orders them itself
  (`sort`, `topk`, `or`, ...), and two series left with the same labels are
  an error (`duplicate output timeseries`).
  """

  alias SmolqueryVictoriaMetrics.Eval.Aggregate
  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Binary
  alias SmolqueryVictoriaMetrics.Eval.Constants
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Transform
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.MetricsQL.Functions
  alias SmolqueryVictoriaMetrics.Rollup
  alias SmolqueryVictoriaMetrics.Samples

  @subquery_max_points 100_000
  @silence_ms 300_000

  @typedoc "What an expression evaluates to."
  @type result :: [Series.t()]

  @typedoc """
  Reads a selector's series with their samples over `{from_ms, to_ms}`, as
  `SmolqueryVictoriaMetrics.Samples.select/4` does.
  """
  @type fetch ::
          (MetricExpr.t(), {integer(), integer()} ->
             {:ok, [Samples.series()]} | {:error, term()})

  @typedoc """
  The grid, the edge's `lookback_ms`, the grid's point ceiling, and how to
  read a selector.
  """
  @type context :: %{
          required(:start_ms) => integer(),
          required(:end_ms) => integer(),
          required(:step_ms) => pos_integer(),
          required(:lookback_ms) => pos_integer(),
          required(:max_points) => pos_integer(),
          required(:fetch) => fetch(),
          optional(:timestamps) => [integer()]
        }

  @typedoc "What evaluating read: the series and the raw samples fetched."
  @type stats :: %{series: non_neg_integer(), samples: non_neg_integer()}

  @type reason ::
          {:unsupported, String.t()}
          | {:too_many_points, String.t()}
          | {:duplicate_series, String.t()}
          | {:invalid_at, String.t()}
          | Args.reason()
          | term()

  @doc """
  Evaluates `expr` over the grid of `context` and prepares the answer: no
  empty series, no duplicates, sorted unless the expression orders them.
  """
  @spec run(Ast.expr(), context()) :: {:ok, result(), stats()} | {:error, reason()}
  def run(expr, context) do
    expr = Constants.prepare(expr)

    with {:ok, timestamps} <- grid(context),
         {:ok, series, stats} <- eval(expr, Map.put(context, :timestamps, timestamps)),
         {:ok, series} <- answer(series, Constants.may_sort?(expr)) do
      {:ok, series, stats}
    end
  end

  @doc """
  Whether `expr` always evaluates to a scalar: a number, a duration,
  `time()`, `scalar(x)` and the like, or an operator between two of them.
  """
  @spec scalar?(Ast.expr()) :: boolean()
  def scalar?(expr), do: expr |> Constants.prepare() |> Constants.scalar?()

  @doc """
  The raw samples of `selector[window] offset d` in `(t - d - window, t - d]`,
  each series with its own timestamps, `__name__` kept: what an instant
  query of a bare range vector answers, as VictoriaMetrics' `QueryHandler`
  exports it instead of evaluating it.
  """
  @spec raw(RollupExpr.t(), integer(), context()) ::
          {:ok, [Series.t()], stats()} | {:error, reason()}
  def raw(%RollupExpr{expr: %MetricExpr{} = selector} = rollup, time_ms, context) do
    step = context.step_ms
    to = time_ms - resolve(rollup.offset, step)
    from = to - resolve(rollup.window, step) + 1

    with :ok <- non_negative(rollup.window, step),
         {:ok, fetched} <- context.fetch.(selector, {from, to}) do
      series =
        for %{labels: labels, timestamps: timestamps, values: values} <- fetched,
            points = within(timestamps, values, from, to),
            points != [],
            do: %Series{labels: labels, values: points}

      {:ok, Series.sort(series), stats(fetched)}
    end
  end

  defp within(timestamps, values, from, to) do
    timestamps
    |> Enum.zip(values)
    |> Enum.filter(fn {t, _v} -> t >= from and t <= to end)
  end

  @doc """
  Whether an instant query of `expr` answers raw samples (`raw/3`): a
  selector with a window, and at most an `offset`.
  """
  @spec raw?(Ast.expr()) :: boolean()
  def raw?(%RollupExpr{expr: %MetricExpr{}, window: %Duration{}, step: nil, at: nil} = rollup),
    do: rollup.inherit_step == false

  def raw?(_expr), do: false

  @doc """
  What an instant query of a window on anything but a bare selector
  answers, `q[5m]`, `q[5m:1m]`, `m[5m:1m]` (VictoriaMetrics' `IsRollup`):
  `q` over the range query `[t - offset - window, t - offset]` at the
  subquery's step, or the request's. `:none` for any other expression, and
  for a window that resolves below zero, which `run/2` refuses.
  """
  @spec instant_range(Ast.expr(), integer(), pos_integer()) ::
          {:ok, Ast.expr(), {integer(), integer(), pos_integer()}} | :none
  def instant_range(%RollupExpr{window: %Duration{}, at: nil} = rollup, time_ms, step_ms) do
    if raw?(rollup) or non_negative(rollup.window, step_ms) != :ok do
      :none
    else
      step = with 0 <- resolve(rollup.step, step_ms), do: step_ms
      finish = time_ms - resolve(rollup.offset, step)
      {:ok, rollup.expr, {finish - resolve(rollup.window, step), finish, step}}
    end
  end

  def instant_range(_expr, _time_ms, _step_ms), do: :none

  @doc """
  A series' labels as PromQL writes a selector: `name{k="v", ...}`.
  """
  @spec describe(%{String.t() => String.t()}) :: String.t()
  def describe(labels), do: Series.describe(labels)

  defp grid(context),
    do: Rollup.grid(context.start_ms, context.end_ms, context.step_ms, context.max_points)

  defp with_grid(context, start, finish, step, max_points \\ nil) do
    {:ok, timestamps} = Rollup.grid(start, finish, step, nil)

    %{
      context
      | start_ms: start,
        end_ms: finish,
        step_ms: step,
        max_points: max_points || context.max_points
    }
    |> Map.put(:timestamps, timestamps)
  end

  defp eval(%Number{value: value}, context),
    do: {:ok, [Series.constant(context.timestamps, Value.from_number(value))], empty()}

  defp eval(%Duration{ms: ms, steps: steps}, context) do
    seconds = Durations.resolve(ms, steps, context.step_ms) / 1000
    {:ok, [Series.constant(context.timestamps, seconds)], empty()}
  end

  defp eval(%StringLiteral{value: text}, context),
    do: {:ok, [Series.string(context.timestamps, text)], empty()}

  defp eval(%MetricExpr{} = selector, context),
    do: rollup("default_rollup", [], %RollupExpr{expr: selector}, false, context)

  defp eval(%RollupExpr{} = rollup, context),
    do: rollup("default_rollup", [], rollup, false, context)

  defp eval(%FuncExpr{name: name} = call, context) do
    case Functions.kind(name) do
      :rollup -> rollup_call(call, context)
      _transform -> transform_call(call, context)
    end
  end

  defp eval(%AggrFuncExpr{name: name} = node, context) do
    if name in Aggregate.functions() do
      with {:ok, args, stats} <- eval_all(node.args, context),
           {:ok, series} <- Aggregate.apply(node, args, context.timestamps),
           do: {:ok, series, stats}
    else
      {:error, {:unsupported, "aggregate function #{name}()"}}
    end
  end

  defp eval(%BinaryOpExpr{} = node, context) do
    with {:ok, [left, right], stats} <- eval_all([node.left, node.right], context),
         {:ok, series} <- Binary.apply(node, left, right),
         do: {:ok, series, stats}
  end

  defp eval(%ParensExpr{exprs: exprs}, context) do
    with {:ok, args, stats} <- eval_all(exprs, context),
         do: {:ok, Transform.union(args, context.timestamps), stats}
  end

  defp eval_all(exprs, context) do
    with {:ok, results} <- Args.collect(exprs, &eval_one(&1, context)) do
      stats = results |> Enum.map(&elem(&1, 1)) |> Enum.reduce(empty(), &merge/2)
      {:ok, Enum.map(results, &elem(&1, 0)), stats}
    end
  end

  defp eval_one(expr, context) do
    with {:ok, series, stats} <- eval(expr, context), do: {:ok, {series, stats}}
  end

  defp transform_call(%FuncExpr{name: name} = call, context) do
    if String.downcase(name) in Transform.functions() do
      grid = Map.take(context, [:timestamps, :start_ms, :end_ms, :step_ms])

      with {:ok, args, stats} <- eval_all(call.args, context),
           {:ok, series} <- Transform.apply(call, args, grid),
           do: {:ok, series, stats}
    else
      {:error, {:unsupported, "transform function #{String.downcase(name)}()"}}
    end
  end

  defp rollup_call(%FuncExpr{name: name, args: args, keep_metric_names: keep}, context) do
    name = String.downcase(name)
    index = Rollup.series_arg_index(name)

    cond do
      not Rollup.supported?(name) ->
        {:error, {:unsupported, "rollup function #{name}()"}}

      Enum.at(args, index) == nil ->
        {:error, {:arity, "#{name}() is missing its series argument"}}

      true ->
        with {:ok, scalars, stats} <- rollup_scalars(List.delete_at(args, index), context),
             {:ok, series, more} <-
               rollup(name, scalars, rollup_arg(Enum.at(args, index)), keep, context),
             do: {:ok, series, merge(stats, more)}
    end
  end

  defp rollup_scalars(exprs, context) do
    with {:ok, args, stats} <- eval_all(exprs, context),
         {:ok, scalars} <- args |> Enum.with_index() |> Args.collect(&first_scalar/1),
         do: {:ok, scalars, stats}
  end

  defp first_scalar({arg, index}) do
    with {:ok, values} <- Args.scalar(arg, index), do: {:ok, List.first(values)}
  end

  defp rollup_arg(%RollupExpr{step: nil, inherit_step: false} = rollup), do: rollup

  defp rollup_arg(%RollupExpr{expr: %MetricExpr{} = selector} = rollup) do
    inner = %FuncExpr{name: "default_rollup", args: [%RollupExpr{expr: selector}]}
    %{rollup | expr: inner}
  end

  defp rollup_arg(%RollupExpr{} = rollup), do: rollup
  defp rollup_arg(expr), do: %RollupExpr{expr: expr}

  defp rollup(name, scalars, %RollupExpr{at: nil} = rollup, keep, context),
    do: rollup_without_at(name, scalars, rollup, keep, context)

  defp rollup(name, scalars, %RollupExpr{at: at} = rollup, keep, context) do
    with {:ok, at_series, stats} <- eval(at, context),
         {:ok, time} <- at_time(at_series),
         at_context = with_grid(context, time, time, context.step_ms),
         {:ok, series, more} <- rollup_without_at(name, scalars, rollup, keep, at_context) do
      spread =
        Enum.map(series, fn %Series{values: [{_t, value} | _rest]} = one ->
          %{one | values: Enum.map(context.timestamps, &{&1, value})}
        end)

      {:ok, spread, merge(stats, more)}
    end
  end

  defp at_time([series]) do
    case series |> Series.values() |> Enum.find(&(&1 != nil)) do
      nil -> {:error, {:invalid_at, "`@` modifier must return a non-NaN value"}}
      seconds -> {:ok, trunc(seconds * 1000)}
    end
  end

  defp at_time(series),
    do:
      {:error,
       {:invalid_at,
        "`@` modifier must return a single series; it returns #{length(series)} series instead"}}

  defp rollup_without_at(name, scalars, rollup, keep, context) do
    step = context.step_ms
    offset = resolve(rollup.offset, step)
    shifted = with_grid(context, context.start_ms - offset, context.end_ms - offset, step)

    result =
      case {non_negative(rollup.window, step), rollup.expr} do
        {{:error, reason}, _expr} ->
          {:error, reason}

        {:ok, %MetricExpr{} = selector} ->
          selector_rollup(name, scalars, selector, rollup, keep, shifted)

        {:ok, _inner} ->
          subquery_rollup(name, scalars, rollup, keep, shifted)
      end

    with {:ok, series, stats} <- result do
      series =
        series
        |> absent_over_time(name, rollup.expr, shifted)
        |> shift(offset)

      {:ok, series, stats}
    end
  end

  defp selector_rollup(name, scalars, selector, rollup, keep, context) do
    step = context.step_ms
    window = window(name, resolve(rollup.window, step), context)
    range = {context.start_ms - max(window, step) - context.lookback_ms, context.end_ms}

    with {:ok, fetched} <- context.fetch.(selector, range),
         {:ok, series} <- each_rollup(fetched, name, scalars, window, keep, context) do
      {:ok, series, stats(fetched)}
    end
  end

  defp subquery_rollup(name, scalars, rollup, keep, context) do
    step = with 0 <- resolve(rollup.step, context.step_ms), do: context.step_ms
    window = resolve(rollup.window, context.step_ms)
    start = context.start_ms - (window + step + @silence_ms)
    finish = context.end_ms + step

    with {:ok, _points} <- subquery_grid(start, finish, step),
         {start, finish} = align(start, finish, step),
         inner_context = with_grid(context, start, finish, step, @subquery_max_points),
         {:ok, inner, stats} <- eval(rollup.expr, inner_context),
         samples = Enum.map(inner, &present_samples/1),
         {:ok, series} <- each_rollup(samples, name, scalars, window, keep, context) do
      {:ok, series, stats}
    end
  end

  defp present_samples(%Series{labels: labels, values: points}) do
    present = Enum.reject(points, &(elem(&1, 1) == nil))

    %{
      labels: labels,
      timestamps: Enum.map(present, &elem(&1, 0)),
      values: Enum.map(present, &elem(&1, 1))
    }
  end

  defp subquery_grid(start, finish, step) do
    case Rollup.grid(start, finish, step, @subquery_max_points) do
      {:ok, grid} ->
        {:ok, grid}

      {:error, {:too_many_points, message}} ->
        {:error, {:invalid_grid, message <> " for a subquery"}}

      error ->
        error
    end
  end

  defp align(start, finish, step) do
    start = start - rem(start, step)
    adjust = rem(finish, step)
    {start, if(adjust > 0, do: finish + step - adjust, else: finish)}
  end

  defp each_rollup(samples, name, scalars, window, keep, context) do
    config = %{
      start_ms: context.start_ms,
      end_ms: context.end_ms,
      step_ms: context.step_ms,
      window_ms: window
    }

    Args.collect(samples, fn %{labels: labels} = one ->
      with {:ok, points} <-
             Rollup.apply(name, scalars, Map.take(one, [:timestamps, :values]), config),
           do: {:ok, %Series{labels: labels(labels, name, keep), values: points}}
    end)
  end

  defp window("default_rollup", 0, context), do: max(context.step_ms, context.lookback_ms)
  defp window(_name, window, _context), do: window

  defp non_negative(window, step) do
    if resolve(window, step) < 0,
      do: {:error, {:invalid_argument, "duration cannot be negative; got #{window.text}"}},
      else: :ok
  end

  defp resolve(nil, _step), do: 0
  defp resolve(%Duration{ms: ms, steps: steps}, step), do: Durations.resolve(ms, steps, step)

  defp labels(labels, name, keep) do
    if keep or Rollup.keeps_metric_name?(name),
      do: labels,
      else: Series.drop_name(labels)
  end

  defp absent_over_time(series, "absent_over_time", expr, context) do
    absent = Transform.absent(expr, [], context.timestamps)

    case series do
      [] ->
        [absent]

      _some ->
        columns = series |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)
        [Series.put_values(absent, Enum.map(columns, &absent_point/1))]
    end
  end

  defp absent_over_time(series, _name, _expr, _context), do: series

  defp absent_point(column), do: if(Enum.member?(column, nil), do: nil, else: 1.0)

  defp shift(series, 0), do: series

  defp shift(series, offset) do
    Enum.map(series, fn one ->
      %{one | values: Enum.map(one.values, fn {t, v} -> {t + offset, v} end)}
    end)
  end

  defp answer(series, may_sort) do
    series = Series.drop_empty(series)
    series = if may_sort, do: Series.sort(series), else: series

    duplicate =
      series
      |> Enum.frequencies_by(& &1.labels)
      |> Enum.find(fn {_labels, count} -> count > 1 end)

    case duplicate do
      nil ->
        {:ok, series}

      {labels, _count} ->
        {:error, {:duplicate_series, "duplicate output timeseries: " <> Series.describe(labels)}}
    end
  end

  defp stats(fetched) do
    %{
      series: length(fetched),
      samples: Enum.reduce(fetched, 0, fn %{timestamps: ts}, acc -> acc + length(ts) end)
    }
  end

  defp empty, do: %{series: 0, samples: 0}

  defp merge(a, b), do: %{series: a.series + b.series, samples: a.samples + b.samples}
end
