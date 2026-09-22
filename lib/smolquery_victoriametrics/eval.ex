defmodule SmolqueryVictoriaMetrics.Eval do
  @moduledoc """
  Evaluates a parsed MetricsQL expression over a step grid (PL-70, T-564),
  as VictoriaMetrics v1.152.0's `app/vmselect/promql/eval.go` does.

  The grid is `start, start + step, ...` up to `end`; an instant query is a
  grid of one point. What an expression evaluates to is a scalar, a number
  or `nil` for NaN, or a list of `SmolqueryVictoriaMetrics.Eval.Series`,
  each with a value, possibly `nil`, at every point of the grid.

  ## What evaluates, in this layer

    * a number or a duration, alone: a scalar (a duration is its seconds);
    * a selector `m{...}`: `default_rollup` over it;
    * a selector with a window, an `offset` or an `@`: `m[5m]`,
      `m offset 1h`, `m offset -5m`, `m @ 1700000000`, `m @ end()`;
    * a call of one of `SmolqueryVictoriaMetrics.Rollup.functions/0` whose
      series argument is one of those selectors, with its scalar arguments
      (`quantile_over_time(0.9, m[5m])`, `count_gt_over_time(m[5m], 10)`)
      and `keep_metric_names`;
    * parentheses around one of these.

  Anything else — an aggregate, a transform, a binary operator, a subquery,
  a union, a rollup over anything but a selector — answers
  `{:error, {:unsupported, what}}` naming it; the evaluator above the
  rollups is the next layer of PL-70, and it adds clauses here.

  ## A rollup over a selector

  As `evalRollupFuncWithMetricExpr` and `evalRollupFuncNoCache`:

    * the window is the one written, resolved at the step (`5m`, `2i`); a
      window not written is `0`, which `SmolqueryVictoriaMetrics.Rollup`
      reads as the step, widened for the functions that may widen it. For
      `default_rollup` alone, the bare selector's function, a window not
      written is `max(step, lookback_ms)`: a sample is current for
      `lookback_ms` after it was taken, as `SmolqueryVictoriaMetrics.Runtime`
      configures;
    * `offset d` evaluates on the grid moved back by `d` and answers at the
      grid's own points; `@ t` evaluates at `t` alone and answers that value
      at every point;
    * the samples are read once per selector, for
      `[start - offset - max(window, step) - lookback_ms, end - offset]`,
      which holds every window and the sample before the first one;
    * the rollup then runs once per series. A rollup drops `__name__`
      unless it is one that keeps it (`Rollup.keeps_metric_name?/1`) or the
      call says `keep_metric_names`; `absent_over_time` answers one series,
      labelled by the selector's `=` matchers, `1` where no series had a
      sample.

  ## The answer

  As `promql.Exec`: series with no value at any point are dropped, two
  series left with the same labels are an error
  (`duplicate output timeseries`), and the rest are sorted by name, then
  labels.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.MetricsQL.Functions
  alias SmolqueryVictoriaMetrics.Rollup
  alias SmolqueryVictoriaMetrics.Samples

  @inf 1.797_693_134_862_315_7e308

  defmodule Series do
    @moduledoc """
    One series of an evaluated expression: its labels, `__name__` among them
    when it is kept, and a value, or `nil` for none, at each point of the
    grid, as `{timestamp_ms, value}`.
    """
    @enforce_keys [:labels, :values]
    defstruct [:labels, :values]

    @type t :: %__MODULE__{
            labels: %{String.t() => String.t()},
            values: [{integer(), float() | nil}]
          }
  end

  @typedoc "What an expression evaluates to: a scalar, or series."
  @type result :: float() | nil | [Series.t()]

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
          start_ms: integer(),
          end_ms: integer(),
          step_ms: pos_integer(),
          lookback_ms: pos_integer(),
          max_points: pos_integer(),
          fetch: fetch()
        }

  @typedoc "What evaluating read: the series and the raw samples fetched."
  @type stats :: %{series: non_neg_integer(), samples: non_neg_integer()}

  @type reason ::
          {:unsupported, String.t()}
          | {:too_many_points, String.t()}
          | {:duplicate_series, String.t()}
          | {:invalid_at, String.t()}
          | term()

  @doc """
  Evaluates `expr` over the grid of `context` and prepares the answer: no
  empty series, no duplicates, sorted.
  """
  @spec run(Ast.expr(), context()) :: {:ok, result(), stats()} | {:error, reason()}
  def run(expr, context) do
    with {:ok, _grid} <- grid(context),
         {:ok, result, stats} <- eval(expr, context),
         {:ok, result} <- answer(result) do
      {:ok, result, stats}
    end
  end

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

    with {:ok, fetched} <- context.fetch.(selector, {from, to}) do
      series =
        for %{labels: labels, timestamps: timestamps, values: values} <- fetched,
            points = within(timestamps, values, from, to),
            points != [],
            do: %Series{labels: labels, values: points}

      {:ok, sorted(series), stats(fetched)}
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

  defp grid(context),
    do: Rollup.grid(context.start_ms, context.end_ms, context.step_ms, context.max_points)

  defp eval(%Number{value: value}, _context), do: {:ok, number(value), empty()}

  defp eval(%Duration{ms: ms, steps: steps}, context),
    do: {:ok, Durations.resolve(ms, steps, context.step_ms) / 1000, empty()}

  defp eval(%ParensExpr{exprs: [expr]}, context), do: eval(expr, context)

  defp eval(%ParensExpr{}, _context),
    do: unsupported("a union of several expressions, `(a, b)`")

  defp eval(%MetricExpr{} = selector, context),
    do: rollup("default_rollup", [], selector, %RollupExpr{expr: selector}, false, context)

  defp eval(%RollupExpr{expr: %MetricExpr{} = selector, step: nil, inherit_step: false} = r, ctx),
    do: rollup("default_rollup", [], selector, r, false, ctx)

  defp eval(%RollupExpr{}, _context), do: unsupported("subqueries, `q[window:step]`")

  defp eval(%FuncExpr{name: name} = call, context) do
    case Functions.kind(name) do
      :rollup -> call(call, context)
      _transform -> unsupported("transform function #{String.downcase(name)}()")
    end
  end

  defp eval(%AggrFuncExpr{name: name}, _context),
    do: unsupported("aggregate function #{name}()")

  defp eval(%BinaryOpExpr{op: op}, _context), do: unsupported("binary operator `#{op}`")

  defp eval(%StringLiteral{}, _context), do: unsupported("a string literal as a result")

  defp call(%FuncExpr{name: name, args: args, keep_metric_names: keep}, context) do
    index = Rollup.series_arg_index(name)

    with :ok <- supported(name),
         {:ok, selector, rollup} <- series_arg(name, Enum.at(args, index)),
         {:ok, scalars} <- scalars(name, List.delete_at(args, index), context) do
      rollup(String.downcase(name), scalars, selector, rollup, keep, context)
    end
  end

  defp supported(name) do
    if Rollup.supported?(name),
      do: :ok,
      else: unsupported("rollup function #{String.downcase(name)}()")
  end

  defp series_arg(_name, %MetricExpr{} = selector),
    do: {:ok, selector, %RollupExpr{expr: selector}}

  defp series_arg(
         _name,
         %RollupExpr{expr: %MetricExpr{} = selector, step: nil, inherit_step: false} = rollup
       ),
       do: {:ok, selector, rollup}

  defp series_arg(name, nil), do: {:error, {:arity, "#{name}() is missing its series argument"}}

  defp series_arg(name, _expr),
    do: unsupported("#{String.downcase(name)}() over anything but a series selector (a subquery)")

  defp scalars(name, exprs, context) do
    exprs
    |> Enum.reduce_while({:ok, []}, fn expr, {:ok, acc} ->
      case eval(expr, context) do
        {:ok, value, _stats} when is_float(value) or is_nil(value) ->
          {:cont, {:ok, [value | acc]}}

        {:ok, _series, _stats} ->
          {:halt, unsupported("a series as a parameter of #{String.downcase(name)}()")}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> in_order()
  end

  defp in_order({:ok, reversed}), do: {:ok, Enum.reverse(reversed)}
  defp in_order(error), do: error

  defp rollup(name, scalars, selector, %RollupExpr{} = rollup, keep, context) do
    step = context.step_ms
    window = window(name, resolve(rollup.window, step), context)
    offset = resolve(rollup.offset, step)

    with {:ok, {start, finish}} <- at(rollup.at, context),
         {:ok, fetched} <-
           context.fetch.(
             selector,
             {start - offset - max(window, step) - context.lookback_ms, finish - offset}
           ),
         {:ok, series} <-
           each_series(fetched, fn samples ->
             Rollup.apply(name, scalars, samples, %{
               start_ms: start - offset,
               end_ms: finish - offset,
               step_ms: step,
               window_ms: window
             })
           end) do
      series =
        series
        |> Enum.map(fn {labels, points} ->
          %Series{
            labels: labels(labels, name, keep),
            values: points |> shift(offset) |> spread(rollup.at, context)
          }
        end)
        |> absent(name, selector, context)

      {:ok, series, stats(fetched)}
    end
  end

  defp window("default_rollup", 0, context), do: max(context.step_ms, context.lookback_ms)
  defp window(_name, window, _context), do: window

  defp resolve(nil, _step), do: 0
  defp resolve(%Duration{ms: ms, steps: steps}, step), do: Durations.resolve(ms, steps, step)

  defp at(nil, context), do: {:ok, {context.start_ms, context.end_ms}}

  defp at(%FuncExpr{name: name, args: []}, context) do
    case String.downcase(name) do
      "start" -> {:ok, {context.start_ms, context.start_ms}}
      "end" -> {:ok, {context.end_ms, context.end_ms}}
      _other -> at_value(%FuncExpr{name: name, args: []}, context)
    end
  end

  defp at(expr, context), do: at_value(expr, context)

  defp at_value(expr, context) do
    case eval(expr, context) do
      {:ok, seconds, _stats} when is_float(seconds) ->
        time = trunc(seconds * 1000)
        {:ok, {time, time}}

      {:ok, nil, _stats} ->
        {:error, {:invalid_at, "`@` modifier must return a non-NaN value"}}

      {:ok, _series, _stats} ->
        unsupported("`@` with a series")

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp each_series(fetched, fun) do
    Enum.reduce_while(fetched, {:ok, []}, fn %{labels: labels} = samples, {:ok, acc} ->
      case fun.(samples) do
        {:ok, points} -> {:cont, {:ok, [{labels, points} | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, series} -> {:ok, Enum.reverse(series)}
      error -> error
    end)
  end

  defp labels(labels, name, keep) do
    if keep or Rollup.keeps_metric_name?(name),
      do: labels,
      else: Map.delete(labels, "__name__")
  end

  defp shift(points, 0), do: points
  defp shift(points, offset), do: Enum.map(points, fn {t, v} -> {t + offset, v} end)

  defp spread(points, nil, _context), do: points

  defp spread([{_t, value}], _at, context) do
    {:ok, grid} = grid(%{context | max_points: nil})
    Enum.map(grid, &{&1, value})
  end

  defp absent(series, "absent_over_time", selector, context) do
    {:ok, grid} = grid(%{context | max_points: nil})

    values =
      series
      |> Enum.map(& &1.values)
      |> Enum.zip_with(& &1)
      |> case do
        [] -> Enum.map(grid, &{&1, 1.0})
        columns -> Enum.zip_with(grid, columns, &absent_point/2)
      end

    [%Series{labels: absent_labels(selector), values: values}]
  end

  defp absent(series, _name, _selector, _context), do: series

  defp absent_point(t, column) do
    if Enum.any?(column, fn {_t, v} -> v == nil end), do: {t, nil}, else: {t, 1.0}
  end

  defp absent_labels(%MetricExpr{filter_sets: [filters]}) do
    for %LabelFilter{name: name, op: :eq, value: value} <- filters,
        name != "__name__",
        into: %{},
        do: {name, value}
  end

  defp absent_labels(_selector), do: %{}

  defp answer(scalar) when not is_list(scalar), do: {:ok, scalar}

  defp answer(series) do
    series =
      Enum.reject(series, fn %Series{values: values} ->
        Enum.all?(values, &(elem(&1, 1) == nil))
      end)

    duplicate =
      series
      |> Enum.frequencies_by(& &1.labels)
      |> Enum.find(fn {_labels, count} -> count > 1 end)

    case duplicate do
      nil ->
        {:ok, sorted(series)}

      {labels, _count} ->
        {:error, {:duplicate_series, "duplicate output timeseries: " <> describe(labels)}}
    end
  end

  defp sorted(series) do
    Enum.sort_by(series, fn %Series{labels: labels} ->
      {Map.get(labels, "__name__", ""), labels |> Map.delete("__name__") |> Enum.sort()}
    end)
  end

  @doc """
  A series' labels as PromQL writes a selector: `name{k="v", ...}`.
  """
  @spec describe(%{String.t() => String.t()}) :: String.t()
  def describe(labels) do
    {name, rest} = Map.pop(labels, "__name__", "")
    pairs = rest |> Enum.sort() |> Enum.map_join(", ", fn {k, v} -> "#{k}=#{inspect(v)}" end)
    "#{name}{#{pairs}}"
  end

  defp stats(fetched) do
    %{
      series: length(fetched),
      samples: Enum.reduce(fetched, 0, fn %{timestamps: ts}, acc -> acc + length(ts) end)
    }
  end

  defp empty, do: %{series: 0, samples: 0}

  defp number(:inf), do: @inf
  defp number(:neg_inf), do: -@inf
  defp number(:nan), do: nil
  defp number(value), do: value * 1.0

  defp unsupported(what), do: {:error, {:unsupported, what}}
end
