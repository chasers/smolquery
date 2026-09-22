defmodule SmolqueryVictoriaMetrics.Rollup do
  @moduledoc """
  MetricsQL's rollup functions over one series' raw samples, a port of
  VictoriaMetrics v1.152.0's `app/vmselect/promql/rollup.go` (PL-70, T-564).

  A rollup turns a series' samples into one value per point of a step grid,
  `start, start + step, ...` up to `end`. At each point `t` it looks at the
  samples in the window `(t - window, t]` and at the sample just before it,
  as `rollupConfig.doInternal` does:

    * the window is the one given; a window of `0` (none was written:
      `rate(m)`, `m[$__interval]`) is the step, widened for the functions
      VictoriaMetrics lets widen it (`rate`, `irate`, `deriv`, `deriv_fast`,
      `ideriv`, `default_rollup`, `rate_over_sum`, `scrape_interval`,
      `timestamp`)
      to the largest gap it expects between two samples, so a zoomed-in
      graph still finds two samples;
    * that gap, `maxPrevInterval`, is the step for an instant query, and for
      a range query the scrape interval (the 0.6 quantile of the series'
      last 20 intervals, `getScrapeInterval`) inflated by
      `getMaxPrevInterval`'s table;
    * `prev_value` is the sample before the window when it is within that
      gap of the window's start and a sample follows it; `real_prev_value`
      is the sample before the window regardless, unless `lookback_delta_ms`
      is set and the gap to the window's first sample is not shorter;
      `real_next_value` is the sample after the window.

  `lookback_delta_ms` is VictoriaMetrics' `LookbackDelta`
  (`-search.maxLookback`), `0` by default as it is there: no staleness
  horizon. It is not the edge's `lookback_ms`, which only sets
  `default_rollup`'s window when none is written
  (`SmolqueryVictoriaMetrics.Eval`).

  `rate`, `increase`, `increase_pure` and `irate` first remove counter
  resets from the whole series (`removeCounterResets`), as VictoriaMetrics
  does, so a reset inside the window or just before it adds the value the
  counter fell from. `rate` does not extrapolate: it is the difference
  between the window's last sample and the sample before the window (or the
  window's first) over the time between them.

  ## Values

  Where VictoriaMetrics answers NaN, a value here is `nil`. Samples never
  hold NaN (the write path drops it), so no function needs to skip one.
  `±Inf` is stored as `±1.7976931348623157e308` (PL-70 D6), and a result is
  held to that bound too: a division by zero, a quantile past `1` and a sum
  past the largest double answer it, which the response writes as `+Inf` or
  `-Inf`. Any other intermediate that overflows a double answers `nil`.

  ## Cost

  One sweep per series: the window's two edges only move forward over the
  samples, so each sample is passed over once however many points the grid
  has, and a function then reads only its window. The samples are held as
  tuples, so reading a window's edge is constant time. Measured on one
  series of 1,000,000 samples at one a second, over a 999-point grid:

  | function | window | time | samples a second |
  |---|---|---|---|
  | `default_rollup` | 5m | 20 ms | 49,000,000 |
  | `quantile_over_time(0.9, ...)` | the step | 48 ms | 20,000,000 |
  | `sum_over_time` | the step | 59 ms | 17,000,000 |
  | `rate` | the step | 135 ms | 7,400,000 |

  `rate` pays for the counter-reset pass over the whole series; a
  function that reads every sample in its window pays for what its
  windows hold.

  Pure: no process, no application environment.
  """

  alias __MODULE__.Window

  @inf 1.797_693_134_862_315_7e308

  @functions ~w(
    absent_over_time avg_over_time changes count_eq_over_time count_gt_over_time
    count_le_over_time count_ne_over_time count_over_time default_rollup delta deriv
    deriv_fast distinct_over_time first_over_time geomean_over_time idelta ideriv increase
    increase_pure
    irate lag last_over_time lifetime max_over_time min_over_time present_over_time
    quantile_over_time range_over_time rate rate_over_sum resets scrape_interval
    stddev_over_time stdvar_over_time sum2_over_time sum_over_time timestamp
    tmax_over_time tmin_over_time
  )

  @keep_metric_name ~w(
    avg_over_time default_rollup first_over_time geomean_over_time last_over_time
    max_over_time min_over_time quantile_over_time
  )

  @can_adjust_window ~w(
    default_rollup deriv deriv_fast ideriv irate rate rate_over_sum scrape_interval timestamp
  )

  @remove_counter_resets ~w(increase increase_pure irate rate)

  @filters %{
    "count_eq_over_time" => :eq,
    "count_ne_over_time" => :ne,
    "count_gt_over_time" => :gt,
    "count_le_over_time" => :le
  }

  @typedoc "A value: a float, or `nil` where VictoriaMetrics has NaN."
  @type value :: float() | nil

  @typedoc "One series' samples, ascending by timestamp in milliseconds."
  @type samples :: %{timestamps: [integer()], values: [float()]}

  @typedoc """
  The step grid and the window: `window_ms` of `0` is "none written";
  `lookback_delta_ms` defaults to `0`, `may_adjust_window` to what the
  function allows (`may_adjust_window?/1`), `max_points` to no limit.
  """
  @type config :: %{
          required(:start_ms) => integer(),
          required(:end_ms) => integer(),
          required(:step_ms) => pos_integer(),
          optional(:window_ms) => non_neg_integer(),
          optional(:lookback_delta_ms) => non_neg_integer(),
          optional(:may_adjust_window) => boolean(),
          optional(:max_points) => pos_integer()
        }

  @type reason ::
          {:unsupported, String.t()}
          | {:arity, String.t()}
          | {:too_many_points, String.t()}
          | {:invalid_grid, String.t()}

  @doc """
  The rollup functions this module computes, lower-case.
  """
  @spec functions() :: [String.t()]
  def functions, do: @functions

  @doc """
  Whether `name` is one of `functions/0`, case-insensitively.
  """
  @spec supported?(String.t()) :: boolean()
  def supported?(name), do: String.downcase(name) in @functions

  @doc """
  Whether a rollup keeps the series' `__name__` without `keep_metric_names`:
  the functions that do not change what the series measures
  (`rollupFuncsKeepMetricName`).
  """
  @spec keeps_metric_name?(String.t()) :: boolean()
  def keeps_metric_name?(name), do: String.downcase(name) in @keep_metric_name

  @doc """
  Whether a window left out may widen past the step
  (`rollupFuncsCanAdjustWindow`).
  """
  @spec may_adjust_window?(String.t()) :: boolean()
  def may_adjust_window?(name), do: String.downcase(name) in @can_adjust_window

  @doc """
  Which argument of a call is the series; the others are scalars.
  `quantile_over_time(phi, m)` takes it second, every other function first.
  """
  @spec series_arg_index(String.t()) :: 0 | 1
  def series_arg_index(name) do
    if String.downcase(name) == "quantile_over_time", do: 1, else: 0
  end

  @doc """
  The step grid `start, start + step, ...` up to and including `end`, and
  refused past `max_points`, as `ValidateMaxPointsPerSeries` refuses it.
  """
  @spec grid(integer(), integer(), integer(), pos_integer() | nil) ::
          {:ok, [integer()]} | {:error, reason()}
  def grid(_start, _end, step, _max) when step <= 0,
    do: {:error, {:invalid_grid, "step must be positive; got #{step}ms"}}

  def grid(start, finish, _step, _max) when start > finish,
    do: {:error, {:invalid_grid, "start (#{start}) cannot exceed end (#{finish})"}}

  def grid(start, finish, step, max) do
    points = div(finish - start, step) + 1

    if is_integer(max) and points > max,
      do:
        {:error,
         {:too_many_points,
          "too many points for the given start=#{start}, end=#{finish} and step=#{step}: " <>
            "#{points}; the maximum number of points is #{max}"}},
      else: {:ok, Enum.to_list(start..(start + (points - 1) * step)//step)}
  end

  @doc """
  Computes the rollup `name` over `samples` at every point of the grid in
  `config`, with `args` the call's scalar arguments in order (`[0.9]` for
  `quantile_over_time(0.9, m)`, `[10.0]` for `count_gt_over_time(m, 10)`).
  """
  @spec apply(String.t(), [value()], samples(), config()) ::
          {:ok, [{integer(), value()}]} | {:error, reason()}
  def apply(name, args, %{timestamps: timestamps, values: values}, config) do
    name = String.downcase(name)

    with {:ok, fun} <- function(name, args),
         {:ok, grid} <-
           grid(config.start_ms, config.end_ms, config.step_ms, Map.get(config, :max_points)) do
      config =
        config
        |> Map.put_new(:window_ms, 0)
        |> Map.put_new(:lookback_delta_ms, 0)
        |> Map.put_new(:may_adjust_window, name in @can_adjust_window)
        |> Map.put(:default_rollup, name == "default_rollup")

      values = prepare(name, values, timestamps, config)
      {:ok, sweep(fun, grid, List.to_tuple(timestamps), List.to_tuple(values), config)}
    end
  end

  @doc """
  Computes the rollup `name` over one window, as a test of one function in
  `rollup_test.go` does over a `rollupFuncArg`.
  """
  @spec evaluate(String.t(), [value()], Window.t()) :: {:ok, value()} | {:error, reason()}
  def evaluate(name, args, %Window{} = window) do
    with {:ok, fun} <- function(String.downcase(name), args), do: {:ok, call(fun, window)}
  end

  @doc """
  Adds back what each counter reset took away, over a whole series
  (`removeCounterResets`). A fall of less than an eighth of the value is a
  partial reset and adds only the fall; any other adds the value fallen
  from. A gap longer than `staleness_ms` (when positive) starts the series
  over. The result never decreases.
  """
  @spec remove_counter_resets([float()], [integer()], non_neg_integer()) :: [float()]
  def remove_counter_resets([], _timestamps, _staleness_ms), do: []

  def remove_counter_resets(values, timestamps, staleness_ms) do
    resets(values, timestamps, staleness_ms, &Kernel.+/2)
  rescue
    ArithmeticError -> resets(values, timestamps, staleness_ms, &add/2)
  end

  defp resets([first | values], [first_ts | timestamps], staleness, plus),
    do: [first | resets(values, timestamps, staleness, plus, {0.0, first, first_ts, first})]

  defp resets([], [], _staleness, _plus, _state), do: []

  defp resets([value | values], [timestamp | timestamps], staleness, plus, state) do
    {correction, previous, previous_ts, previous_out} = state
    correction = correction(value, previous, correction, plus)

    if staleness > 0 and timestamp - previous_ts > staleness do
      [value | resets(values, timestamps, staleness, plus, {0.0, value, timestamp, value})]
    else
      out = plus.(value, correction)
      out = if out < previous_out, do: previous_out, else: out
      [out | resets(values, timestamps, staleness, plus, {correction, value, timestamp, out})]
    end
  end

  defp correction(value, previous, correction, plus) when value < previous do
    fall = plus.(previous, -value)

    if fall < previous / 8,
      do: plus.(correction, fall),
      else: plus.(correction, previous)
  end

  defp correction(_value, _previous, correction, _plus), do: correction

  defp prepare(name, values, timestamps, config) when name in @remove_counter_resets do
    staleness =
      if config.lookback_delta_ms > 0,
        do: config.lookback_delta_ms + config.window_ms,
        else: 0

    remove_counter_resets(values, timestamps, staleness)
  end

  defp prepare(_name, values, _timestamps, _config), do: values

  defp sweep(fun, grid, timestamps, values, config) do
    count = tuple_size(timestamps)
    max_prev = max_prev_interval(timestamps, count, config)
    window = window(config, max_prev)
    limits = %{max_prev: max_prev, lookback_delta: config.lookback_delta_ms}
    series = {timestamps, values, count}

    {points, _edges} =
      Enum.map_reduce(grid, {0, 0}, fn t_end, {first, last} ->
        t_start = t_end - window
        first = seek(timestamps, first, count, t_start)
        last = seek(timestamps, max(first, last), count, t_end)
        arg = window_at(series, {first, last}, {t_start, t_end}, window, limits)
        {{t_end, call(fun, arg)}, {first, last}}
      end)

    points
  end

  defp window_at({timestamps, values, count}, {first, last}, {t_start, t_end}, window, limits) do
    {prev_value, prev_timestamp} =
      prev(timestamps, values, count, first, t_start - limits.max_prev)

    %Window{
      values: values,
      timestamps: timestamps,
      first: first,
      last: last,
      prev_value: prev_value,
      prev_timestamp: prev_timestamp,
      real_prev_value: real_prev(timestamps, values, {first, last}, t_start, limits),
      real_next_value: if(last < count, do: elem(values, last)),
      curr_timestamp: t_end,
      window: window
    }
  end

  defp real_prev(_timestamps, _values, {0, _last}, _t_start, _limits), do: nil

  defp real_prev(timestamps, values, {first, last}, t_start, %{lookback_delta: lookback}) do
    current = if last > first, do: elem(timestamps, first), else: t_start

    if lookback == 0 or current - elem(timestamps, first - 1) < lookback,
      do: elem(values, first - 1)
  end

  defp prev(timestamps, values, count, first, horizon)
       when first < count and first > 0 do
    if elem(timestamps, first - 1) > horizon,
      do: {elem(values, first - 1), elem(timestamps, first - 1)},
      else: {nil, horizon}
  end

  defp prev(_timestamps, _values, _count, _first, horizon), do: {nil, horizon}

  defp seek(timestamps, index, count, bound)
       when index < count and elem(timestamps, index) <= bound,
       do: seek(timestamps, index + 1, count, bound)

  defp seek(_timestamps, index, _count, _bound), do: index

  defp max_prev_interval(timestamps, count, config) do
    max_prev =
      if config.start_ms < config.end_ms,
        do: timestamps |> scrape_interval(count, config.step_ms) |> inflate(),
        else: config.step_ms

    lookback = config.lookback_delta_ms
    if lookback > 0 and max_prev > lookback, do: lookback, else: max_prev
  end

  defp window(%{window_ms: window}, _max_prev) when window > 0, do: window

  defp window(config, max_prev) do
    window = config.step_ms
    window = if config.may_adjust_window and window < max_prev, do: max_prev, else: window
    lookback = config.lookback_delta_ms

    if config.default_rollup and lookback > 0 and window > lookback,
      do: lookback,
      else: window
  end

  @doc """
  The scrape interval of a series: the 0.6 quantile of the intervals between
  its last 21 samples, or `default` with fewer than two samples or a
  non-positive estimate (`getScrapeInterval`).
  """
  @spec scrape_interval(tuple(), non_neg_integer(), integer()) :: integer()
  def scrape_interval(_timestamps, count, default) when count < 2, do: default

  def scrape_interval(timestamps, count, default) do
    lowest = max(count - 21, 0)

    intervals =
      for index <- (count - 1)..(lowest + 1)//-1,
          do: :erlang.float(elem(timestamps, index) - elem(timestamps, index - 1))

    case quantile(0.6, intervals) do
      estimate when is_float(estimate) and trunc(estimate) > 0 -> trunc(estimate)
      _none -> default
    end
  end

  @doc """
  The largest gap a series with this scrape interval is expected to leave
  between two samples, jitter included (`getMaxPrevInterval`).
  """
  @spec inflate(integer()) :: integer()
  def inflate(interval) when interval <= 2_000, do: interval + 4 * interval
  def inflate(interval) when interval <= 4_000, do: interval + 2 * interval
  def inflate(interval) when interval <= 8_000, do: interval + interval
  def inflate(interval) when interval <= 16_000, do: interval + div(interval, 2)
  def inflate(interval) when interval <= 32_000, do: interval + div(interval, 4)
  def inflate(interval), do: interval + div(interval, 8)

  defp call(fun, %Window{} = window) do
    fun.(window)
  rescue
    ArithmeticError -> nil
  end

  defp function(name, args) when is_map_key(@filters, name) do
    case args do
      [limit] -> {:ok, &count_filter(&1, Map.fetch!(@filters, name), limit)}
      _other -> arity(name, 2, args)
    end
  end

  defp function("quantile_over_time", args) do
    case args do
      [phi] -> {:ok, &quantile(phi, Window.values(&1))}
      _other -> arity("quantile_over_time", 2, args)
    end
  end

  defp function(name, []) when name in @functions, do: {:ok, one_arg(name)}
  defp function(name, args) when name in @functions, do: arity(name, 1, args)

  defp function(name, _args), do: {:error, {:unsupported, "rollup function #{name}()"}}

  defp arity(name, expected, args),
    do: {:error, {:arity, "#{name}() takes #{expected} args; got #{length(args) + 1}"}}

  defp one_arg("default_rollup"), do: &last/1
  defp one_arg("last_over_time"), do: &last/1
  defp one_arg("first_over_time"), do: &first/1
  defp one_arg("rate"), do: &derive_fast/1
  defp one_arg("deriv_fast"), do: &derive_fast/1
  defp one_arg("increase"), do: &delta/1
  defp one_arg("delta"), do: &delta/1
  defp one_arg("increase_pure"), do: &increase_pure/1
  defp one_arg("irate"), do: &ideriv/1
  defp one_arg("ideriv"), do: &ideriv/1
  defp one_arg("idelta"), do: &idelta/1
  defp one_arg("deriv"), do: &deriv/1
  defp one_arg("changes"), do: &changes/1
  defp one_arg("resets"), do: &resets/1
  defp one_arg("avg_over_time"), do: &avg/1
  defp one_arg("min_over_time"), do: &minimum/1
  defp one_arg("max_over_time"), do: &maximum/1
  defp one_arg("sum_over_time"), do: &sum_over_time/1
  defp one_arg("count_over_time"), do: &count_over_time/1
  defp one_arg("stddev_over_time"), do: &stddev/1
  defp one_arg("stdvar_over_time"), do: &stdvar/1
  defp one_arg("present_over_time"), do: &present/1
  defp one_arg("absent_over_time"), do: &absent/1
  defp one_arg("lag"), do: &lag/1
  defp one_arg("lifetime"), do: &lifetime/1
  defp one_arg("scrape_interval"), do: &average_interval/1
  defp one_arg("rate_over_sum"), do: &rate_over_sum/1
  defp one_arg("sum2_over_time"), do: &sum2/1
  defp one_arg("geomean_over_time"), do: &geomean/1
  defp one_arg("distinct_over_time"), do: &distinct/1
  defp one_arg("range_over_time"), do: &range/1
  defp one_arg("timestamp"), do: &tlast/1
  defp one_arg("tmin_over_time"), do: &tmin/1
  defp one_arg("tmax_over_time"), do: &tmax/1

  defp last(%Window{first: same, last: same}), do: nil
  defp last(window), do: elem(window.values, window.last - 1)

  defp first(%Window{first: same, last: same}), do: nil
  defp first(window), do: elem(window.values, window.first)

  defp count_over_time(%Window{first: same, last: same}), do: nil
  defp count_over_time(window), do: :erlang.float(window.last - window.first)

  defp present(%Window{first: same, last: same}), do: nil
  defp present(_window), do: 1.0

  defp absent(%Window{first: same, last: same}), do: 1.0
  defp absent(_window), do: nil

  defp avg(%Window{first: same, last: same}), do: nil
  defp avg(window), do: sum(Window.values(window)) / (window.last - window.first)

  defp minimum(%Window{first: same, last: same}), do: nil
  defp minimum(window), do: Enum.min(Window.values(window))

  defp maximum(%Window{first: same, last: same}), do: nil
  defp maximum(window), do: Enum.max(Window.values(window))

  defp range(%Window{first: same, last: same}), do: nil
  defp range(window), do: sub(maximum(window), minimum(window))

  defp sum_over_time(%Window{first: same, last: same}), do: nil
  defp sum_over_time(window), do: sum(Window.values(window))

  defp sum2(%Window{first: same, last: same}), do: nil
  defp sum2(window), do: window |> Window.values() |> Enum.map(&mul(&1, &1)) |> sum()

  defp rate_over_sum(%Window{first: same, last: same}), do: nil

  defp rate_over_sum(window),
    do: divide(sum(Window.values(window)), window.window / 1000)

  defp geomean(%Window{first: same, last: same}), do: nil

  defp geomean(window) do
    values = Window.values(window)
    product = Enum.reduce(values, 1.0, &mul/2)
    power(product, 1 / length(values))
  end

  defp power(base, exponent) when base < 0 and exponent != trunc(exponent), do: nil
  defp power(base, exponent), do: :math.pow(base, exponent)

  defp distinct(%Window{first: same, last: same}), do: nil

  defp distinct(window),
    do:
      window
      |> Window.values()
      |> MapSet.new(&unsigned_zero/1)
      |> MapSet.size()
      |> :erlang.float()

  defp unsigned_zero(value), do: if(value == 0, do: 0.0, else: value)

  defp tlast(%Window{first: same, last: same}), do: nil
  defp tlast(window), do: elem(window.timestamps, window.last - 1) / 1000

  defp tmin(window), do: extreme_timestamp(window, &<=/2)
  defp tmax(window), do: extreme_timestamp(window, &>=/2)

  defp extreme_timestamp(%Window{first: same, last: same}, _keep?), do: nil

  defp extreme_timestamp(window, keep?) do
    first = {elem(window.values, window.first), elem(window.timestamps, window.first)}

    {_value, timestamp} =
      Enum.reduce((window.first + 1)..(window.last - 1)//1, first, fn index, best ->
        pick(keep?, {elem(window.values, index), elem(window.timestamps, index)}, best)
      end)

    timestamp / 1000
  end

  defp pick(keep?, {value, _timestamp} = candidate, {best, _best_timestamp} = current),
    do: if(keep?.(value, best), do: candidate, else: current)

  defp count_filter(%Window{first: same, last: same}, _op, _limit), do: nil

  defp count_filter(window, op, nil),
    do: if(op == :ne, do: :erlang.float(window.last - window.first), else: 0.0)

  defp count_filter(window, op, limit) do
    window
    |> Window.values()
    |> Enum.count(&compare(op, &1, limit))
    |> :erlang.float()
  end

  defp compare(:eq, value, limit), do: value == limit
  defp compare(:ne, value, limit), do: value != limit
  defp compare(:gt, value, limit), do: value > limit
  defp compare(:le, value, limit), do: value <= limit

  defp stddev(window) do
    case stdvar(window) do
      variance when is_float(variance) and variance >= 0 -> :math.sqrt(variance)
      _nan -> nil
    end
  end

  defp stdvar(%Window{first: same, last: same}), do: nil
  defp stdvar(%Window{first: first, last: last}) when last - first == 1, do: 0.0

  defp stdvar(window) do
    {_avg, count, q} =
      window
      |> Window.values()
      |> Enum.reduce({0.0, 0, 0.0}, fn value, {avg, count, q} ->
        count = count + 1
        next = avg + (value - avg) / count
        {next, count, q + (value - avg) * (value - next)}
      end)

    q / count
  end

  defp lag(%Window{first: same, last: same, prev_value: nil}), do: nil

  defp lag(%Window{first: same, last: same} = window),
    do: (window.curr_timestamp - window.prev_timestamp) / 1000

  defp lag(window), do: (window.curr_timestamp - elem(window.timestamps, window.last - 1)) / 1000

  defp lifetime(%Window{prev_value: nil} = window) do
    if window.last - window.first < 2,
      do: nil,
      else: (last_timestamp(window) - elem(window.timestamps, window.first)) / 1000
  end

  defp lifetime(%Window{first: same, last: same}), do: nil
  defp lifetime(window), do: (last_timestamp(window) - window.prev_timestamp) / 1000

  defp average_interval(%Window{prev_value: nil} = window) do
    count = window.last - window.first

    if count < 2,
      do: nil,
      else: (last_timestamp(window) - elem(window.timestamps, window.first)) / 1000 / (count - 1)
  end

  defp average_interval(%Window{first: same, last: same}), do: nil

  defp average_interval(window),
    do: (last_timestamp(window) - window.prev_timestamp) / 1000 / (window.last - window.first)

  defp last_timestamp(window), do: elem(window.timestamps, window.last - 1)

  defp derive_fast(%Window{prev_value: nil} = window) do
    if window.last - window.first < 2 do
      nil
    else
      rate_between(
        window,
        elem(window.values, window.first),
        elem(window.timestamps, window.first)
      )
    end
  end

  defp derive_fast(%Window{first: same, last: same}), do: 0.0
  defp derive_fast(window), do: rate_between(window, window.prev_value, window.prev_timestamp)

  defp rate_between(window, value, timestamp) do
    divide(sub(last(window), value), (last_timestamp(window) - timestamp) / 1000)
  end

  defp ideriv(%Window{first: same, last: same}), do: nil

  defp ideriv(%Window{first: first, last: last} = window) when last - first == 1 do
    case window.prev_value do
      nil ->
        nil

      prev ->
        divide(
          sub(elem(window.values, first), prev),
          (elem(window.timestamps, first) - window.prev_timestamp) / 1000
        )
    end
  end

  defp ideriv(window) do
    t_end = last_timestamp(window)
    v_end = last(window)

    case earlier(window, window.last - 2, t_end) do
      {v_start, t_start} ->
        divide(sub(v_end, v_start), (t_end - t_start) / 1000)

      nil when window.prev_value == nil ->
        0.0

      nil ->
        divide(sub(v_end, window.prev_value), (t_end - window.prev_timestamp) / 1000)
    end
  end

  defp earlier(window, index, t_end) when index >= window.first do
    timestamp = elem(window.timestamps, index)

    if timestamp >= t_end,
      do: earlier(window, index - 1, t_end),
      else: {elem(window.values, index), timestamp}
  end

  defp earlier(_window, _index, _t_end), do: nil

  defp idelta(%Window{first: same, last: same, prev_value: nil}), do: nil
  defp idelta(%Window{first: same, last: same}), do: 0.0

  defp idelta(%Window{first: first, last: last} = window) when last - first == 1 do
    case window.prev_value do
      nil -> last(window)
      prev -> sub(last(window), prev)
    end
  end

  defp idelta(window), do: sub(last(window), elem(window.values, window.last - 2))

  defp delta(%Window{prev_value: nil, first: same, last: same}), do: nil

  defp delta(%Window{prev_value: nil, real_prev_value: real} = window) when real != nil,
    do: sub(last(window), real)

  defp delta(%Window{prev_value: nil} = window) do
    first = elem(window.values, window.first)

    change =
      cond do
        window.last - window.first > 1 -> elem(window.values, window.first + 1) - first
        window.real_next_value != nil -> window.real_next_value - first
        true -> 0.0
      end

    cond do
      abs(first) < 10 * (abs(change) + 1) -> last(window)
      window.last - window.first == 1 -> 0.0
      true -> sub(last(window), first)
    end
  end

  defp delta(%Window{first: same, last: same}), do: 0.0
  defp delta(window), do: sub(last(window), window.prev_value)

  defp increase_pure(%Window{prev_value: nil, first: same, last: same}), do: nil

  defp increase_pure(%Window{prev_value: nil} = window),
    do: sub(last(window), window.real_prev_value || 0.0)

  defp increase_pure(%Window{first: same, last: same}), do: 0.0
  defp increase_pure(window), do: sub(last(window), window.prev_value)

  defp changes(%Window{prev_value: nil, first: same, last: same}), do: nil

  defp changes(%Window{prev_value: nil, real_prev_value: real} = window) when real != nil,
    do: count_changes(Window.values(window), real, 0)

  defp changes(%Window{prev_value: nil} = window) do
    [first | rest] = Window.values(window)
    count_changes(rest, first, 1)
  end

  defp changes(window), do: count_changes(Window.values(window), window.prev_value, 0)

  defp count_changes([], _previous, count), do: :erlang.float(count)

  defp count_changes([value | rest], previous, count) do
    cond do
      value == previous -> count_changes(rest, previous, count)
      abs(value - previous) < 1.0e-12 * abs(value) -> count_changes(rest, previous, count)
      true -> count_changes(rest, value, count + 1)
    end
  end

  defp resets(%Window{prev_value: nil, first: same, last: same}), do: nil
  defp resets(%Window{first: same, last: same}), do: 0.0

  defp resets(%Window{prev_value: nil} = window) do
    [first | rest] = Window.values(window)
    count_resets(rest, first, 0)
  end

  defp resets(window), do: count_resets(Window.values(window), window.prev_value, 0)

  defp count_resets([], _previous, count), do: :erlang.float(count)

  defp count_resets([value | rest], previous, count) do
    reset? = value < previous and abs(value - previous) >= 1.0e-12 * abs(value)
    count_resets(rest, value, if(reset?, do: count + 1, else: count))
  end

  defp deriv(window) do
    {_intercept, slope} =
      linear_regression(
        Window.values(window),
        Window.timestamps(window),
        window.curr_timestamp
      )

    slope
  end

  @doc """
  The least-squares line through `values` at `timestamps`, as its value at
  `intercept_ms` and its slope per second (`linearRegression`); a constant
  series has slope `0`, an empty one neither.
  """
  @spec linear_regression([float()], [integer()], integer()) :: {value(), value()}
  def linear_regression([], _timestamps, _intercept_ms), do: {nil, nil}

  def linear_regression([first | _rest] = values, timestamps, intercept_ms) do
    if Enum.all?(values, &(&1 == first)) do
      {first, 0.0}
    else
      {v_sum, t_sum, tv_sum, tt_sum, n} =
        values
        |> Enum.zip(timestamps)
        |> Enum.reduce({0.0, 0.0, 0.0, 0.0, 0}, fn {v, t}, {vs, ts, tvs, tts, n} ->
          dt = (t - intercept_ms) / 1000
          {vs + v, ts + dt, tvs + dt * v, tts + dt * dt, n + 1}
        end)

      t_diff = tt_sum - t_sum * t_sum / n
      slope = if abs(t_diff) >= 1.0e-6, do: (tv_sum - t_sum * v_sum / n) / t_diff, else: 0.0
      {v_sum / n - slope * t_sum / n, slope}
    end
  end

  @doc """
  The `phi` quantile of `values`, interpolated between the two nearest
  ranks as Prometheus does (`quantile`); `phi` below `0` is `-Inf` and
  above `1` is `+Inf`, both held to the largest double.
  """
  @spec quantile(value(), [float()]) :: value()
  def quantile(nil, _values), do: nil
  def quantile(_phi, []), do: nil
  def quantile(phi, _values) when phi < 0, do: -@inf
  def quantile(phi, _values) when phi > 1, do: @inf

  def quantile(phi, values) do
    sorted = values |> Enum.sort() |> List.to_tuple()
    n = tuple_size(sorted)
    rank = phi * (n - 1)
    floored = floor(rank)
    lower = max(0, floored)
    upper = min(n - 1, lower + 1)
    weight = rank - floored
    elem(sorted, lower) * (1 - weight) + elem(sorted, upper) * weight
  end

  defp sum(values) do
    values |> Enum.sum() |> :erlang.float()
  rescue
    ArithmeticError -> Enum.reduce(values, 0.0, &add/2)
  end

  defp add(a, b) do
    a + b
  rescue
    ArithmeticError -> if a > 0, do: @inf, else: -@inf
  end

  defp sub(a, b), do: add(a, -b)

  defp mul(a, b) do
    a * b
  rescue
    ArithmeticError -> if a > 0 == b > 0, do: @inf, else: -@inf
  end

  defp divide(numerator, denominator) do
    if denominator == 0, do: by_zero(numerator), else: quotient(numerator, denominator)
  end

  defp by_zero(numerator) do
    cond do
      numerator > 0 -> @inf
      numerator < 0 -> -@inf
      true -> nil
    end
  end

  defp quotient(numerator, denominator) do
    numerator / denominator
  rescue
    ArithmeticError -> if numerator > 0 == denominator > 0, do: @inf, else: -@inf
  end
end
