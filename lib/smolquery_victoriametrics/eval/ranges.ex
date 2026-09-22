defmodule SmolqueryVictoriaMetrics.Eval.Ranges do
  @moduledoc """
  The transforms that read a series along the grid rather than one point at
  a time, ported from VictoriaMetrics v1.152.0's `transform.go` (PL-70,
  T-565).

    * `running_sum`, `running_max`, `running_min`, `running_avg`: the value
      so far at each point, from the first point with a value; a point with
      none repeats the value before it. They drop `__name__`.
    * `range_sum`, `range_max`, `range_min`, `range_avg`: the running value
      at the last point, at every point; they drop `__name__` too.
    * `range_first`, `range_last`, `range_quantile(phi, q)`, `range_stddev`,
      `range_stdvar`, `range_mad`, `range_median` (through the parser's
      built-in `range_quantile(0.5, q)`): one statistic of the whole series
      at every point; `range_zscore`, `range_normalize(q, ...)` and
      `range_linear_regression` rescale or fit each point;
      `range_trim_outliers(k, q)`, `range_trim_spikes(phi, q)` and
      `range_trim_zscore(z, q)` blank the points they find.
    * `keep_last_value`, `keep_next_value` and `interpolate` fill the gaps
      of a series; `remove_resets` adds back what counter resets took;
      `smooth_exponential(q, sf)` smooths.

  VictoriaMetrics' own quirks are kept: `running_avg` counts the points
  with no value in its denominator, and a series whose range is infinite is
  left out of `range_normalize`.
  """

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.Rollup

  @running ~w(running_sum running_max running_min running_avg)
  @range_running ~w(range_sum range_max range_min range_avg)
  @whole ~w(range_first range_last range_stddev range_stdvar range_mad range_zscore
    range_linear_regression keep_last_value keep_next_value interpolate remove_resets)
  @with_parameter ~w(range_quantile range_trim_outliers range_trim_spikes range_trim_zscore)

  @doc "The transforms of this module."
  @spec functions() :: [String.t()]
  def functions,
    do:
      @running ++
        @range_running ++ @whole ++ @with_parameter ++ ~w(range_normalize smooth_exponential)

  @doc "Applies the transform `name` to its evaluated `args`."
  @spec apply(String.t(), [[Series.t()]]) :: {:ok, [Series.t()]} | {:error, Args.reason()}
  def apply(name, args) when name in @running do
    with :ok <- Args.count(args, 1), do: {:ok, Enum.map(hd(args), &running(name, &1))}
  end

  def apply("range_" <> kind = name, args) when name in @range_running do
    with :ok <- Args.count(args, 1) do
      {:ok, Enum.map(hd(args), &(running("running_" <> kind, &1) |> last_everywhere()))}
    end
  end

  def apply(name, args) when name in @whole do
    with :ok <- Args.count(args, 1), do: {:ok, Enum.map(hd(args), &whole(name, &1))}
  end

  def apply(name, args) when name in @with_parameter do
    with :ok <- Args.count(args, 2),
         {:ok, parameters} <- Args.scalar(hd(args), 0) do
      parameter = List.first(parameters)
      {:ok, Enum.map(Enum.at(args, 1), &with_parameter(name, parameter, &1))}
    end
  end

  def apply("range_normalize", args),
    do: {:ok, args |> Enum.concat() |> Enum.flat_map(&normalize/1)}

  def apply("smooth_exponential", args) do
    with :ok <- Args.count(args, 2),
         {:ok, factors} <- Args.scalar(Enum.at(args, 1), 1) do
      {:ok, Enum.map(hd(args), &smooth(&1, factors))}
    end
  end

  defp running(name, %Series{labels: labels} = series) do
    values = Series.values(series)
    {leading, rest} = Enum.split_while(values, &is_nil/1)

    running =
      case rest do
        [] ->
          []

        [first | tail] ->
          {tail, _last} =
            tail
            |> Enum.with_index(1)
            |> Enum.map_reduce(first, fn
              {nil, _index}, previous -> {previous, previous}
              {v, index}, previous -> step(name, previous, v, index) |> then(&{&1, &1})
            end)

          [first | tail]
      end

    %{Series.put_values(series, leading ++ running) | labels: Series.drop_name(labels)}
  end

  defp step("running_sum", previous, v, _index), do: Value.add(previous, v)
  defp step("running_max", previous, v, _index), do: if(previous > v, do: previous, else: v)
  defp step("running_min", previous, v, _index), do: if(previous < v, do: previous, else: v)

  defp step("running_avg", previous, v, index),
    do: Value.add(previous, Value.divide(Value.sub(v, previous), index + 1.0))

  defp last_everywhere(series) do
    case series |> Series.values() |> Enum.reject(&is_nil/1) |> List.last() do
      nil -> series
      last -> Series.map_values(series, fn _v -> last end)
    end
  end

  defp first_everywhere(series) do
    case series |> Series.values() |> Enum.find(&(&1 != nil)) do
      nil -> series
      first -> Series.map_values(series, fn _v -> first end)
    end
  end

  defp whole("range_first", series), do: first_everywhere(series)
  defp whole("range_last", series), do: last_everywhere(series)

  defp whole("range_stddev", series),
    do: everywhere(series, Value.sqrt(stdvar(Series.values(series))))

  defp whole("range_stdvar", series), do: everywhere(series, stdvar(Series.values(series)))
  defp whole("range_mad", series), do: everywhere(series, mad(Series.values(series)))

  defp whole("range_zscore", series) do
    values = Series.values(series)
    stddev = Value.sqrt(stdvar(values))
    avg = mean(values)
    Series.map_values(series, &Value.divide(Value.sub(&1, avg), stddev))
  end

  defp whole("range_linear_regression", series) do
    timestamps = Series.timestamps(series)

    case timestamps do
      [] ->
        series

      [intercept | _rest] ->
        {v, k} = regression(Series.values(series), timestamps, intercept)

        %{
          series
          | values:
              Enum.map(timestamps, &{&1, Value.add(v, Value.mul(k, (&1 - intercept) / 1000))})
        }
    end
  end

  defp whole("keep_last_value", series),
    do: Series.put_values(series, carry(Series.values(series)))

  defp whole("keep_next_value", series) do
    values = series |> Series.values() |> Enum.reverse() |> carry() |> Enum.reverse()
    Series.put_values(series, values)
  end

  defp whole("interpolate", series),
    do: Series.put_values(series, interpolate(Series.values(series)))

  defp whole("remove_resets", series),
    do: Series.put_values(series, remove_resets(Series.values(series)))

  defp carry(values) do
    {values, _last} =
      Enum.map_reduce(values, List.first(values), fn
        nil, last -> {last, last}
        v, _last -> {v, v}
      end)

    values
  end

  defp everywhere(series, value), do: Series.map_values(series, fn _v -> value end)

  defp with_parameter("range_quantile", phi, series) do
    values = Series.values(series)

    case Enum.reject(values, &is_nil/1) do
      [] -> series
      present -> everywhere(series, Value.quantile_sorted(phi || 0.0, Enum.sort(present)))
    end
  end

  defp with_parameter("range_trim_outliers", k, series) do
    values = Series.values(series)
    limit = Value.mul(k || 0.0, mad(values))
    median = Value.quantile(0.5, values)

    Series.map_values(
      series,
      &if(Value.gt?(abs_value(Value.sub(&1, median)), limit), do: nil, else: &1)
    )
  end

  defp with_parameter("range_trim_spikes", phi, series) do
    phi = (phi || 0.0) / 2
    sorted = series |> Series.values() |> Enum.reject(&is_nil/1) |> Enum.sort()
    high = Value.quantile_sorted(1 - phi, sorted)
    low = Value.quantile_sorted(phi, sorted)

    Series.map_values(series, fn
      nil -> nil
      v -> if Value.gt?(v, high) or Value.lt?(v, low), do: nil, else: v
    end)
  end

  defp with_parameter("range_trim_zscore", z, series) do
    z = abs(z || 0.0)
    values = Series.values(series)
    stddev = Value.sqrt(stdvar(values))
    avg = mean(values)

    Series.map_values(series, fn v ->
      score = abs_value(Value.divide(Value.sub(v, avg), stddev))
      if Value.gt?(score, z), do: nil, else: v
    end)
  end

  defp normalize(series) do
    present = series |> Series.values() |> Enum.reject(&is_nil/1)

    case present do
      [] ->
        []

      _values ->
        {low, high} = Enum.min_max(present)
        range = Value.sub(high, low)

        if Value.inf?(range) or range == nil,
          do: [],
          else: [Series.map_values(series, &Value.divide(Value.sub(&1, low), range))]
    end
  end

  defp smooth(series, factors) do
    values = Series.values(series)
    {nans, after_nans} = Enum.split_while(values, &is_nil/1)
    {infs, rest} = Enum.split_while(after_nans, &Value.inf?/1)
    {skipped, rest} = if rest == [], do: {nans, after_nans}, else: {nans ++ infs, rest}

    case rest do
      [] ->
        series

      [first | tail] ->
        factors = Enum.drop(factors, length(skipped) + 1)

        {tail, _avg} = tail |> Enum.zip(factors) |> Enum.map_reduce(first, &smooth_step/2)

        Series.put_values(series, skipped ++ [first | tail])
    end
  end

  defp smooth_step({nil, _sf}, avg), do: {nil, avg}

  defp smooth_step({v, sf}, avg) do
    if Value.inf?(v) do
      {avg, avg}
    else
      sf = (sf || 1.0) |> max(0.0) |> min(1.0)
      avg = Value.add(Value.mul(avg, 1 - sf), Value.mul(v, sf))
      {avg, avg}
    end
  end

  defp regression(values, timestamps, intercept) do
    case values do
      [only] ->
        {only, 0.0}

      _many ->
        {present, times} =
          values
          |> Enum.zip(timestamps)
          |> Enum.reject(fn {v, _t} -> is_nil(v) end)
          |> Enum.unzip()

        if present == [],
          do: {nil, nil},
          else: Rollup.linear_regression(present, times, intercept)
    end
  end

  @doc """
  The population variance of a series' values as VictoriaMetrics'
  `stdvar`: `nil` for none, `0` for one value (even a missing one).
  """
  @spec stdvar([Value.t()]) :: Value.t()
  def stdvar([]), do: nil
  def stdvar([_only]), do: 0.0
  def stdvar(values), do: Value.stdvar(values)

  defp mean(values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      present -> Value.divide(Value.sum(present), :erlang.float(length(present)))
    end
  end

  defp mad(values) do
    median = Value.quantile(0.5, values)

    values
    |> Enum.map(&abs_value(Value.sub(&1, median)))
    |> then(&Value.quantile(0.5, &1))
  end

  defp abs_value(nil), do: nil
  defp abs_value(v), do: abs(v)

  @doc """
  Fills the inner gaps of `values` by linear interpolation between the
  values around them (`transformInterpolate`); leading and trailing gaps
  stay.
  """
  @spec interpolate([Value.t()]) :: [Value.t()]
  def interpolate(values) do
    {leading, rest} = Enum.split_while(values, &is_nil/1)
    {trailing, middle} = rest |> Enum.reverse() |> Enum.split_while(&is_nil/1)
    leading ++ fill_gaps(Enum.reverse(middle), nil) ++ trailing
  end

  defp fill_gaps([], _previous), do: []

  defp fill_gaps([nil | _rest] = values, previous) do
    {gap, rest} = Enum.split_while(values, &is_nil/1)
    next = List.first(rest, previous)
    delta = Value.divide(Value.sub(next, previous), length(gap) + 1.0)

    {filled, last} =
      Enum.map_reduce(gap, previous, fn _nil, acc ->
        acc = Value.add(acc, delta)
        {acc, acc}
      end)

    filled ++ fill_gaps(rest, last)
  end

  defp fill_gaps([v | rest], _previous), do: [v | fill_gaps(rest, v)]

  @doc """
  Adds back what each counter reset took, skipping gaps
  (`removeCounterResetsMaybeNaNs`): a fall by less than an eighth adds the
  fall, any other the value fallen from.
  """
  @spec remove_resets([Value.t()]) :: [Value.t()]
  def remove_resets(values) do
    {leading, rest} = Enum.split_while(values, &is_nil/1)

    case rest do
      [] ->
        values

      [first | _tail] ->
        {corrected, _state} = Enum.map_reduce(rest, {0.0, first}, &reset_step/2)
        leading ++ corrected
    end
  end

  defp reset_step(nil, state), do: {nil, state}

  defp reset_step(v, {correction, previous}) do
    correction =
      cond do
        v >= previous ->
          correction

        Value.lt?(Value.mul(Value.sub(previous, v), 8.0), previous) ->
          Value.add(correction, Value.sub(previous, v))

        true ->
          Value.add(correction, previous)
      end

    {Value.add(v, correction), {correction, v}}
  end
end
