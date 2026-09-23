defmodule SmolqueryVictoriaMetrics.Eval.Transform do
  @moduledoc """
  MetricsQL's transform functions over evaluated series, a port of
  VictoriaMetrics v1.152.0's `app/vmselect/promql/transform.go` (PL-70,
  T-565). Every argument arrives evaluated to its series: a number is a
  scalar series and a string a series named by it
  (`SmolqueryVictoriaMetrics.Eval.Args`).

  This module holds the functions of one value at a time (`abs`, `ceil`,
  `floor`, `round`, `exp`, `ln`, `log2`, `log10`, `sqrt`, `sgn`, the
  trigonometric functions, `deg`, `rad`, `clamp`, `clamp_min`,
  `clamp_max`, `bitmap_and`, `bitmap_or`, `bitmap_xor`, and the calendar
  functions `hour`, `minute`, `day_of_month`, `day_of_week`, `day_of_year`,
  `days_in_month`, `month`, `year`, in UTC), the scalar makers (`time`,
  `start`, `end`, `step`, `pi`, `now`, `scalar`, `vector`), `absent`,
  `union`, the sorts and `limit_offset`, `drop_empty_series` and
  `drop_common_labels`. It hands `range_*`, `running_*` and gap filling to
  `SmolqueryVictoriaMetrics.Eval.Ranges`, the label functions to
  `SmolqueryVictoriaMetrics.Eval.LabelFunctions`, and the histogram
  functions to `SmolqueryVictoriaMetrics.Eval.Histogram`.

  A function of one value drops `__name__`, unless it does not change what
  the series measures (`ceil`, `floor`, `round`, `clamp*`) or the call says
  `keep_metric_names` (`transformFuncsKeepMetricName`). Out of Go's `math`,
  a value outside a function's domain is `nil` (`ln(-1)`, `asin(2)`), and a
  pole or an overflow is an infinity (`ln(0)`, `exp(1000)`).

  `rand`, `rand_normal`, `rand_exponential` and `timezone_offset` are not
  ported and answer `{:unsupported, _}`: Go's generator and time zone
  database are not reproduced here.
  """

  import Bitwise

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Histogram
  alias SmolqueryVictoriaMetrics.Eval.LabelFunctions
  alias SmolqueryVictoriaMetrics.Eval.Ranges
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral

  @keep_names ~w(ceil clamp clamp_max clamp_min floor round)
  @math ~w(abs ceil floor exp ln log2 log10 sqrt sin cos tan asin acos atan sinh cosh tanh
    asinh acosh atanh deg rad)
  @calendar ~w(hour minute day_of_month day_of_week day_of_year days_in_month month year)
  @constants ~w(time start end step pi now)
  @sorts ~w(sort sort_desc sort_by_label sort_by_label_desc sort_by_label_numeric
    sort_by_label_numeric_desc)
  @others ~w(sgn round clamp clamp_min clamp_max bitmap_and bitmap_or bitmap_xor scalar vector
    absent union limit_offset drop_empty_series drop_common_labels)

  @typedoc "The grid a transform is evaluated on: its points and its bounds."
  @type grid :: %{
          required(:timestamps) => [integer()],
          required(:start_ms) => integer(),
          required(:end_ms) => integer(),
          required(:step_ms) => pos_integer()
        }

  @type reason :: Args.reason() | {:unsupported, String.t()}

  @doc "The transforms evaluated, across this module and those it hands to."
  @spec functions() :: [String.t()]
  def functions,
    do:
      @math ++
        @calendar ++
        @constants ++
        @sorts ++
        @others ++ Ranges.functions() ++ LabelFunctions.functions() ++ Histogram.functions()

  @doc "Applies the transform `call` to its evaluated `args` on `grid`."
  @spec apply(FuncExpr.t(), [[Series.t()]], grid()) :: {:ok, [Series.t()]} | {:error, reason()}
  def apply(%FuncExpr{name: name} = call, args, grid) do
    name = String.downcase(name)
    dispatch(family(name), name, call, args, grid)
  end

  defp family(name) do
    cond do
      name in @math -> :math
      name in @calendar -> :calendar
      name in @constants -> :constant
      name in @sorts -> :sort
      name in @others -> :other
      true -> handed_to(name)
    end
  end

  defp handed_to(name) do
    cond do
      name in Ranges.functions() -> Ranges
      name in LabelFunctions.functions() -> LabelFunctions
      name in Histogram.functions() -> Histogram
      true -> :unsupported
    end
  end

  defp dispatch(:math, name, call, args, _grid), do: one_value(call, name, args, &math(name, &1))
  defp dispatch(:calendar, name, call, args, grid), do: calendar(call, name, args, grid)
  defp dispatch(:constant, name, _call, args, grid), do: constant(name, args, grid)
  defp dispatch(:sort, name, _call, args, _grid), do: sort(name, args)
  defp dispatch(:other, name, call, args, grid), do: other(name, call, args, grid)

  defp dispatch(:unsupported, name, _call, _args, _grid),
    do: {:error, {:unsupported, "transform function #{name}()"}}

  defp dispatch(module, name, _call, args, _grid), do: module.apply(name, args)

  defp one_value(call, name, args, fun) do
    with :ok <- Args.count(args, 1),
         do: {:ok, values(call, name, hd(args), fn _i, v -> fun.(v) end)}
  end

  defp values(%FuncExpr{keep_metric_names: keep}, name, series, fun) do
    keep = keep or name in @keep_names

    Enum.map(series, fn %Series{labels: labels, values: points} = one ->
      labels = if keep, do: labels, else: Series.drop_name(labels)

      points =
        points |> Enum.with_index() |> Enum.map(fn {{t, v}, i} -> {t, fun.(i, v)} end)

      %{one | labels: labels, values: points}
    end)
  end

  @doc "A function of one value, with Go's `math` results at its edges."
  @spec math(String.t(), Value.t()) :: Value.t()
  def math(_name, nil), do: nil
  def math("abs", v), do: abs(v)
  def math("ceil", v), do: Float.ceil(v)
  def math("floor", v), do: Float.floor(v)
  def math("exp", v), do: guarded(fn -> :math.exp(v) end, Value.inf())
  def math("ln", v), do: logarithm(v, &:math.log/1)
  def math("log2", v), do: logarithm(v, &:math.log2/1)
  def math("log10", v), do: logarithm(v, &:math.log10/1)
  def math("sqrt", v), do: Value.sqrt(v)
  def math(name, v) when name in ["sin", "cos", "tan"], do: periodic(name, v)
  def math("asin", v), do: if(abs(v) > 1, do: nil, else: :math.asin(v))
  def math("acos", v), do: if(abs(v) > 1, do: nil, else: :math.acos(v))
  def math("atan", v), do: :math.atan(v)
  def math("sinh", v), do: guarded(fn -> :math.sinh(v) end, sign_inf(v))
  def math("cosh", v), do: guarded(fn -> :math.cosh(v) end, Value.inf())
  def math("tanh", v), do: :math.tanh(v)
  def math("asinh", v), do: if(Value.inf?(v), do: v, else: :math.asinh(v))
  def math("acosh", v), do: acosh(v)
  def math("atanh", v), do: atanh(v)
  def math("deg", v), do: Value.mul(v, 180 / :math.pi())
  def math("rad", v), do: Value.mul(v, :math.pi() / 180)

  defp logarithm(v, _fun) when v < 0, do: nil
  defp logarithm(+0.0, _fun), do: Value.neg_inf()
  defp logarithm(-0.0, _fun), do: Value.neg_inf()
  defp logarithm(v, fun), do: if(Value.inf?(v), do: v, else: fun.(v))

  defp periodic(_name, v)
       when v >= 1.797_693_134_862_315_7e308 or v <= -1.797_693_134_862_315_7e308,
       do: nil

  defp periodic("sin", v), do: :math.sin(v)
  defp periodic("cos", v), do: :math.cos(v)
  defp periodic("tan", v), do: :math.tan(v)

  defp acosh(v) when v < 1, do: nil
  defp acosh(v), do: if(Value.inf?(v), do: v, else: :math.acosh(v))

  defp atanh(v) when v > 1 or v < -1, do: nil
  defp atanh(1.0), do: Value.inf()
  defp atanh(-1.0), do: Value.neg_inf()
  defp atanh(v), do: :math.atanh(v)

  defp sign_inf(v) when v < 0, do: Value.neg_inf()
  defp sign_inf(_v), do: Value.inf()

  defp guarded(fun, overflow) do
    Value.clamp(fun.())
  rescue
    ArithmeticError -> overflow
  end

  defp calendar(call, name, args, grid) do
    case args do
      [] -> {:ok, values(call, name, [time_series(grid)], fn _i, v -> date_part(name, v) end)}
      [arg] -> {:ok, values(call, name, arg, fn _i, v -> date_part(name, v) end)}
      _more -> Args.invalid("too many args; got #{length(args)}; want up to 1")
    end
  end

  defp date_part(_name, nil), do: nil

  defp date_part(name, v) do
    case DateTime.from_unix(trunc(v)) do
      {:ok, datetime} -> :erlang.float(date_value(name, datetime))
      {:error, _reason} -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp date_value("hour", datetime), do: datetime.hour
  defp date_value("minute", datetime), do: datetime.minute
  defp date_value("day_of_month", datetime), do: datetime.day
  defp date_value("day_of_week", datetime), do: rem(Date.day_of_week(datetime), 7)
  defp date_value("day_of_year", datetime), do: Date.day_of_year(datetime)
  defp date_value("days_in_month", datetime), do: Date.days_in_month(datetime)
  defp date_value("month", datetime), do: datetime.month
  defp date_value("year", datetime), do: datetime.year

  defp time_series(grid), do: Series.generate(grid.timestamps, &(&1 / 1000))

  defp constant(name, args, grid) do
    with :ok <- Args.count(args, 0), do: {:ok, [constant_series(name, grid)]}
  end

  defp constant_series("time", grid), do: time_series(grid)
  defp constant_series("start", grid), do: Series.constant(grid.timestamps, grid.start_ms / 1000)
  defp constant_series("end", grid), do: Series.constant(grid.timestamps, grid.end_ms / 1000)
  defp constant_series("step", grid), do: Series.constant(grid.timestamps, grid.step_ms / 1000)
  defp constant_series("pi", grid), do: Series.constant(grid.timestamps, :math.pi())

  defp constant_series("now", grid),
    do: Series.constant(grid.timestamps, System.system_time(:microsecond) / 1_000_000)

  defp other("sgn", call, args, _grid), do: one_value(call, "sgn", args, &sign/1)

  defp other("round", call, args, grid) do
    nearest =
      case args do
        [_series] -> {:ok, Enum.map(grid.timestamps, fn _t -> 1.0 end)}
        [_series, nearest] -> Args.scalar(nearest, 1)
        _other -> Args.invalid("unexpected number of args: #{length(args)}; want 1 or 2")
      end

    with {:ok, nearest} <- nearest do
      nearest = List.to_tuple(nearest)
      {:ok, values(call, "round", hd(args), fn i, v -> round_to(v, elem(nearest, i)) end)}
    end
  end

  defp other("clamp", call, args, _grid) do
    with :ok <- Args.count(args, 3),
         {:ok, mins} <- Args.scalar(Enum.at(args, 1), 1),
         {:ok, maxs} <- Args.scalar(Enum.at(args, 2), 2) do
      bounds = mins |> Enum.zip(maxs) |> List.to_tuple()

      {:ok,
       values(call, "clamp", hd(args), fn i, v ->
         {low, high} = elem(bounds, i)

         cond do
           Value.gt?(v, high) -> high
           Value.lt?(v, low) -> low
           true -> v
         end
       end)}
    end
  end

  defp other(name, call, args, _grid) when name in ["clamp_min", "clamp_max"] do
    with :ok <- Args.count(args, 2),
         {:ok, bounds} <- Args.scalar(Enum.at(args, 1), 1) do
      bounds = List.to_tuple(bounds)
      beyond = if name == "clamp_min", do: &Value.lt?/2, else: &Value.gt?/2

      {:ok,
       values(call, name, hd(args), fn i, v ->
         bound = elem(bounds, i)
         if beyond.(v, bound), do: bound, else: v
       end)}
    end
  end

  defp other("bitmap_" <> op = name, call, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, masks} <- Args.scalar(Enum.at(args, 1), 1) do
      masks = List.to_tuple(masks)
      {:ok, values(call, name, hd(args), fn i, v -> bitmap(op, v, elem(masks, i)) end)}
    end
  end

  defp other("scalar", call, args, grid) do
    with :ok <- Args.count(args, 1) do
      case {call.args, args} do
        {[%StringLiteral{value: text}], _args} ->
          {:ok, [Series.constant(grid.timestamps, Value.parse(text))]}

        {_exprs, [[one]]} ->
          {:ok, [%{one | labels: %{}}]}

        _other ->
          {:ok, [Series.constant(grid.timestamps, nil)]}
      end
    end
  end

  defp other("vector", _call, args, _grid),
    do: with(:ok <- Args.count(args, 1), do: {:ok, hd(args)})

  defp other("absent", call, args, grid) do
    with :ok <- Args.count(args, 1) do
      {:ok, [absent(hd(call.args), hd(args), grid.timestamps)]}
    end
  end

  defp other("union", _call, args, grid), do: {:ok, union(args, grid.timestamps)}

  defp other("limit_offset", _call, [limit, offset, series], _grid) do
    with {:ok, limit} <- Args.integer(limit, 0),
         {:ok, offset} <- Args.integer(offset, 1) do
      {:ok,
       series |> Series.drop_empty() |> Enum.drop(max(offset, 0)) |> Enum.take(max(limit, 0))}
    end
  end

  defp other("limit_offset", _call, args, _grid), do: Args.count(args, 3)

  defp other("drop_empty_series", _call, args, _grid),
    do: with(:ok <- Args.count(args, 1), do: {:ok, Series.drop_empty(hd(args))})

  defp other("drop_common_labels", _call, args, _grid) do
    with :ok <- Args.at_least(args, 1), do: {:ok, drop_common_labels(Enum.concat(args))}
  end

  defp sign(nil), do: 0.0
  defp sign(v) when v < 0, do: -1.0
  defp sign(v) when v > 0, do: 1.0
  defp sign(_v), do: 0.0

  defp bitmap(_op, nil, _mask), do: nil
  defp bitmap(_op, _v, nil), do: nil

  defp bitmap(op, v, mask) do
    a = band(trunc(v), 0xFFFF_FFFF_FFFF_FFFF)
    b = band(trunc(mask), 0xFFFF_FFFF_FFFF_FFFF)

    case op do
      "and" -> :erlang.float(band(a, b))
      "or" -> :erlang.float(bor(a, b))
      "xor" -> :erlang.float(bxor(a, b))
    end
  end

  @doc """
  `round(v, nearest)` as VictoriaMetrics rounds: half away from zero to a
  multiple of `nearest`, then cut to `nearest`'s decimal digits.
  """
  @spec round_to(Value.t(), Value.t()) :: Value.t()
  def round_to(nil, _nearest), do: nil
  def round_to(_v, nil), do: nil

  def round_to(v, nearest) do
    p10 = Value.pow(10.0, :erlang.float(-decimal_exponent(nearest)))
    shifted = Value.add(v, 0.5 * copysign(nearest, v))

    case Value.sub(shifted, Value.mod(shifted, nearest)) do
      nil -> nil
      rounded -> Value.divide(truncate(Value.mul(rounded, p10)), p10)
    end
  end

  defp copysign(n, v) when v < 0, do: -abs(n)
  defp copysign(n, _v), do: abs(n)

  defp truncate(nil), do: nil
  defp truncate(v), do: if(Value.inf?(v), do: v, else: Float.round(v - :math.fmod(v, 1.0), 0))

  @doc """
  The power of ten of a float's last significant decimal digit, as
  VictoriaMetrics' `decimal.FromFloat` finds it: `0.01` is `-2`, `1` is `0`,
  `100` is `2`.
  """
  @spec decimal_exponent(float()) :: integer()
  def decimal_exponent(+0.0), do: 0
  def decimal_exponent(-0.0), do: 0

  def decimal_exponent(f) do
    f = abs(f)

    cond do
      Value.inf?(f) -> 0
      f == Float.floor(f) and f < 1.844_674_407_370_955_2e19 -> integer_exponent(trunc(f), 0)
      true -> fraction_exponent(f)
    end
  end

  defp integer_exponent(u, scale) when u >= 36_028_797_018_963_968,
    do: integer_exponent(div(u, 10), scale + 1)

  defp integer_exponent(u, scale) when u != 0 and rem(u, 10) == 0,
    do: integer_exponent(div(u, 10), scale + 1)

  defp integer_exponent(_u, scale), do: scale

  defp fraction_exponent(f) do
    {f, scale, precision} =
      if f > 1.0e6 or f < 1.0e-6 do
        {_mantissa, exp} = frexp(f)
        exp = exp |> max(-1022) |> min(1023)
        scale = trunc(exp * (:math.log(2) / :math.log(10)))
        precision = if f > 1.0e6, do: 1.0e15, else: 1.0e12
        {f * :math.pow(10, -scale), scale, precision}
      else
        {f, 0, 1.0e12}
      end

    {u, scale} = multiply_until_whole(f, scale, precision)
    if rem(u, 10) != 0, do: scale, else: scale + 1
  end

  defp multiply_until_whole(f, scale, precision) when f < precision do
    whole = Float.floor(f)
    frac = f - whole

    cond do
      frac * precision < whole -> {trunc(whole), scale}
      (1 - frac) * precision < whole -> {trunc(whole) + 1, scale}
      true -> multiply_until_whole(f * 100, scale - 2, precision)
    end
  end

  defp multiply_until_whole(f, scale, _precision), do: {trunc(f), scale}

  defp frexp(f) do
    <<_sign::1, exponent::11, _fraction::52>> = <<f::float-64>>
    {nil, exponent - 1022}
  end

  @doc """
  `absent(q)`: one series, `1` where `q` has no value at a point; labelled
  by `q`'s `=` matchers when `q` is a selector with one filter set.
  """
  @spec absent(term(), [Series.t()], [integer()]) :: Series.t()
  def absent(expr, series, timestamps) do
    columns =
      case series do
        [] -> Enum.map(timestamps, fn _t -> [] end)
        _some -> series |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)
      end

    values =
      Enum.zip_with(timestamps, columns, fn t, column ->
        {t, if(Enum.all?(column, &is_nil/1), do: 1.0)}
      end)

    %Series{labels: absent_labels(expr), values: values}
  end

  @doc "The labels `absent` gives its series (`getAbsentTimeseries`)."
  @spec absent_labels(term()) :: Series.labels()
  def absent_labels(%MetricExpr{filter_sets: [filters]}) do
    for %LabelFilter{name: name, op: :eq, value: value} <- filters,
        name != "__name__",
        value != "",
        into: %{},
        do: {name, value}
  end

  def absent_labels(_expr), do: %{}

  @doc "`union(a, b, ...)` and `(a, b, ...)` (`transformUnion`)."
  @spec union([[Series.t()]], [integer()]) :: [Series.t()]
  def union([], timestamps), do: [Series.constant(timestamps, nil)]

  def union(args, _timestamps) do
    if Enum.all?(args, &Series.scalar?/1) do
      Enum.map(args, &hd/1)
    else
      args
      |> Enum.concat()
      |> Enum.uniq_by(& &1.labels)
    end
  end

  defp drop_common_labels(series) do
    total = length(series)

    common =
      series
      |> Enum.flat_map(fn one ->
        [
          {"__name__", Series.label(one.labels, "__name__")}
          | Map.to_list(Series.drop_name(one.labels))
        ]
      end)
      |> Enum.frequencies()
      |> Enum.filter(fn {_pair, count} -> count == total end)
      |> Enum.map(fn {{name, _value}, _count} -> name end)

    Enum.map(series, fn one -> %{one | labels: Map.drop(one.labels, common)} end)
  end

  defp sort(name, args) when name in ["sort", "sort_desc"] do
    with :ok <- Args.count(args, 1) do
      less = if name == "sort", do: &value_less?/2, else: &value_greater?/2
      {:ok, Enum.sort(hd(args), fn a, b -> not less.(b, a) end)}
    end
  end

  defp sort(name, args) do
    with :ok <- Args.at_least(args, 2),
         {:ok, labels} <- sort_labels(tl(args)) do
      numeric = String.contains?(name, "numeric")
      desc = String.ends_with?(name, "_desc")
      compare = &label_less?(&1, &2, labels, numeric, desc)
      {:ok, Enum.sort(hd(args), fn a, b -> not compare.(b, a) end)}
    end
  end

  defp sort_labels(args) do
    args
    |> Enum.with_index(1)
    |> Args.collect(&sort_label/1)
  end

  defp sort_label({arg, index}) do
    case Args.string(arg, index) do
      {:ok, label} ->
        {:ok, label}

      {:error, {:invalid_argument, message}} ->
        Args.invalid("cannot parse label ##{index} for sorting: #{message}")
    end
  end

  defp label_less?(a, b, labels, numeric, desc) do
    Enum.reduce_while(labels, false, fn label, _acc ->
      x = Series.label(a.labels, label)
      y = Series.label(b.labels, label)

      cond do
        x == y -> {:cont, false}
        desc -> {:halt, text_less?(y, x, numeric)}
        true -> {:halt, text_less?(x, y, numeric)}
      end
    end)
  end

  defp text_less?(a, b, false), do: a < b
  defp text_less?(a, b, true), do: numeric_less?(a, b)

  @doc """
  Compares two label values as `sort_by_label_numeric` does
  (`numericLess`): runs of digits by their number, the rest as text.
  """
  @spec numeric_less?(String.t(), String.t()) :: boolean()
  def numeric_less?(_a, ""), do: false
  def numeric_less?("", _b), do: true

  def numeric_less?(a, b) do
    a_num = num_prefix(a)
    b_num = num_prefix(b)
    a = binary_part(a, byte_size(a_num), byte_size(a) - byte_size(a_num))
    b = binary_part(b, byte_size(b_num), byte_size(b) - byte_size(b_num))

    cond do
      a_num == "" and b_num != "" ->
        false

      b_num == "" and a_num != "" ->
        true

      a_num != "" and Value.parse(a_num) != Value.parse(b_num) ->
        Value.parse(a_num) < Value.parse(b_num)

      true ->
        text_step(a, b)
    end
  end

  defp text_step(a, b) do
    a_text = non_num_prefix(a)
    b_text = non_num_prefix(b)

    if a_text != b_text,
      do: a_text < b_text,
      else:
        numeric_less?(String.replace_prefix(a, a_text, ""), String.replace_prefix(b, b_text, ""))
  end

  defp num_prefix(s) do
    case Regex.run(~r/\A[-+]?(?:\d+\.?\d*|\.\d+)/, s) do
      [prefix] -> if prefix =~ ~r/\d/, do: prefix, else: ""
      nil -> ""
    end
  end

  defp non_num_prefix(s) do
    case Regex.run(~r/\A\D*/, s) do
      [prefix] -> prefix
      nil -> ""
    end
  end

  defp value_less?(a, b), do: last_differing(a, b, &Kernel.</2)
  defp value_greater?(a, b), do: last_differing(a, b, &Kernel.>/2)

  defp last_differing(a, b, compare) do
    a
    |> Series.values()
    |> Enum.zip(Series.values(b))
    |> Enum.reverse()
    |> Enum.reduce_while(false, fn
      {nil, nil}, _acc -> {:cont, false}
      {nil, _y}, _acc -> {:halt, true}
      {_x, nil}, _acc -> {:halt, false}
      {x, y}, _acc when x == y -> {:cont, false}
      {x, y}, _acc -> {:halt, compare.(x, y)}
    end)
  end
end
