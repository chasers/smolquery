defmodule SmolqueryVictoriaMetrics.Eval.Value do
  @moduledoc """
  One point's value and the arithmetic the evaluator does on it (PL-70,
  T-565), with the IEEE results Go's `float64` gives VictoriaMetrics.

  A value is a float or `nil`, which stands for NaN (a point with no value).
  Elixir floats hold neither NaN nor the infinities, so an infinity is the
  largest double, `±1.7976931348623157e308`, as the edge stores one (PL-70
  D6) and `SmolqueryVictoriaMetrics.Response` writes it back as `+Inf` or
  `-Inf`. Every operation here reads a value at that bound as the infinity:
  `inf - inf` is `nil`, `inf * 0` is `nil`, `1 / 0` is `inf`, `0 / 0` is
  `nil`, and a finite result too large for a double is the infinity of its
  sign. An operation with a `nil` operand is `nil` unless Go's `math` says
  otherwise (`pow(1, NaN)` is `1`).

  `add/2`, `mul/2` and `divide/2` take a guard-only path when neither
  operand is large enough, nor a divisor small enough, for the result to
  leave a double: no closure and no `rescue` for the common point. Over
  3,000 series of 1,000 points (the median of five), `sum by (pod)` went
  from 0.96 s to 0.57 s in `SmolqueryVictoriaMetrics.Eval.Aggregate` and
  `a / b` from 0.91 s to 0.48 s in `SmolqueryVictoriaMetrics.Eval.Binary`.

  Comparisons follow `metricsql/binaryop`: `==` holds for two NaNs and `!=`
  between a NaN and a number; `>`, `<`, `>=` and `<=` never hold with a NaN.
  """

  import Bitwise

  @inf 1.797_693_134_862_315_7e308
  @half_max 8.0e307
  @root_max 1.0e154
  @root_min 1.0e-154

  @type t :: float() | nil

  @doc "The positive infinity, as a value."
  @spec inf() :: float()
  def inf, do: @inf

  @doc "The negative infinity, as a value."
  @spec neg_inf() :: float()
  def neg_inf, do: -@inf

  @doc "Whether `v` is an infinity of either sign."
  @spec inf?(t()) :: boolean()
  def inf?(v) when is_float(v), do: v >= @inf or v <= -@inf
  def inf?(_v), do: false

  @doc """
  A number literal's value: `:inf`, `:neg_inf` and `:nan` as values, a float
  held to the infinities; and a double read back from a frame, where
  Explorer says `:infinity`, `:neg_infinity` and `:nan`, and `nil` for NULL.
  """
  @spec from_number(float() | :inf | :neg_inf | :nan | :infinity | :neg_infinity | nil) :: t()
  def from_number(nil), do: nil
  def from_number(inf) when inf in [:inf, :infinity], do: @inf
  def from_number(neg) when neg in [:neg_inf, :neg_infinity], do: -@inf
  def from_number(:nan), do: nil
  def from_number(v), do: clamp(v * 1.0)

  @doc "A float held to the infinities."
  @spec clamp(float()) :: float()
  def clamp(v) when v >= @inf, do: @inf
  def clamp(v) when v <= -@inf, do: -@inf
  def clamp(v), do: v

  @doc "`a + b`."
  @spec add(t(), t()) :: t()
  def add(a, b)
      when is_float(a) and is_float(b) and a < @half_max and a > -@half_max and b < @half_max and
             b > -@half_max,
      do: a + b

  def add(nil, _b), do: nil
  def add(_a, nil), do: nil
  def add(a, b) when a >= @inf and b <= -@inf, do: nil
  def add(a, b) when a <= -@inf and b >= @inf, do: nil
  def add(a, b) when a >= @inf or b >= @inf, do: @inf
  def add(a, b) when a <= -@inf or b <= -@inf, do: -@inf
  def add(a, b), do: finite(fn -> a + b end, a)

  @doc "`a - b`."
  @spec sub(t(), t()) :: t()
  def sub(a, nil), do: add(a, nil)
  def sub(a, b), do: add(a, 0.0 - b)

  @doc "`a * b`."
  @spec mul(t(), t()) :: t()
  def mul(a, b)
      when is_float(a) and is_float(b) and a < @root_max and a > -@root_max and b < @root_max and
             b > -@root_max,
      do: a * b

  def mul(nil, _b), do: nil
  def mul(_a, nil), do: nil

  def mul(a, b) do
    cond do
      inf?(a) and b == 0.0 -> nil
      inf?(b) and a == 0.0 -> nil
      inf?(a) or inf?(b) -> signed_inf(sign(a) * sign(b))
      true -> finite(fn -> a * b end, sign(a) * sign(b))
    end
  end

  @doc "`a / b`: `x / 0` is the infinity of `x`'s sign and `0 / 0` is `nil`."
  @spec divide(t(), t()) :: t()
  def divide(a, b)
      when is_float(a) and is_float(b) and a < @root_max and a > -@root_max and
             ((b > @root_min and b < @root_max) or (b < -@root_min and b > -@root_max)),
      do: a / b

  def divide(nil, _b), do: nil
  def divide(_a, nil), do: nil

  def divide(a, b) do
    cond do
      inf?(a) and inf?(b) -> nil
      inf?(a) -> signed_inf(sign(a) * nonzero_sign(b))
      inf?(b) -> 0.0
      b == 0.0 and a == 0.0 -> nil
      b == 0.0 -> signed_inf(sign(a) * zero_sign(b))
      true -> finite(fn -> a / b end, sign(a) * sign(b))
    end
  end

  @doc "Go's `math.Mod(a, b)`: the remainder with the sign of `a`."
  @spec mod(t(), t()) :: t()
  def mod(nil, _b), do: nil
  def mod(_a, nil), do: nil

  def mod(a, b) do
    cond do
      b == 0.0 or inf?(a) -> nil
      inf?(b) -> a
      true -> :math.fmod(a, b)
    end
  end

  @doc """
  `a ^ b` as `metricsql/binaryop.Pow`: `NaN ^ b` is `nil`, then Go's
  `math.Pow`.
  """
  @spec pow(t(), t()) :: t()
  def pow(nil, _b), do: nil
  def pow(_a, +0.0), do: 1.0
  def pow(_a, -0.0), do: 1.0
  def pow(1.0, _b), do: 1.0
  def pow(_a, nil), do: nil
  def pow(a, 1.0), do: a

  def pow(a, b) do
    cond do
      a == 0.0 -> zero_pow(b)
      inf?(b) -> inf_exponent(a, b)
      inf?(a) -> inf_base(a, b)
      a < 0 and b != Float.floor(b) -> nil
      true -> finite(fn -> :math.pow(a, b) end, pow_sign(a, b))
    end
  end

  defp zero_pow(b) when b < 0, do: @inf
  defp zero_pow(_b), do: 0.0

  defp inf_exponent(a, _b) when a == -1, do: 1.0

  defp inf_exponent(a, b) do
    if abs(a) < 1 == b > 0, do: 0.0, else: @inf
  end

  defp inf_base(a, b) when a > 0, do: if(b < 0, do: 0.0, else: @inf)

  defp inf_base(_a, b) do
    odd = odd_integer?(b)

    cond do
      b < 0 and odd -> -0.0
      b < 0 -> 0.0
      odd -> -@inf
      true -> @inf
    end
  end

  defp pow_sign(a, b), do: if(a < 0 and odd_integer?(b), do: -1, else: 1)

  defp odd_integer?(b) do
    b == Float.floor(b) and abs(b) < 9.007_199_254_740_992e15 and (trunc(b) &&& 1) == 1
  end

  @doc "Go's `math.Atan2(a, b)`."
  @spec atan2(t(), t()) :: t()
  def atan2(nil, _b), do: nil
  def atan2(_a, nil), do: nil
  def atan2(a, b), do: :math.atan2(a, b)

  @doc "`a == b`, a NaN equal to a NaN."
  @spec eq?(t(), t()) :: boolean()
  def eq?(nil, b), do: b == nil
  def eq?(_a, nil), do: false
  def eq?(a, b), do: a == b

  @doc "`a != b`, a NaN unequal to a number."
  @spec neq?(t(), t()) :: boolean()
  def neq?(a, b), do: not eq?(a, b)

  @doc "`a > b`."
  @spec gt?(t(), t()) :: boolean()
  def gt?(a, b), do: is_float(a) and is_float(b) and a > b

  @doc "`a < b`."
  @spec lt?(t(), t()) :: boolean()
  def lt?(a, b), do: is_float(a) and is_float(b) and a < b

  @doc "`a >= b`."
  @spec gte?(t(), t()) :: boolean()
  def gte?(a, b), do: is_float(a) and is_float(b) and a >= b

  @doc "`a <= b`."
  @spec lte?(t(), t()) :: boolean()
  def lte?(a, b), do: is_float(a) and is_float(b) and a <= b

  @doc "Sums `values`, skipping `nil`; `nil` when all are."
  @spec sum([t()]) :: t()
  def sum(values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      present -> Enum.reduce(present, 0.0, &add(&2, &1))
    end
  end

  @doc """
  The `phi` quantile of `values` as VictoriaMetrics' `quantile`: `nil`s
  dropped, ranks interpolated, `phi` below `0` or above `1` an infinity.
  """
  @spec quantile(t(), [t()]) :: t()
  def quantile(nil, _values), do: nil

  def quantile(phi, values) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      present -> quantile_sorted(phi, Enum.sort(present))
    end
  end

  @doc "The `phi` quantile of sorted values without `nil`."
  @spec quantile_sorted(t(), [float()]) :: t()
  def quantile_sorted(nil, _sorted), do: nil
  def quantile_sorted(_phi, []), do: nil
  def quantile_sorted(phi, _sorted) when phi < 0, do: -@inf
  def quantile_sorted(phi, _sorted) when phi > 1, do: @inf

  def quantile_sorted(phi, sorted) do
    n = length(sorted)
    rank = phi * (n - 1)
    floored = Float.floor(rank)
    lower = max(0, trunc(floored))
    upper = min(n - 1, lower + 1)
    weight = rank - floored
    [low, high] = sorted |> Enum.drop(lower) |> Enum.take(upper - lower + 1) |> pad_pair()
    add(mul(low, 1 - weight), mul(high, weight))
  end

  defp pad_pair([only]), do: [only, only]
  defp pad_pair(pair), do: pair

  @doc """
  The population variance of `values` without their `nil`s, by Welford's
  method as VictoriaMetrics computes it, in this module's arithmetic: `nil`
  for none, and for values holding an infinity, whose `Inf - Inf` makes
  Go's `NaN`; a variance past the largest double is `+Inf`, as it is in Go.
  """
  @spec stdvar([t()]) :: t()
  def stdvar(values) do
    {avg_q, count} =
      values
      |> Enum.reject(&is_nil/1)
      |> Enum.reduce({{0.0, 0.0}, 0}, fn v, {{avg, q}, count} ->
        count = count + 1
        avg_new = add(avg, divide(sub(v, avg), :erlang.float(count)))
        {{avg_new, add(q, mul(sub(v, avg), sub(v, avg_new)))}, count}
      end)

    case {avg_q, count} do
      {_state, 0} -> nil
      {{_avg, q}, count} -> divide(q, :erlang.float(count))
    end
  end

  @doc "The square root, `nil` below zero."
  @spec sqrt(t()) :: t()
  def sqrt(nil), do: nil
  def sqrt(v) when v < 0, do: nil
  def sqrt(v) when v >= @inf, do: @inf
  def sqrt(v), do: :math.sqrt(v)

  @doc """
  A value as Go's `strconv.FormatFloat(v, 'f', -1, 64)` writes it: the fewest
  digits that read back as the same double, never an exponent; `NaN` for
  `nil`, and `+Inf` / `-Inf` at the infinities.
  """
  @spec format(t()) :: String.t()
  def format(nil), do: "NaN"
  def format(v) when v >= @inf, do: "+Inf"
  def format(v) when v <= -@inf, do: "-Inf"

  def format(v) do
    shortest = :erlang.float_to_binary(v * 1.0, [:short])

    {sign, unsigned} =
      case shortest do
        "-" <> rest -> {"-", rest}
        rest -> {"", rest}
      end

    sign <> plain(unsigned)
  end

  @doc """
  A value as Go's `fmt.Sprintf("%g", v)` writes it: the shortest digits, in
  exponent form below `1e-4` and from `1e+06` on (`0.5`, `1e+06`,
  `1.5e-05`), as VictoriaMetrics writes a quantile label.
  """
  @spec format_general(t()) :: String.t()
  def format_general(nil), do: "NaN"
  def format_general(v) when v >= @inf, do: "+Inf"
  def format_general(v) when v <= -@inf, do: "-Inf"
  def format_general(+0.0), do: "0"
  def format_general(-0.0), do: "0"

  def format_general(v) do
    {digits, point} = decimal_digits(abs(v))
    exponent = point - 1
    sign = if v < 0, do: "-", else: ""

    if exponent < -4 or exponent >= 6 do
      <<first::binary-size(1), rest::binary>> = digits
      mantissa = if rest == "", do: first, else: first <> "." <> rest
      exp_sign = if exponent < 0, do: "-", else: "+"
      exp_digits = exponent |> abs() |> Integer.to_string() |> String.pad_leading(2, "0")
      sign <> mantissa <> "e" <> exp_sign <> exp_digits
    else
      format(v)
    end
  end

  defp decimal_digits(v) do
    {mantissa, exponent} =
      case v |> :erlang.float_to_binary([:short]) |> String.split("e") do
        [mantissa] -> {mantissa, 0}
        [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
      end

    [whole, fraction] = String.split(mantissa, ".")
    digits = whole <> fraction
    point = byte_size(whole) + exponent
    trimmed = String.trim_leading(digits, "0")
    {String.trim_trailing(trimmed, "0"), point - (byte_size(digits) - byte_size(trimmed))}
  end

  defp plain(text) do
    {mantissa, exponent} =
      case String.split(text, "e") do
        [mantissa] -> {mantissa, 0}
        [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
      end

    [whole, fraction] = String.split(mantissa, ".")
    fraction = if fraction == "0", do: "", else: fraction
    digits = whole <> fraction
    point = byte_size(whole) + exponent

    {integer, decimals} =
      cond do
        point <= 0 ->
          {"0", String.duplicate("0", -point) <> digits}

        point >= byte_size(digits) ->
          {digits <> String.duplicate("0", point - byte_size(digits)), ""}

        true ->
          String.split_at(digits, point)
      end

    integer = trim_leading_zeros(integer)

    case String.trim_trailing(decimals, "0") do
      "" -> integer
      decimals -> integer <> "." <> decimals
    end
  end

  defp trim_leading_zeros(integer) do
    case String.trim_leading(integer, "0") do
      "" -> "0"
      trimmed -> trimmed
    end
  end

  @doc "A string as Go's `strconv.ParseFloat` reads it; `nil` when it does not."
  @spec parse(String.t()) :: t()
  def parse(text) do
    case String.downcase(text) do
      inf when inf in ["inf", "+inf", "infinity", "+infinity"] -> @inf
      inf when inf in ["-inf", "-infinity"] -> -@inf
      _number -> parse_number(text)
    end
  end

  defp parse_number(text) do
    normalized = if String.starts_with?(text, "."), do: "0" <> text, else: text
    normalized = String.replace(normalized, ~r/^([+-])\./, "\\g{1}0.")
    normalized = String.replace(normalized, ~r/^([+-]?\d+)\.(?=[eE]|$)/, "\\g{1}.0")

    case Float.parse(normalized) do
      {v, ""} -> clamp(v)
      _other -> nil
    end
  end

  defp finite(fun, sign) do
    clamp(fun.())
  rescue
    ArithmeticError -> signed_inf(sign)
  end

  defp signed_inf(sign) when sign < 0, do: -@inf
  defp signed_inf(_sign), do: @inf

  defp sign(v) when v < 0, do: -1
  defp sign(_v), do: 1

  defp nonzero_sign(v) when v < 0, do: -1
  defp nonzero_sign(v), do: zero_sign(v)

  defp zero_sign(-0.0), do: -1
  defp zero_sign(_v), do: 1
end
