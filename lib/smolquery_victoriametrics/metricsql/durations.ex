defmodule SmolqueryVictoriaMetrics.MetricsQL.Durations do
  @moduledoc """
  Duration text to milliseconds, as `DurationValue` in VictoriaMetrics'
  `metricsql` v0.87.4 computes it (PL-70, T-563).

  A duration is one or more parts, each a non-negative decimal and a unit:
  `ms`, `s`, `m`, `h`, `d`, `w` (7 days), `y` (365 days), or `i`, one step of
  the query. Units are case-insensitive except that minutes are a lower-case
  `m`: `M` is the number multiplier, a million. Parts add up (`1h30m`), and a
  part after the first may be negative, which makes every later part negative
  too (`1h-5m` is 55 minutes, `2h-5m10s` is 2 h - 5 m - 10 s), as long as it
  is below zero: `1h-0m5m` is 65 minutes, as VictoriaMetrics has it. A leading
  minus negates the whole. A part too large for a double is refused. A bare decimal is seconds (`3600`, `1.5`, `-10`).

  The value is split into a fixed part in milliseconds and a part in steps,
  because `i` is only known once a query has a step. `to_ms/2` resolves both
  and truncates toward zero into the int64 range, as Go's conversion does.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Literal

  @part ~r/\A(-?)([0-9]+(?:\.[0-9]*)?)([mM][sS]|m|[sShHdDwWyYiI])/
  @unit_ms %{
    "ms" => 1,
    "s" => 1000,
    "m" => 60_000,
    "h" => 3_600_000,
    "d" => 86_400_000,
    "w" => 604_800_000,
    "y" => 31_536_000_000
  }
  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807

  @doc """
  Splits `text` into `{ms, steps}`.
  """
  @spec parse(String.t()) :: {:ok, {number(), number()}} | {:error, String.t()}
  def parse(""), do: {:error, "duration cannot be empty"}

  def parse(text) do
    case seconds(text) do
      {:ok, seconds} -> {:ok, {seconds * 1000, 0}}
      :error -> parts(text, text, false, {0, 0})
    end
  end

  @doc """
  The milliseconds of `text` at `step_ms`, truncated toward zero.
  """
  @spec to_ms(String.t(), integer()) :: {:ok, integer()} | {:error, String.t()}
  def to_ms(text, step_ms) do
    with {:ok, {ms, steps}} <- parse(text), do: {:ok, resolve(ms, steps, step_ms)}
  end

  @doc """
  `ms + steps * step_ms`, truncated toward zero and clamped to the int64 range.
  """
  @spec resolve(number(), number(), integer()) :: integer()
  def resolve(ms, steps, step_ms) do
    total = ms + steps * step_ms

    cond do
      total > @max_int64 -> @max_int64
      total < @min_int64 -> @min_int64
      true -> trunc(total)
    end
  end

  defp seconds(text) do
    {sign, unsigned} = sign(text)

    with true <- String.match?(text, ~r/[0-9.]\z/),
         {:ok, value} when is_float(value) <- Literal.decimal(unsigned) do
      {:ok, sign * value}
    else
      _not_seconds -> :error
    end
  end

  defp sign(<<?-, rest::binary>>), do: {-1, rest}
  defp sign(<<?+, rest::binary>>), do: {1, rest}
  defp sign(text), do: {1, text}

  defp parts("", _text, _negative, total), do: {:ok, total}

  defp parts(rest, text, negative, {ms, steps}) do
    case Regex.run(@part, rest) do
      [part, minus, number, unit] ->
        tail = binary_part(rest, byte_size(part), byte_size(rest) - byte_size(part))

        with {:ok, value} <- part_value(minus, number, text, negative),
             do: parts(tail, text, negative or value < 0, add({ms, steps}, value, unit))

      nil ->
        {:error, "cannot parse duration #{inspect(text)}"}
    end
  end

  defp part_value(minus, number, text, negative) do
    case Literal.decimal(number) do
      {:ok, value} when is_float(value) -> {:ok, signed(value, minus, negative)}
      _too_big -> {:error, "too big duration #{inspect(text)}"}
    end
  end

  defp signed(value, "-", _negative), do: -value
  defp signed(value, _minus, true), do: -value
  defp signed(value, _minus, false), do: value

  defp add({ms, steps}, value, unit) when unit in ["i", "I"], do: {ms, steps + value}

  defp add({ms, steps}, value, unit),
    do: {ms + value * Map.fetch!(@unit_ms, String.downcase(unit)), steps}
end
