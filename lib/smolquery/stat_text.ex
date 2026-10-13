defmodule Smolquery.StatText do
  @moduledoc """
  Reads a date or timestamp bound that DuckDB printed as text.

  DuckDB prints a `DATE` as `YYYY-MM-DD` and a `TIMESTAMP` or `TIMESTAMP_NS`
  as `YYYY-MM-DD HH:MM:SS[.fraction]`, with as many year digits as the value
  needs: `10000-01-01` and `294247-01-10 04:00:54.775806` are in range for
  DuckDB, and `Date`/`NaiveDateTime` hold them, but
  `Date.from_iso8601/1` and `NaiveDateTime.from_iso8601/1` read four year
  digits only. This reads four to seven, separated by a space or a `T`, so
  the same functions read the writer's `RETURN_STATS` text and the hot
  manifest's `to_iso8601` encoding.

  A value with no `Date`/`NaiveDateTime` form here (`infinity`,
  `-infinity`, a `(BC)` suffix, a leading `-`, a day that does not exist) is
  `nil`, which a stats reader takes as an unknown bound.

  A fraction keeps every digit up to nanoseconds and is rounded to
  microseconds in the direction asked for: `:floor` for a minimum, `:ceil` for
  a maximum, so a bound is never narrower than the value it came from (T-475).
  Every timestamp comes back at microsecond precision, `{_, 6}`.
  """

  @date ~r/\A(\d{4,7})-(\d{2})-(\d{2})\z/
  @naive_datetime ~r/\A(\d{4,7})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?\z/

  @doc """
  The `Date` that `text` prints, or `nil`.

      iex> Smolquery.StatText.date("10000-01-01")
      Date.new!(10000, 1, 1)

      iex> Smolquery.StatText.date("0044-03-15 (BC)")
      nil

  """
  @spec date(String.t()) :: Date.t() | nil
  def date(text) when is_binary(text) do
    with [_text | parts] <- Regex.run(@date, text),
         [year, month, day] <- Enum.map(parts, &String.to_integer/1),
         {:ok, date} <- Date.new(year, month, day) do
      date
    else
      _unreadable -> nil
    end
  end

  def date(_text), do: nil

  @doc """
  The `NaiveDateTime` that `text` prints, at microsecond precision, its
  fraction rounded toward `direction`, or `nil`.

      iex> Smolquery.StatText.naive_datetime("2024-01-01 00:00:00.000000001", :ceil)
      ~N[2024-01-01 00:00:00.000001]

      iex> Smolquery.StatText.naive_datetime("10000-01-01T00:00:00", :floor)
      NaiveDateTime.new!(10000, 1, 1, 0, 0, 0, {0, 6})

  """
  @spec naive_datetime(String.t(), :floor | :ceil) :: NaiveDateTime.t() | nil
  def naive_datetime(text, direction) when is_binary(text) and direction in [:floor, :ceil] do
    with [_text | parts] <- Regex.run(@naive_datetime, text),
         {fields, fraction} = Enum.split(parts, 6),
         [year, month, day, hour, minute, second] <- Enum.map(fields, &String.to_integer/1),
         {:ok, datetime} <- NaiveDateTime.new(year, month, day, hour, minute, second, {0, 6}) do
      NaiveDateTime.add(datetime, micros(nanos(fraction), direction), :microsecond)
    else
      _unreadable -> nil
    end
  end

  def naive_datetime(_text, _direction), do: nil

  defp nanos([]), do: 0
  defp nanos([fraction]), do: fraction |> String.pad_trailing(9, "0") |> String.to_integer()

  defp micros(ns, :floor), do: Integer.floor_div(ns, 1_000)
  defp micros(ns, :ceil), do: -Integer.floor_div(-ns, 1_000)
end
