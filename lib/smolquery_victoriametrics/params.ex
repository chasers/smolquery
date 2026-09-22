defmodule SmolqueryVictoriaMetrics.Params do
  @moduledoc """
  The time and duration arguments of the Prometheus query API, read as
  VictoriaMetrics v1.152.0 reads them (`lib/httputil`: `GetTime`,
  `GetDuration`) (PL-70, T-564).

  A time is Unix seconds, whole or with a fraction (`1695000000`,
  `1695000000.123`), or RFC 3339 (`2023-09-18T01:20:00Z`,
  `2023-09-18T04:20:00.5+03:00`). A time before the epoch is the epoch, as
  VictoriaMetrics' storage has no earlier one. A missing time is its
  default rounded down to the second, which keeps a dashboard's queries
  aligned whenever they run.

  A duration is seconds as a number (`15`, `0.5`) or a duration
  (`15s`, `1m30s`, `1h`), and must lie in `1ms` to 100 years. Grafana may
  send `undefined`, which is taken as missing.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL

  @max_duration_ms 100 * 365 * 24 * 3600 * 1000
  @max_time_ms div(9_223_372_036_854_775_807, 1_000_000)

  @doc """
  The time `key` in `params`, in milliseconds, or `default_ms` rounded down
  to the second.
  """
  @spec time(%{String.t() => String.t()}, String.t(), integer()) ::
          {:ok, integer()} | {:error, String.t()}
  def time(params, key, default_ms) do
    case Map.get(params, key, "") do
      "" -> {:ok, default_ms - rem(default_ms, 1000)}
      text -> parse_time(key, text)
    end
  end

  defp parse_time(key, text) do
    case seconds(text) || rfc3339(text) do
      nil -> {:error, "cannot parse #{key}=#{text}: not Unix seconds or RFC 3339"}
      ms -> {:ok, ms |> max(0) |> min(@max_time_ms)}
    end
  end

  defp seconds(text) do
    case Regex.run(~r/\A(-?)(\d+)(?:\.(\d*))?\z/, text) do
      [_all, sign, whole | fraction] ->
        millis = fraction |> List.first("") |> String.pad_trailing(3, "0") |> binary_part(0, 3)
        ms = String.to_integer(whole) * 1000 + String.to_integer(millis)
        if sign == "-", do: -ms, else: ms

      nil ->
        float_seconds(text)
    end
  end

  defp float_seconds(text) do
    case Float.parse(text) do
      {seconds, ""} -> trunc(seconds * 1000)
      _other -> nil
    end
  end

  defp rfc3339(text) do
    case DateTime.from_iso8601(text) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime, :millisecond)
      {:error, _reason} -> nil
    end
  end

  @doc """
  The duration `key` in `params`, in milliseconds, or `default_ms`.
  """
  @spec duration(%{String.t() => String.t()}, String.t(), integer() | nil) ::
          {:ok, integer() | nil} | {:error, String.t()}
  def duration(params, key, default_ms) do
    case Map.get(params, key, "") do
      blank when blank in ["", "undefined"] -> {:ok, default_ms}
      text -> parse_duration(key, text)
    end
  end

  defp parse_duration(key, text) do
    case duration_ms(text) do
      {:ok, ms} when ms > 0 and ms <= @max_duration_ms ->
        {:ok, ms}

      {:ok, ms} ->
        {:error, "#{key}=#{ms}ms is out of allowed range [1ms ... #{@max_duration_ms}ms]"}

      :error ->
        {:error, "cannot parse #{key}=#{inspect(text)}"}
    end
  end

  defp duration_ms(text) do
    case Float.parse(text) do
      {seconds, ""} ->
        {:ok, trunc(seconds * 1000)}

      _duration ->
        case MetricsQL.duration_to_ms(text, 0) do
          {:ok, ms} -> {:ok, ms}
          {:error, _reason} -> :error
        end
    end
  end
end
