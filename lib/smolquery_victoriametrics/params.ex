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

  The arguments themselves come from the URL and, for a form-encoded
  `POST`, from the body, as Go's `Request.ParseForm` gathers them for
  `FormValue`: `read/1` keeps every pair, the body's first, so `values/1`
  lets the body win and `all/2` answers a repeated key such as `match[]`
  with every value it was given (T-566).
  """

  import Plug.Conn

  alias SmolqueryApi.Body
  alias SmolqueryVictoriaMetrics.MetricsQL

  @max_duration_ms 100 * 365 * 24 * 3600 * 1000
  @max_time_ms div(9_223_372_036_854_775_807, 1_000_000)
  @max_body_bytes 1_048_576
  @max_float_seconds 1.0e15

  @typedoc "A request's arguments in the order Go's `r.Form` holds them: the body's, then the URL's."
  @type pairs :: [{String.t(), String.t()}]

  @doc """
  Every argument of `conn`: a form-encoded `POST` body's pairs, then the
  URL's, each in the order sent. A body past 1 MiB is refused, with the
  conn as far as it was read, which is the conn the refusal must be sent on
  (`SmolqueryApi.Body`).
  """
  @spec read(Plug.Conn.t()) ::
          {:ok, pairs(), Plug.Conn.t()} | {:error, {:bad_data, String.t()}, Plug.Conn.t()}
  def read(conn) do
    url = conn.query_string |> URI.query_decoder() |> Enum.to_list()

    case form(conn) do
      {:ok, form, conn} -> {:ok, form ++ url, conn}
      {:error, :too_large, conn} -> {:error, {:bad_data, "the request body is too large"}, conn}
    end
  end

  defp form(%Plug.Conn{method: "POST"} = conn) do
    if form?(get_req_header(conn, "content-type")) do
      with {:ok, body, conn} <- Body.read(conn, @max_body_bytes),
           do: {:ok, body |> URI.query_decoder() |> Enum.to_list(), conn}
    else
      {:ok, [], conn}
    end
  end

  defp form(conn), do: {:ok, [], conn}

  defp form?([type | _rest]), do: String.starts_with?(type, "application/x-www-form-urlencoded")
  defp form?([]), do: false

  @doc """
  A MetricsQL text from a request, checked before it is parsed: at most
  `max_bytes` bytes, as VictoriaMetrics' `-search.maxQueryLen` holds it, and
  valid UTF-8. Neither refusal repeats the text.

      iex> SmolqueryVictoriaMetrics.Params.query_text("up", 16)
      {:ok, "up"}
      iex> SmolqueryVictoriaMetrics.Params.query_text(<<0xFF>>, 16)
      {:error, {:bad_data, "the query is not valid UTF-8"}}
  """
  @spec query_text(binary(), pos_integer()) ::
          {:ok, String.t()} | {:error, {:bad_data, String.t()}}
  def query_text(text, max_bytes) when byte_size(text) > max_bytes,
    do:
      {:error,
       {:bad_data,
        "the query is #{byte_size(text)} bytes, past the #{max_bytes}-byte limit " <>
          "(SMOLQUERY_VICTORIAMETRICS_MAX_QUERY_BYTES)"}}

  def query_text(text, _max_bytes) do
    if String.valid?(text),
      do: {:ok, text},
      else: {:error, {:bad_data, "the query is not valid UTF-8"}}
  end

  @doc """
  One value per key, the first given, as Go's `FormValue` reads it: the
  body's over the URL's.

      iex> SmolqueryVictoriaMetrics.Params.values([{"a", "body"}, {"a", "url"}, {"b", "1"}])
      %{"a" => "body", "b" => "1"}
  """
  @spec values(pairs()) :: %{String.t() => String.t()}
  def values(pairs) do
    Enum.reduce(pairs, %{}, fn {key, value}, acc -> Map.put_new(acc, key, value) end)
  end

  @doc """
  Every value of each of `keys`, key by key, each in the order given.

      iex> pairs = [{"match[]", "a"}, {"match", "b"}, {"match[]", "c"}]
      iex> SmolqueryVictoriaMetrics.Params.all(pairs, ["match[]", "match"])
      ["a", "c", "b"]
  """
  @spec all(pairs(), [String.t()]) :: [String.t()]
  def all(pairs, keys) do
    Enum.flat_map(keys, fn key -> for {^key, value} <- pairs, do: value end)
  end

  @doc """
  The integer `key` in `params`, `0` when missing, as `GetInt` reads it.
  """
  @spec int(%{String.t() => String.t()}, String.t()) :: {:ok, integer()} | {:error, String.t()}
  def int(params, key) do
    with text when text != "" <- Map.get(params, key, ""),
         {n, ""} <- Integer.parse(text) do
      {:ok, n}
    else
      "" -> {:ok, 0}
      _unreadable -> {:error, "cannot parse integer #{inspect(key)}=#{inspect(params[key])}"}
    end
  end

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
      :out_of_range -> {:error, "#{key}=#{text} is out of the range a timestamp holds"}
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
      {seconds, ""} when abs(seconds) <= @max_float_seconds -> trunc(seconds * 1000)
      {_seconds, ""} -> :out_of_range
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
  The request's `timeout` in milliseconds, held to `max_ms`, which is also
  its default: VictoriaMetrics' `-search.maxQueryDuration` bounds whatever a
  client asks for.

      iex> SmolqueryVictoriaMetrics.Params.timeout(%{"timeout" => "100y"}, 30_000)
      {:ok, 30_000}
      iex> SmolqueryVictoriaMetrics.Params.timeout(%{"timeout" => "5s"}, 30_000)
      {:ok, 5_000}
      iex> SmolqueryVictoriaMetrics.Params.timeout(%{}, 30_000)
      {:ok, 30_000}
  """
  @spec timeout(%{String.t() => String.t()}, pos_integer()) ::
          {:ok, pos_integer()} | {:error, String.t()}
  def timeout(params, max_ms) do
    with {:ok, ms} <- duration(params, "timeout", max_ms), do: {:ok, min(ms, max_ms)}
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

      :out_of_range ->
        {:error, "#{key}=#{text} is out of allowed range [1ms ... #{@max_duration_ms}ms]"}

      :error ->
        {:error, "cannot parse #{key}=#{inspect(text)}"}
    end
  end

  defp duration_ms(text) do
    case Float.parse(text) do
      {seconds, ""} when abs(seconds) <= @max_float_seconds ->
        {:ok, trunc(seconds * 1000)}

      {_seconds, ""} ->
        :out_of_range

      _duration ->
        case MetricsQL.duration_to_ms(text, 0) do
          {:ok, ms} -> {:ok, ms}
          {:error, _reason} -> :error
        end
    end
  end
end
