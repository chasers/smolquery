defmodule SmolqueryVictoriaMetrics.Response do
  @moduledoc """
  A query's answer in the Prometheus HTTP API's JSON (PL-70, T-564), as
  VictoriaMetrics v1.152.0 writes it (`app/vmselect/prometheus/*.qtpl`):

      {"status":"success","isPartial":false,
       "data":{"resultType":"matrix","result":[
         {"metric":{"__name__":"up","job":"node"},"values":[[1695000000,"1"],[1695000015,"1"]]}]},
       "stats":{"seriesFetched":"1","executionTimeMsec":3}}

    * `matrix/1`: a range query's series, each point `[seconds, "value"]`,
      a point with no value left out and a series with none left out;
    * `vector/1`: an instant query's series, one `"value"` each;
    * `scalar/1`: an instant query of a scalar, `[seconds, "value"]`.

  `metric` holds `__name__` first when the series kept it, then its labels
  in name order. `seriesFetched` is a string, as VictoriaMetrics writes it
  for vmalert's sake. `isPartial` is always `false`: a query that could not
  read everything fails instead.

  ## Numbers

  A timestamp is seconds, `1695000000` or `1695000000.5`, never
  `1.695e9`. A value is a string as Go's `strconv.FormatFloat(v, 'f', -1,
  64)` writes it, which VictoriaMetrics' templates use: the fewest digits
  that read back as the same double, never an exponent (`1`, `0.1`,
  `1000000000000000000000`, `0.0000001`), and `NaN`. The largest double
  and its negation are `+Inf` and `-Inf`: that is how the edge stores an
  infinity (PL-70 D6), and how a rollup holds one.
  """

  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value

  @typedoc "The request's counters: series fetched, and the time taken."
  @type stats :: %{series: non_neg_integer(), duration_ms: non_neg_integer()}

  @doc """
  A range query's answer.
  """
  @spec matrix([Series.t()], stats()) :: iodata()
  def matrix(series, stats) do
    result =
      for %Series{labels: labels, values: values} <- series,
          points = for({t, v} <- values, v != nil, do: point(t, v)),
          points != [] do
        ["{\"metric\":", metric(labels), ",\"values\":[", Enum.intersperse(points, ","), "]}"]
      end

    success("matrix", ["[", Enum.intersperse(result, ","), "]"], stats)
  end

  @doc """
  An instant query's answer: each series' first point, a series without one
  left out.
  """
  @spec vector([Series.t()], stats()) :: iodata()
  def vector(series, stats) do
    result =
      for %Series{labels: labels, values: [{t, v} | _rest]} <- series, v != nil do
        ["{\"metric\":", metric(labels), ",\"value\":", point(t, v), "}"]
      end

    success("vector", ["[", Enum.intersperse(result, ","), "]"], stats)
  end

  @doc """
  An instant query's scalar answer.
  """
  @spec scalar({integer(), float() | nil}, stats()) :: iodata()
  def scalar({t, v}, stats), do: success("scalar", point(t, v), stats)

  defp success(type, result, stats) do
    [
      "{\"status\":\"success\",\"isPartial\":false,\"data\":{\"resultType\":\"",
      type,
      "\",\"result\":",
      result,
      "},\"stats\":{\"seriesFetched\":\"",
      Integer.to_string(stats.series),
      "\",\"executionTimeMsec\":",
      Integer.to_string(stats.duration_ms),
      "}}"
    ]
  end

  defp metric(labels) do
    {name, rest} = Map.pop(labels, "__name__")
    pairs = Enum.sort(rest)
    pairs = if name, do: [{"__name__", name} | pairs], else: pairs

    [
      "{",
      pairs
      |> Enum.map(fn {k, v} -> [JSON.encode!(k), ":", JSON.encode!(v)] end)
      |> Enum.intersperse(","),
      "}"
    ]
  end

  defp point(t, v), do: ["[", timestamp(t), ",\"", value(v), "\"]"]

  @doc """
  A millisecond timestamp as the seconds the API writes: `1695000000`,
  `1695000000.5`.
  """
  @spec timestamp(integer()) :: String.t()
  def timestamp(ms) do
    sign = if ms < 0, do: "-", else: ""
    seconds = Integer.to_string(div(abs(ms), 1000))

    case rem(abs(ms), 1000) do
      0 ->
        sign <> seconds

      fraction ->
        digits = fraction |> Integer.to_string() |> String.pad_leading(3, "0")
        sign <> seconds <> "." <> String.trim_trailing(digits, "0")
    end
  end

  @doc """
  A value as the API writes it: Go's `strconv.FormatFloat(v, 'f', -1, 64)`,
  `NaN` for `nil`, and `+Inf` / `-Inf` at the largest double.
  """
  @spec value(float() | nil) :: String.t()
  def value(v), do: Value.format(v)
end
