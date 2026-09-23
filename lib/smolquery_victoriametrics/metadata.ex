defmodule SmolqueryVictoriaMetrics.Metadata do
  @moduledoc """
  `/api/v1/labels`, `/api/v1/label/<name>/values` and `/api/v1/series`:
  what Grafana's query builder browses, answered from the edge's table in
  the Prometheus HTTP API's JSON (PL-70, T-566).

      GET|POST /api/v1/labels?match[]=up&start=...&end=...&limit=...
      GET|POST /api/v1/label/job/values?match[]=up&start=...&end=...&limit=...
      GET|POST /api/v1/series?match[]=up&match[]=node_load1&start=...&end=...

      {"status":"success","data":["__name__","instance","job"]}
      {"status":"success","data":["api","node"]}
      {"status":"success","data":[{"__name__":"up","job":"api"},{"__name__":"up","job":"node"}]}

  The arguments are read as VictoriaMetrics v1.152.0's `LabelsHandler`,
  `LabelValuesHandler` and `SeriesHandler` read them
  (`app/vmselect/prometheus/prometheus.go`, `getCommonParamsForLabelsAPI`),
  from the URL and a form-encoded `POST` body alike
  (`SmolqueryVictoriaMetrics.Params`):

    * `end` is now when missing, and `start` is 5 minutes before `end`
      when missing or `0`, not the beginning of time as in Prometheus, so
      an unbounded request scans 5 minutes, not the whole table. An `end`
      before `start` is `start`;
    * `match[]`, which may repeat, and `match`, which VictoriaMetrics also
      reads, are MetricsQL selectors; a series matching any of them is
      kept, `or` inside one selector included, as their filter sets are
      pooled. `/api/v1/series` requires one;
    * `limit` is an integer, `0` when missing. The two label routes return
      at most that many, or 100,000 when it is not positive or larger
      (`SmolqueryVictoriaMetrics.Labels`); `/api/v1/series` truncates its
      sorted answer to it when positive;
    * `timeout` bounds each query service job, held to the runtime's
      `max_query_duration_ms`, which is also its default.

  `/api/v1/series` is the rollup engine's series query
  (`SmolqueryVictoriaMetrics.Samples.series/4`) over the pooled selectors,
  so it is held to `max_series` as a query is. Its answer is sorted by the
  metric name and then the labels in name order, and each series is written
  `__name__` first, as VictoriaMetrics writes it.

  A label name in the path must be a Prometheus label name,
  `[a-zA-Z_][a-zA-Z0-9_]*`. One spelled `U__...` is unescaped as the UTF-8
  escaping of Prometheus' proposal 0028 defines, as VictoriaMetrics does,
  so a label whose name is not a legacy one can still be asked for.

  ## Cost

  Each of the three scans every sample of the selected range the predicate
  keeps: the cost is proportional to the range scanned, not to the answer,
  until a series index exists (PL-70, T-569). A selector naming its metric
  with `=` prunes to that metric's segments; one without a metric, or no
  selector at all, prunes by time only.

  ## Refusals

    * 400 `bad_data` — a time, duration or `limit` that does not read, a
      `match[]` that is not a selector, is past `max_query_bytes` or is not
      UTF-8, a label name that is not one, or
      `/api/v1/series` without `match[]`;
    * everything else as `SmolqueryVictoriaMetrics.Query.failure/1` has it:
      422 for a selector with no non-empty matcher or past `max_series`,
      503 for a timeout or an unavailable query service.
  """

  import Plug.Conn

  alias SmolqueryVictoriaMetrics.Errors
  alias SmolqueryVictoriaMetrics.Labels
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.Params
  alias SmolqueryVictoriaMetrics.Query
  alias SmolqueryVictoriaMetrics.Response
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Samples

  @default_range_ms 300_000
  @label_name ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/
  @hex ~r/\A[0-9a-fA-F]{1,6}\z/

  @typedoc "Which of the three routes: label names, one label's values, or series."
  @type route :: :labels | {:label_values, String.t()} | :series

  @typedoc "A request's arguments, read and checked."
  @type request :: %{
          range: {integer(), integer()},
          limit: integer(),
          selector: MetricExpr.t() | nil,
          opts: keyword()
        }

  @doc """
  Answers `route`.
  """
  @spec call(Plug.Conn.t(), Runtime.t(), route()) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime, route) do
    case Params.read(conn) do
      {:ok, pairs, conn} -> respond(conn, runtime, route, pairs)
      {:error, reason, conn} -> refuse(conn, reason)
    end
  end

  defp respond(conn, runtime, route, pairs) do
    with {:ok, route} <- label_name(route),
         :ok <- matches_within(pairs, runtime.max_query_bytes),
         {:ok, request} <- request(route, pairs, now_ms(), runtime.max_query_duration_ms),
         {:ok, data} <- answer(route, runtime, request) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Response.data(data))
    else
      {:error, reason} -> refuse(conn, reason)
    end
  end

  defp refuse(conn, reason), do: Errors.send_error(conn, Query.failure(reason))

  defp matches_within(pairs, max_bytes) do
    pairs
    |> Params.all(["match[]", "match"])
    |> Enum.find_value(:ok, fn match ->
      case Params.query_text(match, max_bytes) do
        {:ok, _match} -> nil
        {:error, {:bad_data, message}} -> {:error, {:bad_data, "match[]: " <> message}}
      end
    end)
  end

  @doc """
  The arguments of a request to `route` from its `pairs`, with `now_ms` as
  the default `end` and `max_timeout_ms` as the default and the ceiling of
  `timeout`.
  """
  @spec request(route(), Params.pairs(), integer(), pos_integer()) ::
          {:ok, request()} | {:error, term()}
  def request(route, pairs, now_ms, max_timeout_ms) do
    params = Params.values(pairs)

    with {:ok, range} <- range(params, now_ms),
         {:ok, limit} <- bad_data(Params.int(params, "limit")),
         {:ok, timeout} <- bad_data(Params.timeout(params, max_timeout_ms)),
         {:ok, selector} <- selector(Params.all(pairs, ["match[]", "match"]), route == :series) do
      {:ok, %{range: range, limit: limit, selector: selector, opts: [timeout_ms: timeout]}}
    end
  end

  @doc """
  The time range of a request, as `getCommonParamsForLabelsAPI` has it:
  `end` now by default, `start` 5 minutes before `end` when missing or `0`,
  an `end` before `start` taken as `start`.
  """
  @spec range(%{String.t() => String.t()}, integer()) ::
          {:ok, {integer(), integer()}} | {:error, {:bad_data, String.t()}}
  def range(params, now_ms) do
    with {:ok, start} <- bad_data(Params.time(params, "start", 0)),
         {:ok, finish} <- bad_data(Params.time(params, "end", now_ms)) do
      finish = max(finish, start)
      start = if start == 0, do: finish - @default_range_ms, else: start
      {:ok, {start, finish}}
    end
  end

  @doc """
  The selectors of `match[]` and `match`, pooled into one: `nil` for none,
  or a refusal when one is `required` and none was given.
  """
  @spec selector([String.t()], boolean()) ::
          {:ok, MetricExpr.t() | nil} | {:error, {:bad_data, String.t()}}
  def selector([], true), do: {:error, {:bad_data, "missing `match[]` arg"}}
  def selector([], false), do: {:ok, nil}

  def selector(matches, _required) do
    matches
    |> Enum.reduce_while({:ok, []}, fn match, {:ok, groups} ->
      case parse_match(match) do
        {:ok, %MetricExpr{filter_sets: sets}} -> {:cont, {:ok, [sets | groups]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, %MetricExpr{filter_sets: groups |> Enum.reverse() |> Enum.concat()}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_match(match) do
    case MetricsQL.parse(match) do
      {:ok, %MetricExpr{filter_sets: [_set | _more]} = expr} ->
        {:ok, expr}

      {:ok, %MetricExpr{}} ->
        {:error, {:bad_data, "cannot parse match[]=#{match}: labelFilterss cannot be empty"}}

      {:ok, expr} ->
        {:error,
         {:bad_data,
          "cannot parse match[]=#{match}: expecting metricSelector; got " <>
            inspect(MetricsQL.to_string(expr))}}

      {:error, {_kind, message}} ->
        {:error, {:bad_data, "cannot parse match[]=#{match}: #{message}"}}
    end
  end

  @doc """
  A label name from a request's path, checked and unescaped: `U__` names are
  Prometheus' UTF-8 escaping, `_2e_` a code point in hex and `__` an
  underscore; one that does not unescape is taken as it is, as
  VictoriaMetrics takes it.

      iex> SmolqueryVictoriaMetrics.Metadata.label_name({:label_values, "U__http_2e_method"})
      {:ok, {:label_values, "http.method"}}
      iex> SmolqueryVictoriaMetrics.Metadata.label_name({:label_values, "job-name"})
      {:error, {:bad_data, "invalid label name \\"job-name\\""}}
  """
  @spec label_name(route()) :: {:ok, route()} | {:error, {:bad_data, String.t()}}
  def label_name({:label_values, name}) do
    if Regex.match?(@label_name, name),
      do: {:ok, {:label_values, unescape(name)}},
      else: {:error, {:bad_data, "invalid label name #{inspect(name)}"}}
  end

  def label_name(route), do: {:ok, route}

  defp unescape("U__" <> escaped = name) do
    case unescape(escaped, []) do
      {:ok, unescaped} -> unescaped
      :error -> name
    end
  end

  defp unescape(name), do: name

  defp unescape("", acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
  defp unescape("__" <> rest, acc), do: unescape(rest, ["_" | acc])

  defp unescape("_" <> rest, acc) do
    with [hex, rest] <- String.split(rest, "_", parts: 2),
         true <- Regex.match?(@hex, hex),
         char when is_binary(char) <- utf8(String.to_integer(hex, 16)) do
      unescape(rest, [char | acc])
    else
      _invalid -> :error
    end
  end

  defp unescape(<<byte, rest::binary>>, acc), do: unescape(rest, [byte | acc])

  defp utf8(code) when code in 0..0xD7FF or code in 0xE000..0x10FFFF, do: <<code::utf8>>
  defp utf8(_code), do: nil

  defp answer(:labels, runtime, request) do
    with {:ok, names} <-
           Labels.names(
             runtime,
             request.selector,
             request.range,
             Labels.limit(request.limit),
             request.opts
           ),
         do: {:ok, JSON.encode!(names)}
  end

  defp answer({:label_values, name}, runtime, request) do
    with {:ok, values} <-
           Labels.values(
             runtime,
             name,
             request.selector,
             request.range,
             Labels.limit(request.limit),
             request.opts
           ),
         do: {:ok, JSON.encode!(values)}
  end

  defp answer(:series, runtime, request) do
    with {:ok, infos} <- Samples.series(runtime, request.selector, request.range, request.opts) do
      series =
        infos
        |> Map.values()
        |> Enum.map(fn %{name: name, labels: labels} -> {name, Enum.sort(labels)} end)
        |> Enum.sort()
        |> truncate(request.limit)
        |> Enum.map(fn {name, labels} ->
          Response.metric(Map.put(Map.new(labels), "__name__", name))
        end)

      {:ok, ["[", Enum.intersperse(series, ","), "]"]}
    end
  end

  defp truncate(series, limit) when limit > 0, do: Enum.take(series, limit)
  defp truncate(series, _limit), do: series

  defp bad_data({:ok, value}), do: {:ok, value}
  defp bad_data({:error, message}), do: {:error, {:bad_data, message}}

  defp now_ms, do: System.system_time(:millisecond)
end
