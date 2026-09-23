defmodule SmolqueryVictoriaMetrics.Query do
  @moduledoc """
  `/api/v1/query` and `/api/v1/query_range`: MetricsQL over the edge's table,
  answered in the Prometheus HTTP API's JSON (PL-70, T-564).

      GET|POST /api/v1/query?query=up&time=1695000000
      GET|POST /api/v1/query_range?query=rate(m[1m])&start=...&end=...&step=15s

  The arguments come from the URL and, for a `POST`, from a form-encoded
  body as Grafana sends them; the body wins. They are read as
  VictoriaMetrics v1.152.0's `QueryHandler` and `QueryRangeHandler` read
  them (`app/vmselect/prometheus/prometheus.go`):

    * `query` is required;
    * `/api/v1/query` takes `time`, now by default, and `step`, 5 minutes
      by default, which is the window a bare selector looks back at the
      least;
    * `/api/v1/query_range` takes `start` (5 minutes ago), `end` (now) and
      `step` (5 minutes). An `end` before `start` is `start` plus 5
      minutes, as VictoriaMetrics has it. A grid of 50 points or more is
      aligned to the step, `start` down and `end` up with the count kept,
      unless `nocache=1`, as `AdjustStartEnd` does so dashboards share
      points;
    * `timeout` bounds the whole request, held to the runtime's
      `max_query_duration_ms` (VictoriaMetrics' `-search.maxQueryDuration`,
      30 s), which is also its default. It is one deadline: each query
      service job the request runs is given what is left of it, and a
      selector reached after it has passed is not read.

  Times and durations are read by `SmolqueryVictoriaMetrics.Params`. The
  expression is parsed by `SmolqueryVictoriaMetrics.MetricsQL` and
  evaluated by `SmolqueryVictoriaMetrics.Eval`, reading samples through
  `SmolqueryVictoriaMetrics.Samples`. An instant query of a bare range
  vector, `m[5m]`, answers that window's raw samples as a matrix, as
  VictoriaMetrics does, and of any other window, `q[5m]` or `q[5m:1m]`,
  the range query of `q` over that window at the subquery's step, also a
  matrix (`IsRollup`). An expression that is always a scalar (`1+1`,
  `time()`, `scalar(x)`) answers `resultType` `scalar` from
  `/api/v1/query`, which Grafana's connection test, `query=1%2B1`, expects;
  VictoriaMetrics itself answers a one-point vector there. From
  `/api/v1/query_range` a scalar is a one-series matrix with no labels.

  ## Refusals

  `SmolqueryVictoriaMetrics.Errors`' JSON form, with:

    * 400 `bad_data` — `query` missing, past `max_query_bytes`
      (`SmolqueryVictoriaMetrics.Runtime`) or not UTF-8, or a time or
      duration that does not read;
    * 422 `execution` — an expression that does not parse, a function not
      ported, an argument of the wrong kind, a selector with no non-empty
      matcher, duplicate series, a statement the engine refused, and a
      query past `max_series`, `max_samples`, `max_samples_per_query` or
      `max_points_per_series`;
    * 503 `timeout` — a request past its deadline; the job running then is
      cancelled;
    * 503 `unavailable`, with `retry-after` — the query service is not
      running here or is at its job limit, or a job failed for the
      service's reasons rather than the query's
      (`Smolquery.QueryService.Client.service_failure?/1`): an engine that
      did not start, died or ran out of memory, a disk or connection error,
      a worker or a buffer node holding unsealed samples that could not be
      reached. Grafana and vmalert retry these; a 422 they do not.

  ## Telemetry

  Each answered query emits `[:smolquery, :victoriametrics, :query]` with
  `%{series: n, samples: n, duration_us: n, fetch_us: n}`: what it read into
  the node, which `Smolquery.Telemetry` counts so the ceilings can be sized
  from production numbers, and how long it took. `fetch_us` is the time
  spent in `SmolqueryVictoriaMetrics.Samples.select/4`, summed over the
  query's selectors: the grouped query and copying its lists into the
  node. The rest of `duration_us` is parsing, the rollup sweep and
  evaluation; rendering the JSON comes after it and is not counted.
  """

  import Plug.Conn

  alias Smolquery.QueryService.Client
  alias SmolqueryVictoriaMetrics.Errors
  alias SmolqueryVictoriaMetrics.Eval
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.Params
  alias SmolqueryVictoriaMetrics.Response
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Samples

  @default_step_ms 300_000
  @min_points_for_alignment 50

  @doc """
  Answers an instant query (`:instant`) or a range query (`:range`).
  """
  @spec call(Plug.Conn.t(), Runtime.t(), :instant | :range) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime, kind) do
    started = System.monotonic_time(:microsecond)

    case Params.read(conn) do
      {:ok, pairs, conn} -> run(conn, runtime, kind, Params.values(pairs), started)
      {:error, reason, conn} -> refuse(conn, reason)
    end
  end

  defp run(conn, runtime, kind, params, started) do
    fetch_us = :counters.new(1, [:write_concurrency])

    with {:ok, query} <- query(params),
         {:ok, query} <- Params.query_text(query, runtime.max_query_bytes),
         {:ok, grid} <- grid(kind, params, now_ms()),
         {:ok, timeout} <- bad_data(Params.timeout(params, runtime.max_query_duration_ms)),
         {:ok, expr} <- parse(query) do
      deadline = System.monotonic_time(:millisecond) + timeout

      context =
        Map.merge(grid, %{
          lookback_ms: runtime.lookback_ms,
          max_points: runtime.max_points_per_series,
          fetch: timed(fetch_us, fetcher(runtime, deadline))
        })

      kind |> evaluate(expr, context, started) |> answer(conn, fetch_us)
    else
      {:error, reason} -> refuse(conn, reason)
    end
  end

  defp query(%{"query" => query}) when query != "", do: {:ok, query}
  defp query(_params), do: {:error, {:bad_data, "missing `query` arg"}}

  defp grid(:instant, params, now) do
    with {:ok, time} <- bad_data(Params.time(params, "time", now)),
         {:ok, step} <- bad_data(Params.duration(params, "step", @default_step_ms)) do
      {:ok, %{start_ms: time, end_ms: time, step_ms: step}}
    end
  end

  defp grid(:range, params, now) do
    with {:ok, start} <- bad_data(Params.time(params, "start", now - @default_step_ms)),
         {:ok, finish} <- bad_data(Params.time(params, "end", now)),
         {:ok, step} <- bad_data(Params.duration(params, "step", @default_step_ms)) do
      finish = if start > finish, do: start + @default_step_ms, else: finish
      {start, finish} = align(start, finish, step, Map.get(params, "nocache") in ["1", "true"])
      {:ok, %{start_ms: start, end_ms: finish, step_ms: step}}
    end
  end

  @doc """
  VictoriaMetrics' `AdjustStartEnd`: with 50 points or more and caching not
  refused, `start` rounds down to a multiple of `step` and `end` up, keeping
  the number of points.
  """
  @spec align(integer(), integer(), pos_integer(), boolean()) :: {integer(), integer()}
  def align(start, finish, _step, true), do: {start, finish}

  def align(start, finish, step, false) do
    points = div(finish - start, step) + 1

    if points < @min_points_for_alignment do
      {start, finish}
    else
      aligned_start = start - rem(start, step)
      adjust = rem(finish, step)
      aligned_end = if adjust > 0, do: finish + step - adjust, else: finish
      extra = max(div(aligned_end - aligned_start, step) + 1 - points, 0)
      {aligned_start, aligned_end - extra * step}
    end
  end

  defp bad_data({:ok, value}), do: {:ok, value}
  defp bad_data({:error, message}), do: {:error, {:bad_data, message}}

  defp parse(query) do
    case MetricsQL.parse(query) do
      {:ok, expr} -> {:ok, expr}
      {:error, {:unknown_function, name}} -> {:error, {:parse, "unknown function #{name}()"}}
      {:error, {_kind, message}} -> {:error, {:parse, message}}
    end
  end

  defp timed(counter, fetch) do
    fn selector, range ->
      started = System.monotonic_time(:microsecond)
      result = fetch.(selector, range)
      :counters.add(counter, 1, System.monotonic_time(:microsecond) - started)
      result
    end
  end

  @doc """
  How a query reads its selectors: `SmolqueryVictoriaMetrics.Samples.select/4`
  under the request's one `deadline` (monotonic milliseconds) and its one
  sample budget. Each read is given the time left, and refused with
  `:timeout` once none is; each is held to `max_samples` or to what is left
  of `max_samples_per_query`, whichever is less, and a read that would pass
  the latter is `{:too_many_samples_per_query, max}`.
  """
  @spec fetcher(Runtime.t(), integer()) :: Eval.fetch()
  def fetcher(%Runtime{} = runtime, deadline) do
    read = :counters.new(1, [])
    per_selector = runtime.max_samples
    per_query = runtime.max_samples_per_query

    fn selector, range ->
      limit = min(per_selector, per_query - :counters.get(read, 1))

      with {:ok, left} <- time_left(deadline),
           {:ok, series} <-
             budgeted(
               Samples.select(runtime, selector, range, max_samples: limit, timeout_ms: left),
               limit < per_selector,
               per_query
             ) do
        :counters.add(read, 1, Enum.reduce(series, 0, &(length(&1.timestamps) + &2)))
        {:ok, series}
      end
    end
  end

  defp time_left(deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      left when left > 0 -> {:ok, left}
      _past -> {:error, :timeout}
    end
  end

  defp budgeted({:error, {:too_many_samples, _limit}}, true, per_query),
    do: {:error, {:too_many_samples_per_query, per_query}}

  defp budgeted(result, _query_budget_binds, _per_query), do: result

  defp evaluate(:instant, expr, context, started) do
    cond do
      Eval.raw?(expr) ->
        with {:ok, series, stats} <- Eval.raw(expr, context.start_ms, context) do
          {:ok, &Response.matrix(series, &1), stats, started}
        end

      match?({:ok, _child, _grid}, Eval.instant_range(expr, context.start_ms, context.step_ms)) ->
        {:ok, child, {start, finish, step}} =
          Eval.instant_range(expr, context.start_ms, context.step_ms)

        {start, finish} = align(start, finish, step, false)

        evaluate(
          :range,
          child,
          %{context | start_ms: start, end_ms: finish, step_ms: step},
          started
        )

      true ->
        with {:ok, series, stats} <- Eval.run(expr, context) do
          {:ok, instant(expr, series, context.start_ms), stats, started}
        end
    end
  end

  defp evaluate(:range, expr, context, started) do
    with {:ok, series, stats} <- Eval.run(expr, context) do
      {:ok, &Response.matrix(series, &1), stats, started}
    end
  end

  defp instant(expr, series, time) do
    if Eval.scalar?(expr) do
      value =
        case series do
          [%Eval.Series{values: [{_t, value} | _rest]} | _more] -> value
          [] -> nil
        end

      &Response.scalar({time, value}, &1)
    else
      &Response.vector(series, &1)
    end
  end

  defp answer({:ok, render, stats, started}, conn, fetch_us) do
    duration_us = System.monotonic_time(:microsecond) - started

    :telemetry.execute(
      [:smolquery, :victoriametrics, :query],
      %{
        series: stats.series,
        samples: stats.samples,
        duration_us: duration_us,
        fetch_us: :counters.get(fetch_us, 1)
      },
      %{}
    )

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, render.(%{series: stats.series, duration_ms: div(duration_us, 1000)}))
  end

  defp answer({:error, reason}, conn, _fetch_us), do: refuse(conn, reason)

  defp refuse(conn, reason), do: Errors.send_error(conn, failure(reason))

  @doc """
  The answer a refused query takes, as `SmolqueryVictoriaMetrics.Errors`
  sends it.
  """
  @spec failure(term()) :: Errors.t()
  def failure({:bad_data, message}), do: {400, "bad_data", message, nil}

  def failure({:parse, message}),
    do: execution("cannot parse the query: #{message}")

  def failure({:unsupported, what}),
    do: execution("#{what} is not supported by this edge yet")

  def failure({kind, message})
      when kind in [
             :empty_selector,
             :duplicate_series,
             :invalid_at,
             :arity,
             :invalid_grid,
             :invalid_argument
           ],
      do: execution(message)

  def failure({:too_many_points, message}),
    do: execution("#{message}; see SMOLQUERY_VICTORIAMETRICS_MAX_POINTS_PER_SERIES")

  def failure({:too_many_series, max}),
    do:
      execution(
        "the query selects more than #{max} series; narrow its selectors or raise " <>
          "SMOLQUERY_VICTORIAMETRICS_MAX_SERIES"
      )

  def failure({:too_many_samples, max}),
    do:
      execution(
        "the query reads more than #{max} samples; narrow its selectors or time range, " <>
          "raise its step, or raise SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES"
      )

  def failure({:too_many_samples_per_query, max}),
    do:
      execution(
        "the query's selectors read more than #{max} samples between them; narrow them " <>
          "or the time range, or raise SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES_PER_QUERY"
      )

  def failure({:invalid_time, ms}), do: {400, "bad_data", "time #{ms} ms is out of range", nil}

  def failure(:timeout),
    do:
      {503, "timeout",
       "the query did not finish within its timeout and was cancelled; see the `timeout` " <>
         "arg and SMOLQUERY_VICTORIAMETRICS_MAX_QUERY_DURATION_MS", nil}

  def failure(:cancelled), do: {503, "timeout", "the query was cancelled", nil}

  def failure(:too_many_jobs),
    do: {503, "unavailable", "too many queries in flight, retry later", 1}

  def failure(:query_service_unavailable),
    do: {503, "unavailable", "the query service is not available here", 5}

  def failure({:job, {:hot_tier_unavailable, _reason}}), do: hot_tier_unavailable()
  def failure({:job, {:hot_tier_unavailable, _ref, _reason}}), do: hot_tier_unavailable()

  def failure({:job, error}) do
    message = job_message(error)

    if Client.service_failure?(error),
      do: {503, "unavailable", message <> "; retry", 1},
      else: execution(message)
  end

  def failure(reason), do: {500, "internal", "query failed: #{inspect(reason)}", nil}

  defp hot_tier_unavailable,
    do:
      {503, "unavailable",
       "a buffer node holding unsealed samples for this query could not be reached; retry", 1}

  defp job_message({:invalid_query, message}) when is_binary(message), do: message
  defp job_message(error) when is_exception(error), do: Exception.message(error)
  defp job_message(error), do: "query failed: #{inspect(error)}"

  defp execution(message), do: {422, "execution", message, nil}

  defp now_ms, do: System.system_time(:millisecond)
end
