defmodule Smolquery.Tracing do
  @moduledoc """
  smolquery's telemetry as OpenTelemetry traces, for an OTLP endpoint named
  by the standard `OTEL_*` variables (T-662).

  Nothing here is instrumentation: every span comes from a telemetry event
  the code already emits. `Smolquery.Telemetry` turns those events into
  counters and histograms; this module turns a subset into spans, and only
  when `OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._TRACES_ENDPOINT`) is set and
  `OTEL_TRACES_EXPORTER` is not `none`, which `config/runtime.exs` reads
  into `enabled: true`. Off, no handler is attached and nothing costs
  anything.

  ## What becomes a span

    * **A request** on the API, the ClickHouse edge or the VictoriaMetrics
      edge: `Plug.Telemetry`'s `:start` opens a server span in the request
      process and `:stop` closes it, named `METHOD route` once the router has
      matched (`GET /v1/datasets/:ds/tables/:t`), with
      `http.request.method`, `url.path` and `http.response.status_code`. An
      incoming W3C `traceparent` header continues the caller's trace. A
      request that raises never reaches `:stop`, so Bandit's
      `[:bandit, :request, :exception]` closes the span as an error instead;
      without that the span would sit in the SDK's table for good.
    * **A query job**: the `[:smolquery, :query, :job]` event a job settles
      with carries the job, its phase spans, their monotonic origin and the
      span that submitted it. The job becomes a span named `query job`,
      child of that request, with `db.query.text` and the job's id, state,
      row count and snapshot; each phase (`parse`, `prune`, `engine_start`,
      `execute`, ...) becomes a child at its own start and duration, so the
      waterfall HyperDX draws is the one `trace: true` answers over the API.
      The job starts at `submitted_at`, so the wait before its first phase
      shows as the gap it is.

  ## Clocks

  The SDK stamps a span with `erlang:monotonic_time/0` and adds the time
  offset on export. A job's `submitted_at` and `finished_at` are system
  time in milliseconds and its phases are monotonic microseconds, so both
  are brought to monotonic native units before they are handed over; a
  system time given as a monotonic one would land decades away from the
  request it belongs to.

  ## Which jobs are collected

  A job's phases are collected into its trace when the caller asked for
  them (`trace: true`) or when tracing is on and the span that submitted
  the job is recording, so a request the sampler dropped costs its jobs no
  collector. A job with no parent is collected and the sampler decides at
  the end.

  ## What stays out

  Buffer flushes, seals and scatter shards keep their events and metrics
  and get no spans yet. A shard runs on another node with no context
  propagated, so a scattered query is one span on the coordinator.
  """

  use GenServer

  @handler_id "smolquery-tracing"
  @tracer_name :smolquery
  @query_text_limit 2_048
  @context_key :"$__current_otel_ctx"

  @edges [:api, :clickhouse, :victoriametrics]
  @requests for edge <- @edges, event <- [:start, :stop], do: [:smolquery, edge, event]
  @exception [:bandit, :request, :exception]
  @job [:smolquery, :query, :job]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc """
  Whether spans are made at all: `true` once an OTLP endpoint is named and
  the traces exporter is not `none`.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    :smolquery |> Application.get_env(__MODULE__, []) |> Keyword.get(:enabled, false)
  end

  @doc """
  The span the calling process is in, to parent a job to the request that
  submitted it, or `nil` outside any span. A `Task` does not inherit the
  OpenTelemetry context, so a caller with none looks up the `$callers`
  chain `Task` leaves behind: a job submitted from a request's task is
  still the request's child.
  """
  @spec current_span() :: OpenTelemetry.span_ctx() | nil
  def current_span do
    case :otel_tracer.current_span_ctx() do
      :undefined -> Enum.find_value(Process.get(:"$callers", []), &caller_span/1)
      span_ctx -> span_ctx
    end
  end

  @doc """
  Whether a job under `parent` is worth collecting: `true` for no parent,
  and for a parent that is recording.
  """
  @spec sampled?(OpenTelemetry.span_ctx() | nil) :: boolean()
  def sampled?(nil), do: true
  def sampled?(span_ctx), do: :otel_span.is_recording(span_ctx)

  defp caller_span(pid) do
    with {:dictionary, dictionary} <- Process.info(pid, :dictionary),
         %{} = ctx <- Keyword.get(dictionary, @context_key),
         span_ctx when span_ctx != :undefined <- :otel_tracer.current_span_ctx(ctx) do
      span_ctx
    else
      _no_span -> nil
    end
  end

  @impl true
  def init(nil) do
    Process.flag(:trap_exit, true)
    :telemetry.detach(@handler_id)

    if enabled?() do
      events = [@job, @exception | @requests]
      :ok = :telemetry.attach_many(@handler_id, events, &__MODULE__.handle_event/4, nil)
    end

    {:ok, nil}
  end

  @impl true
  def terminate(_reason, _state), do: :telemetry.detach(@handler_id)

  @doc false
  @spec handle_event([atom()], map(), map(), nil) :: :ok
  def handle_event([:smolquery, edge, :start], _measurements, %{conn: conn}, nil) do
    ctx = :otel_propagator_text_map.extract_to(:otel_ctx.new(), conn.req_headers)

    span_ctx =
      :otel_tracer.start_span(ctx, tracer(), "#{conn.method} #{edge}", %{
        kind: :server,
        attributes: %{
          "http.request.method" => conn.method,
          "url.path" => conn.request_path,
          "server.port" => conn.port,
          "smolquery.edge" => Atom.to_string(edge)
        }
      })

    token = :otel_ctx.attach(:otel_tracer.set_current_span(ctx, span_ctx))
    Process.put({__MODULE__, edge}, {token, span_ctx})

    :ok
  end

  def handle_event([:smolquery, edge, :stop], _measurements, %{conn: conn}, nil) do
    case Process.delete({__MODULE__, edge}) do
      {token, span_ctx} ->
        :otel_span.update_name(span_ctx, request_name(conn))
        :otel_span.set_attributes(span_ctx, %{"http.response.status_code" => conn.status})

        if conn.status >= 500,
          do: :otel_span.set_status(span_ctx, :opentelemetry.status(:error, ""))

        :otel_span.end_span(span_ctx)
        :otel_ctx.detach(token)

      nil ->
        :ok
    end

    :ok
  end

  def handle_event(@exception, _measurements, meta, nil) do
    for edge <- @edges, {token, span_ctx} <- List.wrap(Process.delete({__MODULE__, edge})) do
      :otel_span.set_status(span_ctx, :opentelemetry.status(:error, exception_text(meta)))
      :otel_span.end_span(span_ctx)
      :otel_ctx.detach(token)
    end

    :ok
  end

  def handle_event(@job, _measurements, %{job: job, spans: spans} = meta, nil) do
    ctx = parent_ctx(Map.get(meta, :parent))
    origin = Map.get(meta, :origin_us)
    start = at(job.submitted_at)

    job_span =
      :otel_tracer.start_span(ctx, tracer(), "query job", %{
        start_time: start,
        kind: :internal,
        attributes: job_attributes(job)
      })

    job_ctx = :otel_tracer.set_current_span(ctx, job_span)
    Enum.each(spans, &phase_span(job_ctx, origin, &1))

    if job.state in [:error, :cancelled],
      do: :otel_span.set_status(job_span, :opentelemetry.status(:error, inspect(job.error)))

    :otel_span.end_span(job_span, job_end(job, origin, spans))

    :ok
  end

  def handle_event(_event, _measurements, _meta, nil), do: :ok

  defp phase_span(job_ctx, origin, %{name: name, start_us: offset, duration_us: duration} = phase) do
    span_ctx =
      :otel_tracer.start_span(job_ctx, tracer(), Atom.to_string(name), %{
        start_time: us(origin + offset),
        kind: :internal,
        attributes: attributes("smolquery.", Map.get(phase, :meta, %{}))
      })

    :otel_span.end_span(span_ctx, us(origin + offset + duration))
  end

  defp job_end(job, _origin, []), do: at(job.finished_at || System.system_time(:millisecond))

  defp job_end(job, origin, spans) do
    last_phase = Enum.reduce(spans, 0, &max(&1.start_us + &1.duration_us, &2))

    max(job_end(job, origin, []), us(origin + last_phase))
  end

  defp parent_ctx(nil), do: :otel_ctx.new()
  defp parent_ctx(span_ctx), do: :otel_tracer.set_current_span(:otel_ctx.new(), span_ctx)

  defp request_name(%{method: method} = conn) do
    case conn.private do
      %{plug_route: {route, _plug}} -> "#{method} #{route}"
      _unrouted -> method
    end
  end

  defp exception_text(%{kind: kind, exception: exception}) when is_exception(exception),
    do: "#{kind}: #{Exception.message(exception)}"

  defp exception_text(%{kind: kind, reason: reason}), do: "#{kind}: #{inspect(reason)}"
  defp exception_text(_meta), do: "request raised"

  defp job_attributes(job) do
    %{
      "smolquery.job.id" => job.id,
      "smolquery.job.state" => Atom.to_string(job.state),
      "db.query.text" => String.slice(job.sql, 0, @query_text_limit),
      "smolquery.job.row_count" => job.row_count,
      "smolquery.job.snapshot" => job.snapshot,
      "smolquery.job.scatter_shards" => job.scatter && job.scatter.shards,
      "smolquery.job.file_cache" => job.file_cache
    }
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new(fn {key, value} -> {key, attribute(value)} end)
  end

  defp attributes(prefix, meta) when is_map(meta),
    do: Map.new(meta, fn {key, value} -> {prefix <> to_string(key), attribute(value)} end)

  defp attribute(value) when is_binary(value) or is_number(value) or is_boolean(value), do: value
  defp attribute(value) when is_atom(value), do: Atom.to_string(value)
  defp attribute(value), do: inspect(value, limit: 50, printable_limit: 256)

  defp at(system_ms),
    do: System.convert_time_unit(system_ms, :millisecond, :native) - :erlang.time_offset()

  defp us(monotonic_us), do: System.convert_time_unit(monotonic_us, :microsecond, :native)

  defp tracer, do: :opentelemetry.get_tracer(@tracer_name)
end
