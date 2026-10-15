defmodule Smolquery.Tracing do
  @moduledoc """
  smolquery's telemetry as OpenTelemetry traces, for an OTLP endpoint named
  by the standard `OTEL_*` variables (T-662).

  Nothing here is instrumentation: every span comes from a telemetry event
  the code already emits. `Smolquery.Telemetry` turns those events into
  counters and histograms; this module turns a subset into spans, and only
  when `OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._TRACES_ENDPOINT`) is set, which
  `config/runtime.exs` reads into `enabled: true`. Off, the handlers are not
  attached and nothing costs anything.

  ## What becomes a span

    * **A request** on the API, the ClickHouse edge or the VictoriaMetrics
      edge: `Plug.Telemetry`'s `:start` opens a server span in the request
      process and `:stop` closes it, named `METHOD route` once the router has
      matched (`GET /v1/datasets/:ds/tables/:t`), with
      `http.request.method`, `url.path` and `http.response.status_code`. An
      incoming W3C `traceparent` header continues the caller's trace.
    * **A query job**: the `[:smolquery, :query, :job]` event a job settles
      with carries the job, its phase spans and the span that submitted it.
      The job becomes a span named `query job`, child of that request, with
      `db.query.text` and the job's id, state, row count and snapshot; each
      phase (`parse`, `prune`, `engine_start`, `execute`, ...) becomes a
      child at its own start and duration, so the waterfall HyperDX draws is
      the one `trace: true` answers over the API. Phases are stamped from
      the job's wall-clock `submitted_at`, and the job span ends at its
      last phase's end when that is later than the millisecond
      `finished_at`, so a child never leaves its parent.

  ## What stays out

  Buffer flushes, seals and scatter shards keep their events and metrics
  and get no spans yet. A shard runs on another node with no context
  propagated, so a scattered query is one span on the coordinator.
  """

  use GenServer

  @handler_id "smolquery-tracing"
  @tracer_name :smolquery
  @query_text_limit 2_048

  @requests for edge <- [:api, :clickhouse, :victoriametrics],
                event <- [:start, :stop],
                do: [:smolquery, edge, event]

  @job [:smolquery, :query, :job]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc """
  Whether spans are made at all: `true` once an OTLP endpoint is named.
  A runner reads it to decide whether to collect a job's phases for the
  job span when the caller did not ask for them.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    :smolquery |> Application.get_env(__MODULE__, []) |> Keyword.get(:enabled, false)
  end

  @doc """
  The span the calling process is in, to parent a job to the request that
  submitted it, or `nil` outside any span.
  """
  @spec current_span() :: OpenTelemetry.span_ctx() | nil
  def current_span do
    case :otel_tracer.current_span_ctx() do
      :undefined -> nil
      span_ctx -> span_ctx
    end
  end

  @impl true
  def init(nil) do
    :telemetry.detach(@handler_id)

    if enabled?() do
      :ok =
        :telemetry.attach_many(@handler_id, [@job | @requests], &__MODULE__.handle_event/4, nil)
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

  def handle_event(@job, _measurements, %{job: job, spans: spans} = meta, nil) do
    ctx = parent_ctx(Map.get(meta, :parent))
    start = at(job.submitted_at)

    job_span =
      :otel_tracer.start_span(ctx, tracer(), "query job", %{
        start_time: start,
        kind: :internal,
        attributes: job_attributes(job)
      })

    job_ctx = :otel_tracer.set_current_span(ctx, job_span)
    Enum.each(spans, &phase_span(job_ctx, start, &1))

    if job.state in [:error, :cancelled],
      do: :otel_span.set_status(job_span, :opentelemetry.status(:error, inspect(job.error)))

    :otel_span.end_span(job_span, job_end(job, start, spans))

    :ok
  end

  def handle_event(_event, _measurements, _meta, nil), do: :ok

  defp job_end(job, start, spans) do
    settled = at(job.finished_at || System.system_time(:millisecond))
    last_phase = Enum.reduce(spans, 0, &max(&1.start_us + &1.duration_us, &2))

    max(settled, start + us(last_phase))
  end

  defp phase_span(job_ctx, start, %{name: name, start_us: offset, duration_us: duration} = phase) do
    span_ctx =
      :otel_tracer.start_span(job_ctx, tracer(), Atom.to_string(name), %{
        start_time: start + us(offset),
        kind: :internal,
        attributes: attributes("smolquery.", Map.get(phase, :meta, %{}))
      })

    :otel_span.end_span(span_ctx, start + us(offset + duration))
  end

  defp parent_ctx(nil), do: :otel_ctx.new()
  defp parent_ctx(span_ctx), do: :otel_tracer.set_current_span(:otel_ctx.new(), span_ctx)

  defp request_name(%{method: method} = conn) do
    case conn.private do
      %{plug_route: {route, _plug}} -> "#{method} #{route}"
      _unrouted -> method
    end
  end

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

  defp at(milliseconds), do: System.convert_time_unit(milliseconds, :millisecond, :native)
  defp us(microseconds), do: System.convert_time_unit(microseconds, :microsecond, :native)

  defp tracer, do: :opentelemetry.get_tracer(@tracer_name)
end
