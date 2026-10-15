defmodule Smolquery.TracingTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_private: 3, put_req_header: 3]
  import Plug.Test

  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Tracing

  require Record

  @span_fields Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  @trace_id 0x4BF92F3577B34DA6A3CE929D0E0E4736
  @parent_id 0x00F067AA0BA902B7
  @ten_seconds System.convert_time_unit(10, :second, :native)

  setup do
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    on_exit(fn -> :otel_simple_processor.set_exporter(:otel_exporter_none, []) end)
    :ok
  end

  defp start_request(headers \\ []) do
    conn = conn(:get, "/v1/datasets/analytics/tables/events")
    conn = Enum.reduce(headers, conn, fn {key, value}, acc -> put_req_header(acc, key, value) end)
    :telemetry.execute([:smolquery, :api, :start], %{}, %{conn: conn})
    conn
  end

  defp request(headers \\ []) do
    conn =
      headers
      |> start_request()
      |> put_private(:plug_route, {"/v1/datasets/:ds/tables/:t", &Function.identity/1})
      |> Map.put(:status, 200)

    :telemetry.execute([:smolquery, :api, :stop], %{duration: 1}, %{conn: conn})
  end

  defp in_request_span(fun) do
    tracer = :opentelemetry.get_tracer(:smolquery)
    request = :otel_tracer.start_span(:otel_ctx.new(), tracer, "POST /v1/queries", %{})
    token = :otel_ctx.attach(:otel_tracer.set_current_span(:otel_ctx.new(), request))

    try do
      fun.()
    after
      :otel_ctx.detach(token)
      :otel_span.end_span(request)
    end

    :otel_span.span_id(request)
  end

  test "enabled?/0 follows the application config" do
    assert Tracing.enabled?()
  end

  describe "a request" do
    test "is one server span named by its matched route, with the status it answered" do
      request()

      assert_receive {:span, span(name: name, kind: :server, attributes: attributes)}
      assert name == "GET /v1/datasets/:ds/tables/:t"

      assert %{
               "http.request.method" => "GET",
               "url.path" => "/v1/datasets/analytics/tables/events",
               "http.response.status_code" => 200,
               "smolquery.edge" => "api"
             } = :otel_attributes.map(attributes)
    end

    test "continues the trace a W3C traceparent header names" do
      request([{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}])

      assert_receive {:span, span(trace_id: @trace_id, parent_span_id: @parent_id)}
    end

    test "that raises is closed as an error by Bandit's exception event, and leaves the process clean" do
      start_request()

      :telemetry.execute(
        [:bandit, :request, :exception],
        %{},
        %{kind: :error, exception: %ArgumentError{message: "bad query"}, stacktrace: []}
      )

      assert_receive {:span, span(name: "GET api", status: status)}
      assert elem(status, 1) == :error
      assert Tracing.current_span() == nil
    end

    test "answers nothing for a stop with no start" do
      conn = conn(:get, "/") |> Map.put(:status, 404)
      :telemetry.execute([:smolquery, :api, :stop], %{duration: 1}, %{conn: conn})

      refute_receive {:span, _span}
      assert Tracing.current_span() == nil
    end
  end

  describe "a query job" do
    setup do
      name = :"tracing_query_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {QueryService.Supervisor,
         name: name, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
        id: name
      )

      on_exit(fn -> Runtime.delete(name) end)
      %{name: name}
    end

    test "is a span with its phases as children, under the request that submitted it, on the SDK's clock",
         %{name: name} do
      request_id =
        in_request_span(fn ->
          assert {:ok, %{state: :done}, _frame} = Client.query(name, "SELECT 1 AS n")
        end)

      assert_receive {:span,
                      span(name: "query job", parent_span_id: ^request_id, span_id: job_id) = job}

      assert %{"db.query.text" => "SELECT 1 AS n", "smolquery.job.state" => "done"} =
               :otel_attributes.map(span(job, :attributes))

      assert abs(span(job, :start_time) - System.monotonic_time()) < @ten_seconds

      assert_receive {:span, span(name: "execute", parent_span_id: ^job_id) = execute}
      assert span(execute, :start_time) >= span(job, :start_time)
      assert span(execute, :end_time) <= span(job, :end_time)
    end

    test "submitted from a Task inside a request is still the request's child", %{name: name} do
      request_id =
        in_request_span(fn ->
          task = Task.async(fn -> Client.query(name, "SELECT 2 AS n") end)
          assert {:ok, %{state: :done}, _frame} = Task.await(task)
        end)

      assert_receive {:span, span(name: "query job", parent_span_id: ^request_id)}
    end

    test "keeps the job's own trace opt-in over the API", %{name: name} do
      assert {:ok, %{trace: nil}, _frame} = Client.query(name, "SELECT 1")
      assert {:ok, %{trace: [_ | _]}, _frame} = Client.query(name, "SELECT 1", trace: true)
    end
  end
end
