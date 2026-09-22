defmodule SmolqueryVictoriaMetrics.SupervisorTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryApi.Admission
  alias SmolqueryVictoriaMetrics.Runtime

  @password "supervisor-test-password"
  @fixture Path.expand("../support/fixtures/victoriametrics/write_snappy.bin", __DIR__)

  defp start_edge(opts \\ []) do
    name = :"vm_supervisor_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryVictoriaMetrics.Supervisor,
       Keyword.merge([name: name, password: @password, port: 0], opts)}
    )

    on_exit(fn -> Runtime.delete(name) end)

    {:ok, {_ip, port}} = SmolqueryVictoriaMetrics.Supervisor.bound(name)

    {name, "http://127.0.0.1:#{port}"}
  end

  defp write(base, headers) do
    Req.post!(base <> "/api/v1/write",
      body: File.read!(@fixture),
      headers:
        [{"content-type", "application/x-protobuf"}, {"content-encoding", "snappy"}] ++ headers,
      retry: false
    )
  end

  test "serves the edge over a real listener" do
    {_name, base} = start_edge()

    for path <- ["/health", "/-/healthy", "/-/ready"] do
      assert %{status: 200, body: "OK"} = Req.get!(base <> path, retry: false)
    end

    assert write(base, []).status == 401
    assert write(base, [{"authorization", "Bearer wrong"}]).status == 401

    response = write(base, [{"authorization", "Bearer " <> @password}])

    assert response.status == 503
    assert [_seconds] = Req.Response.get_header(response, "retry-after")
    assert %{"status" => "error", "errorType" => "unavailable"} = response.body
  end

  test "answers a query over a real listener" do
    {_name, base} = start_edge()

    response =
      Req.get!(base <> "/api/v1/query?query=42&time=1695000000",
        headers: [{"authorization", "Bearer " <> @password}],
        retry: false
      )

    assert response.status == 200

    assert %{
             "status" => "success",
             "data" => %{"resultType" => "scalar", "result" => [1_695_000_000, "42"]}
           } = response.body
  end

  test "takes basic auth with any user name" do
    {_name, base} = start_edge()

    response =
      write(base, [{"authorization", Plug.BasicAuth.encode_basic_auth("vmagent", @password)}])

    assert response.status == 503
  end

  test "starts the edge's own admission counter" do
    {name, _base} = start_edge(insert_max_in_flight_bytes: 1_000)

    assert Admission.in_flight(name) == 0
  end

  @tag :tmp_dir
  @tag :capture_log
  test "answers Grafana's connect-and-browse sequence over a real listener", context do
    stack = VictoriaMetricsStack.start(context)
    now_ms = System.system_time(:millisecond)

    :ok =
      VictoriaMetricsStack.write(stack, [
        {%{"__name__" => "up", "job" => "node"}, [{now_ms - 1_000, 1}]}
      ])

    {_name, base} =
      start_edge(query_name: stack.query, ingest_name: stack.runtime.ingest_name)

    auth = [{"authorization", Plug.BasicAuth.encode_basic_auth("grafana", @password)}]

    answers =
      for {method, path, form} <- [
            {:get, "/api/v1/status/buildinfo", nil},
            {:get, "/api/v1/query?query=1%2B1", nil},
            {:get, "/api/v1/labels", nil},
            {:get, "/api/v1/label/__name__/values", nil},
            {:get, "/api/v1/metadata", nil},
            {:post, "/api/v1/series", [{"match[]", "up"}]}
          ] do
        opts = [headers: auth, retry: false]
        opts = if form, do: Keyword.put(opts, :form, form), else: opts
        response = Req.request!([method: method, url: base <> path] ++ opts)

        assert response.status == 200, "#{method} #{path}: #{inspect(response.body)}"
        assert %{"status" => "success", "data" => data} = response.body
        {path, data}
      end

    assert %{
             "/api/v1/status/buildinfo" => %{"version" => "2.24.0"},
             "/api/v1/query?query=1%2B1" => %{"resultType" => "scalar", "result" => [_t, "2"]},
             "/api/v1/labels" => ["__name__", "job"],
             "/api/v1/label/__name__/values" => ["up"],
             "/api/v1/metadata" => %{},
             "/api/v1/series" => [%{"__name__" => "up", "job" => "node"}]
           } = Map.new(answers)
  end

  test "refuses to boot without a password" do
    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryVictoriaMetrics.Supervisor.start_link(name: :vm_no_password, password: "")
    end
  end
end
