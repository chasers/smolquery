defmodule SmolqueryVictoriaMetrics.SupervisorTest do
  use ExUnit.Case, async: true

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

  test "refuses to boot without a password" do
    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryVictoriaMetrics.Supervisor.start_link(name: :vm_no_password, password: "")
    end
  end
end
