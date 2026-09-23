defmodule SmolqueryClickHouse.SupervisorTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.RawHttp
  alias SmolqueryApi.Admission
  alias SmolqueryClickHouse.Runtime

  @password "supervisor-test-password"

  defp start_edge(opts \\ []) do
    name = :"ch_supervisor_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryClickHouse.Supervisor,
       Keyword.merge([name: name, password: @password, port: 0], opts)}
    )

    on_exit(fn -> Runtime.delete(name) end)

    {:ok, {_ip, port}} = SmolqueryClickHouse.Supervisor.bound(name)

    {name, "http://127.0.0.1:#{port}"}
  end

  test "serves the edge over a real listener" do
    {_name, base} = start_edge()

    assert %{status: 200, body: "Ok.\n"} = Req.get!(base <> "/ping", retry: false)
    assert Req.post!(base <> "/", params: [query: "SELECT 1"], retry: false).status == 401

    response =
      Req.get!(base <> "/",
        params: [query: "SELECT 1"],
        headers: [{"x-clickhouse-key", @password}],
        retry: false
      )

    assert response.status == 503
  end

  test "takes a request line past Bandit's 10,000-byte default, as ClickHouse does" do
    {_name, base} = start_edge()
    columns = Enum.map_join(1..2_000, ", ", &"column_#{&1}")

    response =
      Req.post!(base <> "/",
        params: [query: "INSERT INTO logs.wide (#{columns}) FORMAT RowBinary"],
        headers: [{"x-clickhouse-key", @password}],
        body: "",
        retry: false
      )

    assert response.status != 414
    assert [_code] = Req.Response.get_header(response, "x-clickhouse-exception-code")
  end

  test "answers an oversized insert and the next request on the same connection promptly" do
    {_name, base} = start_edge(max_ndjson_bytes: 100_000)
    socket = base |> URI.parse() |> Map.fetch!(:port) |> RawHttp.connect()
    path = "/?query=INSERT%20INTO%20logs.events%20FORMAT%20RowBinary"
    headers = [{"x-clickhouse-key", @password}]

    assert {413, _body} =
             RawHttp.request(socket, "POST", path, headers, :binary.copy(<<0>>, 300_000))

    assert {status, _body} =
             RawHttp.request(socket, "POST", path, headers, :binary.copy(<<0>>, 50_000))

    assert status >= 400
  end

  test "starts the edge's own admission counter" do
    {name, _base} = start_edge(insert_max_in_flight_bytes: 1_000)

    assert Admission.in_flight(name) == 0
  end

  test "refuses to boot without a password" do
    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryClickHouse.Supervisor.start_link(name: :ch_no_password, password: "")
    end
  end
end
