defmodule Smolquery.Test.RawHttpTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.RawHttp

  defmodule Echo do
    @moduledoc false
    @behaviour Plug

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, _opts) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Plug.Conn.send_resp(conn, 201, conn.method <> " " <> body)
    end
  end

  test "sends two requests on one connection and reads both answers" do
    server = start_supervised!({Bandit, plug: Echo, ip: :loopback, port: 0, startup_log: false})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    socket = RawHttp.connect(port)

    assert RawHttp.request(socket, "POST", "/", [], "hello") == {201, "POST hello"}
    assert RawHttp.request(socket, "GET", "/", []) == {201, "GET "}
  end

  test "fails when no answer arrives" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listen)
    socket = RawHttp.connect(port)

    assert_raise ExUnit.AssertionError, ~r/no answer/, fn ->
      RawHttp.request(socket, "GET", "/", [])
    end
  end
end
