defmodule Smolquery.PeerSocketTest do
  use ExUnit.Case, async: false

  alias Smolquery.PeerSocket

  @linux match?({:unix, :linux}, :os.type())

  defp read_back(options) do
    for {:raw, 6, option, <<value::32-native>>} <- options, into: %{}, do: {option, value}
  end

  defp decoded(options) do
    for {:raw, {6, option, <<value::32-native>>}} <- options, into: %{}, do: {option, value}
  end

  test "on Linux sets keepalive timers and TCP_USER_TIMEOUT" do
    options = PeerSocket.tcp_options(PeerSocket.config(), {:unix, :linux})

    assert options[:keepalive] == true
    assert decoded(options) == %{4 => 5, 5 => 5, 18 => 15_000}
  end

  test "elsewhere sets keepalive only" do
    assert PeerSocket.tcp_options(PeerSocket.config(), {:unix, :darwin}) == [keepalive: true]
  end

  test "config overrides each default" do
    previous = Application.fetch_env(:smolquery, PeerSocket)
    Application.put_env(:smolquery, PeerSocket, user_timeout_ms: 7_000, keepalive_idle_s: 3)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:smolquery, PeerSocket, value)
        :error -> Application.delete_env(:smolquery, PeerSocket)
      end
    end)

    assert PeerSocket.config()[:keepalive_interval_s] == 5
    assert decoded(PeerSocket.tcp_options(PeerSocket.config(), {:unix, :linux}))[18] == 7_000
    assert decoded(PeerSocket.tcp_options(PeerSocket.config(), {:unix, :linux}))[4] == 3
  end

  @tag skip: not @linux and "Linux socket options"
  test "a socket opened with tcp_options/0 carries them" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary] ++ PeerSocket.tcp_options())

    {:ok, options} =
      :inet.getopts(socket, [:keepalive] ++ for(o <- [4, 5, 18], do: {:raw, 6, o, 4}))

    assert {:keepalive, true} in options
    assert read_back(options) == %{4 => 5, 5 => 5, 18 => 15_000}
  end
end
