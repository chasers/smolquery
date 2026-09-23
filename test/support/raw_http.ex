defmodule Smolquery.Test.RawHttp do
  @moduledoc """
  HTTP/1.1 over one raw TCP socket, for tests that must know two requests
  share a connection: `Req` may open another for the second, so it cannot
  show what keep-alive does after a refused body.
  """

  @timeout 2_000

  @doc """
  Connects to a listener on loopback.
  """
  @spec connect(:inet.port_number()) :: :gen_tcp.socket()
  def connect(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    socket
  end

  @doc """
  Sends one request, `headers` as `{name, value}`, with a `content-length`
  for `body`, and returns the answer's status and body. Fails the test when
  no answer arrives within two seconds.
  """
  @spec request(:gen_tcp.socket(), String.t(), String.t(), [{String.t(), String.t()}], iodata()) ::
          {pos_integer(), binary()}
  def request(socket, method, path, headers, body \\ "") do
    head =
      Enum.map(
        [{"host", "localhost"}, {"content-length", Integer.to_string(IO.iodata_length(body))}] ++
          headers,
        fn {name, value} -> [name, ": ", value, "\r\n"] end
      )

    :ok = :gen_tcp.send(socket, [method, " ", path, " HTTP/1.1\r\n", head, "\r\n", body])
    response(socket, "")
  end

  defp response(socket, acc) do
    case :binary.split(acc, "\r\n\r\n") do
      [head, rest] ->
        "HTTP/1.1 " <> <<status::binary-size(3), _reason::binary>> = head
        [_line, length] = Regex.run(~r/content-length: (\d+)/i, head)
        {String.to_integer(status), body(socket, rest, String.to_integer(length))}

      [_partial] ->
        response(socket, acc <> recv(socket))
    end
  end

  defp body(_socket, rest, length) when byte_size(rest) >= length, do: rest
  defp body(socket, rest, length), do: body(socket, rest <> recv(socket), length)

  defp recv(socket) do
    case :gen_tcp.recv(socket, 0, @timeout) do
      {:ok, data} ->
        data

      {:error, reason} ->
        raise ExUnit.AssertionError, "no answer on the socket: #{inspect(reason)}"
    end
  end
end
