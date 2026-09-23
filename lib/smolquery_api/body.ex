defmodule SmolqueryApi.Body do
  @moduledoc """
  Reads a request body whole, up to a byte limit: how the ingest routes take
  their bytes, the NDJSON insert and the ClickHouse insert (T-476) alike.

  The limit is checked as each chunk arrives, so a body over it is refused
  without holding more than one chunk past the limit.

  A refusal carries the conn as far as it was read. The answer must be sent
  on that conn: its adapter knows how much of the body is still on the
  socket, so the server can drain or close it. Answering on the conn from
  before the read makes Bandit read the body again on a keep-alive socket,
  where it blocks for its read timeout or takes the next request's bytes as
  this one's body.
  """

  @doc """
  The whole body, or `{:error, :too_large, conn}` once it passes `max_bytes`.
  """
  @spec read(Plug.Conn.t(), pos_integer()) ::
          {:ok, binary(), Plug.Conn.t()} | {:error, :too_large, Plug.Conn.t()}
  def read(conn, max_bytes), do: read(conn, max_bytes, [])

  defp read(conn, max_bytes, acc) do
    case Plug.Conn.read_body(conn, length: max_bytes) do
      {:ok, chunk, conn} ->
        body = IO.iodata_to_binary(Enum.reverse([chunk | acc]))

        if byte_size(body) > max_bytes,
          do: {:error, :too_large, conn},
          else: {:ok, body, conn}

      {:more, chunk, conn} ->
        case Enum.reduce(acc, byte_size(chunk), &(byte_size(&1) + &2)) do
          over when over > max_bytes -> {:error, :too_large, conn}
          _within -> read(conn, max_bytes, [chunk | acc])
        end
    end
  end
end
