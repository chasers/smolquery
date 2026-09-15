defmodule SmolqueryApi.Body do
  @moduledoc """
  Reads a request body whole, up to a byte limit: how the ingest routes take
  their bytes, the NDJSON insert and the ClickHouse insert (T-476) alike.

  The limit is checked as each chunk arrives, so a body over it is refused
  without holding more than one chunk past the limit.
  """

  @doc """
  The whole body, or `{:error, :too_large}` once it passes `max_bytes`.
  """
  @spec read(Plug.Conn.t(), pos_integer()) ::
          {:ok, binary(), Plug.Conn.t()} | {:error, :too_large}
  def read(conn, max_bytes), do: read(conn, max_bytes, [])

  defp read(conn, max_bytes, acc) do
    case Plug.Conn.read_body(conn, length: max_bytes) do
      {:ok, chunk, conn} ->
        body = IO.iodata_to_binary(Enum.reverse([chunk | acc]))

        if byte_size(body) > max_bytes,
          do: {:error, :too_large},
          else: {:ok, body, conn}

      {:more, chunk, conn} ->
        case Enum.reduce(acc, byte_size(chunk), &(byte_size(&1) + &2)) do
          over when over > max_bytes -> {:error, :too_large}
          _within -> read(conn, max_bytes, [chunk | acc])
        end
    end
  end
end
