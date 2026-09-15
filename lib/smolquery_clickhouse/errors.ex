defmodule SmolqueryClickHouse.Errors do
  @moduledoc """
  Answers a failure the way ClickHouse's HTTP interface does (T-476).

  A `text/plain` line `Code: N. DB::Exception: message. (NAME)` with
  `X-ClickHouse-Exception-Code`, and `retry-after` when resending later can
  succeed. A ClickHouse client reads the code from the header and the
  message from the body, so every refusal on this edge, from authentication
  to a refused row, speaks this one form.
  """

  import Plug.Conn

  @typedoc """
  HTTP status, ClickHouse error code, the code's name, the message, and the
  `retry-after` seconds or `nil`.
  """
  @type t ::
          {Plug.Conn.int_status(), non_neg_integer(), String.t(), String.t(), pos_integer() | nil}

  @doc """
  Sends `exception` as ClickHouse's text form.
  """
  @spec send_exception(Plug.Conn.t(), t()) :: Plug.Conn.t()
  def send_exception(conn, {status, code, name, message, retry_after}) do
    conn
    |> retry_after(retry_after)
    |> put_resp_header("x-clickhouse-exception-code", Integer.to_string(code))
    |> put_resp_content_type("text/plain")
    |> send_resp(status, "Code: #{code}. DB::Exception: #{message}. (#{name})\n")
  end

  defp retry_after(conn, nil), do: conn

  defp retry_after(conn, seconds),
    do: put_resp_header(conn, "retry-after", Integer.to_string(seconds))
end
