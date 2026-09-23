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

  alias Smolquery.QueryService.Client

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

  @doc """
  The refusal an engine's error message answers as.

  The engine reports a failure as text, at parse time and at run time
  alike, and both the query path and the emulated catalog read it here, so
  one statement answers one code wherever it ran: 62 for the parser's, 60
  for an unknown table, 46 for an unknown function, 47 for an unknown
  column. A failure that is the server's — the disk, memory, a connection
  the engine could not make — is a 500, which a client may retry; anything
  else is the statement's, a 400.
  """
  @spec engine_failure(String.t()) :: t()
  def engine_failure(message) when is_binary(message) do
    cond do
      Regex.match?(~r/Parser Error|syntax error/i, message) ->
        {400, 62, "SYNTAX_ERROR", message, nil}

      Regex.match?(~r/Catalog Error: Table|Table with name .* does not exist/i, message) ->
        {404, 60, "UNKNOWN_TABLE", message, nil}

      Regex.match?(~r/Function with name .* does not exist/i, message) ->
        {404, 46, "UNKNOWN_FUNCTION", message, nil}

      String.contains?(message, "Binder Error") and String.contains?(message, "column") ->
        {400, 47, "UNKNOWN_IDENTIFIER", message, nil}

      Client.server_failure_message?(message) ->
        {500, 1002, "UNKNOWN_EXCEPTION", message, nil}

      true ->
        {400, 1002, "UNKNOWN_EXCEPTION", message, nil}
    end
  end

  defp retry_after(conn, nil), do: conn

  defp retry_after(conn, seconds),
    do: put_resp_header(conn, "retry-after", Integer.to_string(seconds))
end
