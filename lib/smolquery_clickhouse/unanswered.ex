defmodule SmolqueryClickHouse.Unanswered do
  @moduledoc """
  The statements this edge could not answer, written down (T-480, PL-65 D1).

  What a ClickHouse client sends is known only from what it has sent. The
  dialect work is ordered by a corpus, and the corpus grows from here: when
  a statement is refused for a reason that is the dialect's, not the
  client's data, the edge logs it with the client's `user-agent` and counts
  it in `smolquery_clickhouse_unanswered_total`, by code. A first run of a
  real HyperDX against smolquery leaves, in the log, the list of what to
  build next.

  ## Which refusals

  62 `SYNTAX_ERROR`, 46 `UNKNOWN_FUNCTION`, 47 `UNKNOWN_IDENTIFIER`, 60
  `UNKNOWN_TABLE`, 73 `UNKNOWN_FORMAT`, 456 `UNKNOWN_QUERY_PARAMETER`, and
  36 `BAD_ARGUMENTS`, which is how a parameter type with no literal here
  answers. A timeout, a full node, a missing password and a result past its
  cap say nothing about the dialect and are not logged.

  ## What is written

  The statement as the client sent it, before any rewrite, on one line and
  cut at `@max_bytes`. `Runtime.unanswered_log` chooses how:

  - `:redacted` (the default) replaces every string literal with `'?'`. A
    statement's literals are a user's search terms and values; its shape is
    what the corpus needs. Parameter values are in the URL and are never
    logged. The engine's error is cut to its first line, since the lines
    after it quote the statement.
  - `:verbatim` keeps them. A literal is sometimes the point — a type name
    in `JSONExtract(x, 'Map(String, String)')` — so an operator reproducing
    a client's failure can ask for it.
  - `:off` logs nothing; the counter still counts.
  """

  require Logger

  alias Smolquery.Sql
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Runtime

  @codes [36, 46, 47, 60, 62, 73, 456]
  @max_bytes 8_192

  @doc """
  Records `sql` as unanswered when `exception` is one of the dialect's.
  """
  @spec record(Plug.Conn.t(), Runtime.t(), String.t(), Errors.t()) :: :ok
  def record(conn, %Runtime{} = runtime, sql, {_status, code, name, message, _retry_after})
      when code in @codes do
    :telemetry.execute([:smolquery, :clickhouse, :unanswered], %{count: 1}, %{code: code})

    log(runtime.unanswered_log, conn, sql, code, name, message)
  end

  def record(_conn, _runtime, _sql, _exception), do: :ok

  defp log(:off, _conn, _sql, _code, _name, _message), do: :ok

  defp log(mode, conn, sql, code, name, message) do
    Logger.warning(fn ->
      "clickhouse edge could not answer: code=#{code} name=#{name} " <>
        "user_agent=#{inspect(user_agent(conn))} statement=#{inspect(line(sql, mode))} " <>
        "error=#{inspect(reason(message, mode))}"
    end)
  end

  @doc """
  `sql` as the log writes it under `mode`: on one line, cut at #{@max_bytes}
  bytes, and with its string literals replaced when `mode` is `:redacted`.
  """
  @spec line(String.t(), :redacted | :verbatim) :: String.t()
  def line(sql, :verbatim), do: sql |> one_line() |> cut()

  def line(sql, :redacted) do
    sql
    |> Sql.tokens(dialect: :clickhouse)
    |> Enum.map_join(fn
      {:string, _literal} -> "'?'"
      {_kind, text} -> text
    end)
    |> one_line()
    |> cut()
  end

  defp reason(message, :verbatim), do: one_line(message)

  defp reason(message, :redacted),
    do: message |> String.split("\n", parts: 2) |> hd() |> one_line()

  defp one_line(text), do: text |> String.replace(~r/\s+/, " ") |> String.trim()

  defp cut(text) when byte_size(text) <= @max_bytes, do: text

  defp cut(text),
    do: (text |> binary_part(0, @max_bytes) |> String.replace_invalid("")) <> " …"

  defp user_agent(conn) do
    case Plug.Conn.get_req_header(conn, "user-agent") do
      [agent | _rest] -> agent
      [] -> ""
    end
  end
end
