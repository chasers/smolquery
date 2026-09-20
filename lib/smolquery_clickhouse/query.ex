defmodule SmolqueryClickHouse.Query do
  @moduledoc """
  A query a ClickHouse client sends over HTTP, run through the node's query
  service (T-478).

      GET  /?query=SELECT count() FROM logs.events
      POST /   SELECT id, msg FROM logs.events LIMIT 10

  The statement is smolquery's SQL, run as the API and the Postgres wire run
  it: the same planner, the same two tiers, the same result cap. It is not
  translated from ClickHouse's dialect, so a ClickHouse function smolquery's
  engine lacks fails as an unknown function.

  ## Format

  The answer's format is the statement's trailing `FORMAT name`, else the
  `X-ClickHouse-Format` header, else the `default_format` parameter, else
  `TabSeparated` (`SmolqueryClickHouse.Format`). A result carries
  `X-ClickHouse-Format`, which `ch` reads to decide how to decode it; a
  statement with no rows to answer, such as an `ALTER TABLE`, answers an
  empty body without it.

  ## What a client asks on connect

  `version()` answers `#{"24.8.1.1"}` and `timezone()` answers `UTC`, each
  rewritten to a literal before the statement runs, in the statement's code
  only: the same text inside a string literal, a quoted identifier or a
  comment, or after a `.`, is left as written. So the
  `select 1, version()` that `ch` runs on every new connection answers as
  ClickHouse's does. The version is the ClickHouse release whose HTTP
  behavior this edge follows, not smolquery's.

  ## Settings

  `max_execution_time`, in seconds, bounds the query; without it the query
  service's default applies. A value past what a timer takes, which is how a
  client says "no limit", is held to the longest one, about 49 days. Every
  other setting is accepted and ignored. A setting arrives as a URL
  parameter or in the statement's `SETTINGS` clause, before or after
  `FORMAT`; the clause wins (`Statement.split_settings/1`).

  ## Parameters and quoting

  Backquoted identifiers and ClickHouse's backslash escapes are written the
  way the engine reads them (`Statement.standard_quoting/1`), so a statement
  a ClickHouse client quoted for ClickHouse parses here. `{name:Type}`
  placeholders are then filled from the request's `param_<name>` values
  (`SmolqueryClickHouse.Params`), in that order: a literal written from a
  parameter is never read under ClickHouse's escape rules, or a value
  ending in a backslash would reopen it.

  ## Refusals

  A request with `GET` is read-only, as ClickHouse's is: a statement that is
  not a `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN` or `EXISTS` is code
  164 `READONLY`. The engine's parser errors are 62 `SYNTAX_ERROR`, an
  unknown table is 60 `UNKNOWN_TABLE`, an unknown column is 47
  `UNKNOWN_IDENTIFIER`, a query past its time is 159 `TIMEOUT_EXCEEDED`, and
  a node at its job limit is 202 `TOO_MANY_SIMULTANEOUS_QUERIES` with
  `retry-after`.
  """

  import Plug.Conn

  alias Explorer.DataFrame
  alias Smolquery.Ddl
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias Smolquery.Sql
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Format
  alias SmolqueryClickHouse.Params
  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.Statement

  @version "24.8.1.1"

  @rewrites [
    {~r/(?<![\w.])version\s*\(\s*\)/i, "'#{@version}'"},
    {~r/(?<![\w.])timezone\s*\(\s*\)/i, "'UTC'"}
  ]

  @max_timeout_ms 4_294_967_295

  @read_only_keywords ~w(select with show describe desc explain exists)

  @doc """
  Runs `sql` and answers with its result.

  With `read_only: true`, as a `GET` is, a statement that could change
  anything is refused before it runs.
  """
  @spec call(Plug.Conn.t(), Runtime.t(), String.t(), keyword()) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime, sql, opts \\ []) do
    {statement, clause, settings} = clauses(sql)

    with :ok <- present(statement),
         :ok <- writable(statement, Keyword.get(opts, :read_only, false)),
         {:ok, format} <- format(clause, conn),
         {:ok, timeout} <- timeout(Map.merge(conn.query_params, settings)),
         {:ok, statement} <-
           statement |> Statement.standard_quoting() |> Params.substitute(conn.query_params) do
      run(conn, runtime, rewrite(statement), format, timeout)
    else
      {:error, exception} -> Errors.send_exception(conn, exception)
    end
  end

  defp clauses(sql) do
    {statement, format} = Statement.split_format(sql)
    {statement, settings} = Statement.split_settings(statement)

    case format do
      nil ->
        {statement, format} = Statement.split_format(statement)
        {statement, format, settings}

      format ->
        {statement, format, settings}
    end
  end

  defp present(""), do: {:error, {400, 62, "SYNTAX_ERROR", "Empty query", nil}}
  defp present(_statement), do: :ok

  defp writable(_statement, false), do: :ok

  defp writable(statement, true) do
    keyword =
      statement
      |> String.split(~r/[\s(]+/, parts: 2, trim: true)
      |> List.first("")
      |> String.downcase()

    if keyword in @read_only_keywords,
      do: :ok,
      else:
        {:error,
         {400, 164, "READONLY",
          "Cannot execute query in readonly mode. For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
          nil}}
  end

  defp format(clause, conn) do
    name =
      clause ||
        List.first(get_req_header(conn, "x-clickhouse-format")) ||
        conn.query_params["default_format"] ||
        "TabSeparated"

    case Format.fetch(name) do
      {:ok, format} ->
        {:ok, format}

      :error ->
        {:error,
         {404, 73, "UNKNOWN_FORMAT",
          "Unknown output format #{name}; use TabSeparated, TabSeparatedWithNames, TabSeparatedWithNamesAndTypes, JSON, JSONCompact, JSONEachRow or RowBinaryWithNamesAndTypes",
          nil}}
    end
  end

  defp timeout(%{"max_execution_time" => seconds}) do
    case Float.parse(seconds) do
      {value, ""} when value > 0 -> {:ok, [timeout_ms: bounded_ms(value)]}
      {value, ""} when value >= 0 -> {:ok, []}
      _invalid -> {:error, {400, 36, "BAD_ARGUMENTS", "max_execution_time is not a number", nil}}
    end
  end

  defp timeout(_params), do: {:ok, []}

  defp bounded_ms(seconds) when seconds >= @max_timeout_ms / 1000, do: @max_timeout_ms
  defp bounded_ms(seconds), do: max(round(seconds * 1000), 1)

  defp rewrite(statement) do
    Sql.map_code(statement, fn code ->
      Enum.reduce(@rewrites, code, fn {pattern, literal}, sql ->
        Regex.replace(pattern, sql, literal)
      end)
    end)
  end

  defp run(conn, runtime, statement, format, opts) do
    case Client.query(runtime.query_name, statement, opts) do
      {:ok, %Job{state: :done} = job, %DataFrame{} = frame} ->
        rows(conn, job, frame, format)

      {:ok, %Job{state: :done} = job, nil} ->
        conn
        |> headers(job, 0)
        |> put_resp_content_type("text/plain")
        |> send_resp(200, "")

      {:ok, %Job{state: :cancelled}, _frame} ->
        Errors.send_exception(conn, {500, 394, "QUERY_WAS_CANCELLED", "Query was cancelled", nil})

      {:ok, %Job{error: error}, _frame} ->
        Errors.send_exception(conn, describe(error))

      {:error, reason} ->
        Errors.send_exception(conn, refusal(reason))
    end
  end

  defp rows(conn, job, frame, format) do
    dtypes = DataFrame.dtypes(frame)

    columns =
      Enum.map(DataFrame.names(frame), &{&1, Map.fetch!(dtypes, &1), &1 in job.json_columns})

    rows = Frame.to_rows(frame, json_columns: job.json_columns)

    conn
    |> headers(job, length(rows))
    |> put_resp_header("x-clickhouse-format", Format.name(format))
    |> put_resp_header("content-type", Format.content_type(format))
    |> send_resp(200, Format.encode(format, columns, rows, job.duration_ms || 0))
  end

  defp headers(conn, job, result_rows) do
    {read_rows, read_bytes} = scanned(job.statistics)

    summary =
      JSON.encode!(%{
        "read_rows" => Integer.to_string(read_rows),
        "read_bytes" => Integer.to_string(read_bytes),
        "written_rows" => "0",
        "written_bytes" => "0",
        "total_rows_to_read" => Integer.to_string(read_rows),
        "result_rows" => Integer.to_string(result_rows),
        "result_bytes" => "0",
        "elapsed_ns" => Integer.to_string((job.duration_ms || 0) * 1_000_000)
      })

    conn
    |> put_resp_header("x-clickhouse-query-id", job.id)
    |> put_resp_header("x-clickhouse-timezone", "UTC")
    |> put_resp_header("x-clickhouse-summary", summary)
  end

  defp scanned(%{hot: hot, sealed: sealed}),
    do: {hot.rows_scanned + sealed.rows_scanned, hot.bytes_scanned + sealed.bytes_scanned}

  defp scanned(_none), do: {0, 0}

  defp describe({:invalid_query, message}) when is_binary(message) do
    cond do
      Regex.match?(~r/Parser Error|syntax error/i, message) ->
        {400, 62, "SYNTAX_ERROR", message, nil}

      Regex.match?(~r/Catalog Error: Table|Table with name .* does not exist/i, message) ->
        {404, 60, "UNKNOWN_TABLE", message, nil}

      String.contains?(message, "Binder Error") and String.contains?(message, "column") ->
        {400, 47, "UNKNOWN_IDENTIFIER", message, nil}

      true ->
        {400, 1002, "UNKNOWN_EXCEPTION", message, nil}
    end
  end

  defp describe({:unknown_table, {dataset, table}}),
    do: {404, 60, "UNKNOWN_TABLE", "Table #{dataset}.#{table} does not exist", nil}

  defp describe({:unknown_table, name}),
    do: {404, 60, "UNKNOWN_TABLE", "Table #{name} does not exist", nil}

  defp describe({:unknown_dataset, dataset}),
    do: {404, 81, "UNKNOWN_DATABASE", "Database #{dataset} does not exist", nil}

  defp describe({:result_too_large, max}),
    do:
      {400, 396, "TOO_MANY_ROWS_OR_BYTES",
       "Result exceeded result_max_rows (#{max}); add a LIMIT or aggregate the query", nil}

  defp describe({:hot_tier_unavailable, _reason}), do: hot_tier_unavailable()
  defp describe({:hot_tier_unavailable, _ref, _reason}), do: hot_tier_unavailable()

  defp describe(error) do
    if Ddl.error?(error),
      do: {400, 1002, "UNKNOWN_EXCEPTION", Ddl.message(error), nil},
      else: {500, 1002, "UNKNOWN_EXCEPTION", "query failed: #{inspect(error)}", nil}
  end

  defp hot_tier_unavailable,
    do:
      {503, 1002, "UNKNOWN_EXCEPTION",
       "a buffer node holding unsealed rows for this query could not be reached; retry", 1}

  defp refusal(:timeout),
    do: {500, 159, "TIMEOUT_EXCEEDED", "Timeout exceeded: the query was cancelled", nil}

  defp refusal(:too_many_jobs),
    do: {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", "too many queries in flight, retry later", 1}

  defp refusal(:query_service_unavailable),
    do: {503, 1002, "UNKNOWN_EXCEPTION", "the query service is not available here", 5}

  defp refusal(reason),
    do: {500, 1002, "UNKNOWN_EXCEPTION", "query failed: #{inspect(reason)}", nil}
end
