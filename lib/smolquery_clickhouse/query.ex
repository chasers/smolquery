defmodule SmolqueryClickHouse.Query do
  @moduledoc """
  A query a ClickHouse client sends over HTTP, run through the node's query
  service (T-478).

      GET  /?query=SELECT count() FROM logs.events
      POST /   SELECT id, msg FROM logs.events LIMIT 10

  The statement runs as the API and the Postgres wire run theirs: the same
  planner, the same two tiers, the same result cap. ClickHouse's dialect is
  met part of the way. Its function names are macros a job defines when its
  statement names them (`Smolquery.QueryService.ClickHouseFunctions`), and the syntax the
  engine's parser refuses is rewritten first (`SmolqueryClickHouse.Rewrite`).
  What neither covers fails as the engine reports it;
  `docs/clickhouse-sql-gaps.md` lists what that is.

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
  other setting but `date_time_output_format` is accepted and ignored. A
  setting arrives as a URL
  parameter or in the statement's `SETTINGS` clause, before or after
  `FORMAT`; the clause wins (`Statement.split_settings/1`).

  ## EXPLAIN ESTIMATE

  `EXPLAIN ESTIMATE <statement>` answers what the statement would read,
  without running it (T-506): ClickHouse's five columns, `database`, `table`,
  `parts`, `rows` and `marks`, in one row. HyperDX sends it before each search
  to show the rows the search will scan, and reads `rows`. `EXPLAIN
  ESTIMATE` is taken off first, and the statement under it is then filled,
  quoted and rewritten exactly as it would be to run, so whatever runs can
  be estimated. It is planned at the same snapshot, and `rows` and `parts`
  are the rows and files the plan keeps
  (`Smolquery.QueryService.Statistics`). In the hot tier that is after the
  planner's pruning and its Top-N bound. In the sealed tier it is every
  segment at the snapshot: those are pruned inside the engine, from
  DuckLake's per-file stats, where the planner cannot see, so a search over
  fifteen minutes of a large sealed table estimates the whole table. It is
  an upper bound on what will be read, not what will match. When the plan
  has no sizes to give, `rows` and `parts` are `NULL`, not `0`: a search
  that reads nothing is a different claim. `database` and `table` are empty
  and `marks` is `0`: one row stands for the whole statement, and smolquery
  has no marks. A statement the catalog answers (`system.*`) estimates `0`.
  A statement that does not plan or bind answers its error, which is how
  HyperDX checks an expression a user typed.

  ## The catalog

  `system.*`, `DESCRIBE`, `SHOW` and `EXISTS` are answered by
  `SmolqueryClickHouse.SystemCatalog`, in the edge's own engine, before the
  query service is asked. Such an answer has no job behind it, so its
  `X-ClickHouse-Query-Id` is the request's `query_id`, or a random one.

  ## Names and timestamps

  An unaliased `count()` or `count(*)` answers as `count()`, ClickHouse's
  name for it, not the engine's `count_star()`: HyperDX reads its total by
  that name. With `date_time_output_format=iso` a timestamp answers as ISO
  8601 with a `Z` in the text formats. A `TIMESTAMP_NS` column answers all
  nine of its digits, read from the frame as integer nanoseconds: HyperDX
  finds a clicked row by the timestamp it was shown.

  ## Parameters and quoting

  Backquoted identifiers and ClickHouse's backslash escapes are written the
  way the engine reads them (`Statement.standard_quoting/1`), so a statement
  a ClickHouse client quoted for ClickHouse parses here. `{name:Type}`
  placeholders are then filled from the request's `param_<name>` values
  (`SmolqueryClickHouse.Params`), in that order: a literal written from a
  parameter is never read under ClickHouse's escape rules, or a value
  ending in a backslash would reopen it.

  ## What could not be answered

  A statement refused for a reason that is the dialect's — a syntax error,
  an unknown function, table, format or parameter — is logged as the client
  sent it, with its `user-agent`, and counted
  (`SmolqueryClickHouse.Unanswered`, T-480). That log is how the dialect work
  learns what clients send.

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
  alias Explorer.Series
  alias Smolquery.Ddl
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Statistics
  alias Smolquery.Sql
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Format
  alias SmolqueryClickHouse.Params
  alias SmolqueryClickHouse.Rewrite
  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.Statement
  alias SmolqueryClickHouse.SystemCatalog
  alias SmolqueryClickHouse.Unanswered

  @version "24.8.1.1"

  @rewrites [
    {~r/(?<![\w.])version\s*\(\s*\)/i, "'#{@version}'"},
    {~r/(?<![\w.])timezone\s*\(\s*\)/i, "'UTC'"}
  ]

  @max_timeout_ms 4_294_967_295

  @statement :smolquery_clickhouse_statement
  @settings :smolquery_clickhouse_settings

  @engine_count "count_star()"
  @count "count()"

  @read_only_keywords ~w(select with show describe desc explain exists)

  @doc """
  Runs `sql` and answers with its result.

  With `read_only: true`, as a `GET` is, a statement that could change
  anything is refused before it runs.
  """
  @spec call(Plug.Conn.t(), Runtime.t(), String.t(), keyword()) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime, sql, opts \\ []) do
    {statement, clause, settings} = clauses(sql)
    {mode, statement} = estimated(statement)

    conn =
      conn
      |> put_private(@statement, sql)
      |> put_private(@settings, Map.merge(conn.query_params, settings))

    with :ok <- present(statement),
         :ok <- writable(statement, Keyword.get(opts, :read_only, false)),
         {:ok, format} <- format(clause, conn),
         {:ok, timeout} <- timeout(conn.private[@settings]),
         {:ok, statement} <-
           statement |> Statement.standard_quoting() |> Params.substitute(conn.query_params) do
      answer(mode, conn, runtime, translate(statement), format, timeout)
    else
      {:error, exception} -> refuse(conn, runtime, exception)
    end
  end

  defp refuse(conn, runtime, exception) do
    Unanswered.record(conn, runtime, conn.private[@statement], exception)

    Errors.send_exception(conn, exception)
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
          "Unknown output format #{name}; use the TabSeparated, CSV, JSON, JSONCompact, JSONEachRow or JSONCompactEachRow families, or RowBinaryWithNamesAndTypes",
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

  defp translate(statement),
    do: statement |> Rewrite.call() |> server_constants()

  defp server_constants(statement) do
    Sql.map_code(statement, fn code ->
      Enum.reduce(@rewrites, code, fn {pattern, literal}, sql ->
        Regex.replace(pattern, sql, literal)
      end)
    end)
  end

  defp estimated(statement) do
    with {:word, "explain", rest} <- Sql.next_token(statement),
         {:word, "estimate", inner} <- Sql.next_token(rest) do
      {:estimate, Sql.skip_trivia(inner)}
    else
      _another_statement -> {:rows, statement}
    end
  end

  defp answer(:rows, conn, runtime, statement, format, opts) do
    case SystemCatalog.answer(runtime.name, statement, database(conn)) do
      {:ok, frame} -> rows(conn, catalog_job(conn), frame, format)
      :pass -> run(conn, runtime, statement, format, opts)
      {:error, exception} -> refuse(conn, runtime, exception)
    end
  end

  defp answer(:estimate, conn, runtime, statement, format, opts) do
    case SystemCatalog.answer(runtime.name, statement, database(conn)) do
      {:ok, _frame} -> rows(conn, catalog_job(conn), estimate_frame({0, 0}), format)
      :pass -> estimate(conn, runtime, statement, format, opts)
      {:error, exception} -> refuse(conn, runtime, exception)
    end
  end

  defp estimate(conn, runtime, statement, format, opts) do
    runtime.query_name
    |> Client.query(statement, [explain: :plan] ++ opts)
    |> settle(conn, runtime, fn job, _plan ->
      rows(conn, job, estimate_frame(planned(job.statistics)), format)
    end)
  end

  defp estimate_frame({files, rows}) do
    DataFrame.new(
      [database: [""], table: [""], parts: [files], rows: [rows], marks: [0]],
      dtypes: [parts: {:u, 64}, rows: {:u, 64}, marks: {:u, 64}]
    )
  end

  defp planned(%Statistics{} = statistics),
    do: {Statistics.files_scanned(statistics), Statistics.rows_scanned(statistics)}

  defp planned(_no_sizes), do: {nil, nil}

  defp database(conn) do
    conn.query_params["database"] ||
      List.first(get_req_header(conn, "x-clickhouse-database")) ||
      "default"
  end

  defp catalog_job(conn) do
    id =
      conn.query_params["query_id"] || Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    %{id: id, statistics: nil, duration_ms: 0, json_columns: []}
  end

  defp run(conn, runtime, statement, format, opts) do
    runtime.query_name
    |> Client.query(statement, opts)
    |> settle(conn, runtime, fn
      job, %DataFrame{} = frame ->
        rows(conn, job, frame, format)

      job, nil ->
        conn
        |> headers(job, 0)
        |> put_resp_content_type("text/plain")
        |> send_resp(200, "")
    end)
  end

  defp settle({:ok, %Job{state: :done} = job, result}, _conn, _runtime, done),
    do: done.(job, result)

  defp settle({:ok, %Job{state: :cancelled}, _result}, conn, _runtime, _done),
    do: Errors.send_exception(conn, {500, 394, "QUERY_WAS_CANCELLED", "Query was cancelled", nil})

  defp settle({:ok, %Job{error: error}, _result}, conn, runtime, _done),
    do: refuse(conn, runtime, describe(error))

  defp settle({:error, reason}, conn, runtime, _done), do: refuse(conn, runtime, refusal(reason))

  defp rows(conn, job, frame, format) do
    frame = clickhouse_names(frame)
    dtypes = DataFrame.dtypes(frame)

    columns =
      Enum.map(DataFrame.names(frame), &{&1, Map.fetch!(dtypes, &1), &1 in job.json_columns})

    rows =
      frame
      |> nanoseconds(dtypes)
      |> Frame.to_rows(json_columns: job.json_columns, map_entries: true)

    conn
    |> headers(job, length(rows))
    |> put_resp_header("x-clickhouse-format", Format.name(format))
    |> put_resp_header("content-type", Format.content_type(format))
    |> send_resp(200, Format.encode(format, columns, rows, encoding(conn, job)))
  end

  defp nanoseconds(frame, dtypes) do
    for {name, {:naive_datetime, :nanosecond}} <- dtypes, reduce: frame do
      frame -> DataFrame.put(frame, name, Series.cast(frame[name], {:s, 64}))
    end
  end

  defp clickhouse_names(frame) do
    names = DataFrame.names(frame)

    if @engine_count in names and @count not in names,
      do: DataFrame.rename(frame, %{@engine_count => @count}),
      else: frame
  end

  defp encoding(conn, job) do
    case conn.private[@settings]["date_time_output_format"] do
      "iso" -> [elapsed_ms: job.duration_ms || 0, date_time: :iso]
      _simple -> [elapsed_ms: job.duration_ms || 0]
    end
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

  defp describe({:invalid_query, message}) when is_binary(message),
    do: Errors.engine_failure(message)

  defp describe(error) when is_exception(error),
    do: error |> Exception.message() |> Errors.engine_failure()

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
