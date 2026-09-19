defmodule SmolqueryClickHouse.Insert do
  @moduledoc """
  ClickHouse's HTTP insert, for a client that already speaks it (T-476), on
  the `:clickhouse` role's own listener (T-477).

      POST /?query=INSERT INTO logs.events (id, ts) FORMAT RowBinary
      <RowBinary rows>

  The statement is parsed by `SmolqueryClickHouse.Statement`. The table is
  `{database, table}`: the statement's qualifier, else the `database`
  parameter, else the `X-ClickHouse-Database` header, else `default`. The body
  is `RowBinary`, `RowBinaryWithNames` or `RowBinaryWithNamesAndTypes`,
  decoded on this node by `Smolquery.IngestService.Client.insert_rowbinary/5`
  and written as the NDJSON insert writes.

  A plain `RowBinary` body carries no types, so each column is read as the
  type its smolquery column implies (`Smolquery.RowBinary`). A producer whose
  ClickHouse types differ, a `UUID` or `UInt8` column among them, sends
  `RowBinaryWithNamesAndTypes`.

  An insert is all or nothing, as ClickHouse's is: when any row is refused,
  none is written. A nonzero `input_format_allow_errors_num` or
  `input_format_allow_errors_ratio` writes the rest instead, without counting
  against the number. `insert_deduplication_token` is the insert's
  idempotency key, `insertId` on the NDJSON route. Every other setting is
  accepted and ignored. Settings come from the URL and from the statement's
  `SETTINGS` clause, and the clause wins.

  `SmolqueryClickHouse.Router` has already checked the password and counted
  the body against ingest admission by the time this runs.

  A 200 carries an empty body and `X-ClickHouse-Summary`. A failure answers
  through `SmolqueryClickHouse.Errors`, and a retryable one carries
  `retry-after`.
  """

  import Plug.Conn

  alias Smolquery.BufferService.Backlog
  alias Smolquery.IngestService
  alias SmolqueryApi.Body
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.Statement

  @formats %{
    "rowbinary" => :row_binary,
    "rowbinarywithnames" => :with_names,
    "rowbinarywithnamesandtypes" => :with_names_and_types
  }

  @max_token_bytes 128
  @shown_refusals 5

  @doc """
  Runs the `INSERT` in the `query` parameter against the request body.
  """
  @spec call(Plug.Conn.t(), Runtime.t()) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime) do
    conn = fetch_query_params(conn)

    with {:ok, query} <- query(conn.query_params),
         {:ok, insert} <- parse(query),
         {:ok, format} <- format(insert.format),
         settings = Map.merge(conn.query_params, insert.settings),
         {:ok, opts} <- insert_opts(insert, settings, runtime),
         {:ok, body, read} <- Body.read(conn, runtime.max_ndjson_bytes),
         {:ok, result} <- insert(runtime, table_ref(conn, insert), body, format, opts) do
      summary(read, result, byte_size(body))
    else
      {:error, reason} -> Errors.send_exception(conn, describe(reason, runtime))
    end
  end

  defp query(%{"query" => query}) when is_binary(query) and query != "", do: {:ok, query}
  defp query(_params), do: {:error, {:syntax, "the query parameter holds no statement"}}

  defp parse(query) do
    case Statement.parse(query) do
      {:ok, insert} ->
        {:ok, insert}

      {:error, message} ->
        if Statement.insert?(query),
          do: {:error, {:syntax, message}},
          else: {:error, :not_insert}
    end
  end

  defp format(name) do
    case Map.fetch(@formats, String.downcase(name)) do
      {:ok, format} -> {:ok, format}
      :error -> {:error, {:unknown_format, name}}
    end
  end

  defp insert_opts(insert, settings, runtime) do
    with {:ok, batch_id} <- batch_id(settings["insert_deduplication_token"]) do
      {:ok,
       [
         columns: insert.columns,
         batch_id: batch_id,
         skip_invalid_rows: allows_errors?(settings),
         max_ndjson_bytes: runtime.max_ndjson_bytes
       ]}
    end
  end

  defp batch_id(nil), do: {:ok, nil}
  defp batch_id(""), do: {:ok, nil}

  defp batch_id(token) when byte_size(token) <= @max_token_bytes, do: {:ok, token}

  defp batch_id(_token),
    do: {:error, {:bad_argument, "insert_deduplication_token is over #{@max_token_bytes} bytes"}}

  defp allows_errors?(settings) do
    positive?(settings["input_format_allow_errors_num"]) or
      positive?(settings["input_format_allow_errors_ratio"])
  end

  defp positive?(value) when is_binary(value) do
    case Float.parse(value) do
      {number, ""} -> number > 0
      _not_a_number -> false
    end
  end

  defp positive?(_absent), do: false

  defp table_ref(conn, %{database: nil, table: table}) do
    database =
      conn.query_params["database"] ||
        List.first(get_req_header(conn, "x-clickhouse-database")) ||
        "default"

    {database, table}
  end

  defp table_ref(_conn, %{database: database, table: table}), do: {database, table}

  defp insert(runtime, table_ref, body, format, opts) do
    case IngestService.Client.insert_rowbinary(runtime.ingest_name, table_ref, body, format, opts) do
      {:ok, %{errors: [_ | _] = errors} = result} ->
        if opts[:skip_invalid_rows],
          do: {:ok, result},
          else: {:error, {:rows_refused, errors}}

      other ->
        other
    end
  end

  defp summary(conn, result, bytes) do
    written = Integer.to_string(result.inserted)
    bytes_text = Integer.to_string(bytes)

    summary =
      JSON.encode!(%{
        "read_rows" => written,
        "read_bytes" => bytes_text,
        "written_rows" => written,
        "written_bytes" => bytes_text,
        "total_rows_to_read" => "0",
        "result_rows" => written,
        "result_bytes" => bytes_text
      })

    conn
    |> put_resp_header("x-clickhouse-summary", summary)
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "")
  end

  defp describe({:syntax, message}, _runtime), do: {400, 62, "SYNTAX_ERROR", message, nil}

  defp describe(:not_insert, _runtime),
    do: {501, 48, "NOT_IMPLEMENTED", "this endpoint runs INSERT ... FORMAT statements only", nil}

  defp describe({:unknown_format, name}, _runtime),
    do:
      {404, 73, "UNKNOWN_FORMAT",
       "format #{name} is not taken here; use RowBinary, RowBinaryWithNames or RowBinaryWithNamesAndTypes",
       nil}

  defp describe({:bad_argument, message}, _runtime), do: {400, 36, "BAD_ARGUMENTS", message, nil}

  defp describe({:invalid_identifier, name}, _runtime),
    do: {400, 36, "BAD_ARGUMENTS", "invalid name: #{inspect(name)}", nil}

  defp describe(:too_large, runtime),
    do:
      {413, 36, "BAD_ARGUMENTS",
       "insert bodies are limited to #{runtime.max_ndjson_bytes} bytes; send smaller blocks", nil}

  defp describe({:decoded_too_large, bytes, limit}, _runtime),
    do:
      {413, 36, "BAD_ARGUMENTS",
       "the block's rows decode past the #{limit}-byte insert limit (stopped at #{bytes} bytes); send smaller blocks",
       nil}

  defp describe({:invalid_rowbinary, message}, _runtime) do
    if String.contains?(message, "ends mid-value"),
      do: {400, 33, "CANNOT_READ_ALL_DATA", message, nil},
      else: {400, 117, "INCORRECT_DATA", message, nil}
  end

  defp describe({:rows_refused, errors}, _runtime),
    do: {400, 117, "INCORRECT_DATA", refusal_message(errors), nil}

  defp describe({:unknown_table, {dataset, table}}, _runtime),
    do: {404, 60, "UNKNOWN_TABLE", "table #{dataset}.#{table} does not exist", nil}

  defp describe({:unknown_dataset, dataset}, _runtime),
    do: {404, 81, "UNKNOWN_DATABASE", "database #{dataset} does not exist", nil}

  defp describe(:buffer_full, _runtime),
    do: {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", "buffer full, retry later", 1}

  defp describe({:overloaded, predicted_ms}, _runtime),
    do:
      {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES",
       "write path overloaded, ~#{predicted_ms} ms behind; retry later",
       max(ceil(predicted_ms / 1000), 1)}

  defp describe({:backlog_full, refusal}, _runtime),
    do: {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", Backlog.message(refusal), 5}

  defp describe({:stale_schema, _ref, names}, _runtime),
    do:
      {503, 1002, "UNKNOWN_EXCEPTION",
       "the table's columns changed while inserting (#{Enum.join(names, ", ")}); retry", 1}

  defp describe(reason, _runtime)
       when reason in [:not_owner, :ownership_settling, :ring_config_stale, :draining],
       do: {503, 1002, "UNKNOWN_EXCEPTION", "table ownership is moving; retry", 1}

  defp describe(reason, _runtime)
       when reason in [:buffer_service_unavailable, :ingest_service_unavailable],
       do: {503, 1002, "UNKNOWN_EXCEPTION", "the write path is not available here", 5}

  defp describe({:catalog_unavailable, _reason}, _runtime),
    do:
      {503, 1002, "UNKNOWN_EXCEPTION", "the catalog could not confirm the table's columns; retry",
       1}

  defp describe(:whole_request_unsupported, _runtime),
    do:
      {503, 1002, "UNKNOWN_EXCEPTION",
       "the owning buffer node runs a release that cannot refuse a whole request; finish the rollout",
       5}

  defp describe(reason, _runtime),
    do: {500, 1002, "UNKNOWN_EXCEPTION", "insert failed: #{inspect(reason)}", nil}

  defp refusal_message(errors) do
    shown =
      errors
      |> Enum.take(@shown_refusals)
      |> Enum.map_join("; ", fn %{index: index, errors: messages} ->
        "row #{index}: " <> Enum.map_join(messages, ", ", & &1.message)
      end)

    more =
      if length(errors) > @shown_refusals,
        do: "; and #{length(errors) - @shown_refusals} more",
        else: ""

    "#{length(errors)} row(s) refused, nothing was written: #{shown}#{more}"
  end
end
