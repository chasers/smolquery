defmodule SmolqueryApi.InsertController do
  @moduledoc """
  The streaming-insert route's logic, over `Smolquery.IngestService.Client`.

  A 200 means the buffer service has every accepted row durable and queryable,
  and the body says which rows were not accepted (`insertErrors`, per-index) —
  partial failure is a successful response, BigQuery-style. Whole-request
  failures map to the envelope: an unknown table is a 404, a full buffer is a
  429 with `retry-after`, a service that is not running here is a 503.
  """

  use SmolqueryApi, :controller

  alias Smolquery.IngestService
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime

  @doc """
  Inserts the body's rows into a table.

  An optional `insertId` makes the request idempotent: retrying it — after a
  timeout, a dropped connection, or a 5xx whose write may still have landed —
  with the same id cannot double-count the rows. The id identifies *this
  batch*, so a retry must carry the same rows; BigQuery-style, best-effort
  scope: the window closes for as long as the batch's segment stays in the
  hot tier, which comfortably covers any retry loop.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset, "table" => table}) do
    if ndjson?(conn) do
      spooled(conn, {dataset, table})
    else
      parsed(conn, {dataset, table})
    end
  end

  # `SmolqueryApi.Parsers` lists application/x-ndjson in `:pass`, so the body
  # arrives unread and unparsed. It goes to disk as it streams and DuckDB reads it
  # at flush — the point being that no row here becomes an Elixir term.
  #
  # The trade is explicit and is not the JSON route's: nothing validates per row,
  # so `insertErrors` is always empty and a value the schema cannot take fails the
  # whole flush rather than one index. Requires `flush_writer: :duckdb`.
  defp spooled(conn, table_ref) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)
    path = spool_path()

    case write_body(conn, path, SmolqueryApi.Parsers.max_body_bytes()) do
      {:ok, conn, 0} ->
        File.rm(path)

        Json.send_json(conn, 200, %{"insertedRows" => 0, "insertErrors" => []})

      {:ok, conn, rows} ->
        insert_spooled(conn, runtime, table_ref, path, rows)

      {:error, conn, :too_large} ->
        File.rm(path)

        Errors.send_error(
          conn,
          413,
          "REQUEST_TOO_LARGE",
          "request body exceeds #{SmolqueryApi.Parsers.max_body_bytes()} bytes; split the batch"
        )
    end
  end

  defp insert_spooled(conn, runtime, table_ref, path, rows) do
    batch_id =
      case get_req_header(conn, "x-smolquery-insert-id") do
        [id | _rest] -> id
        [] -> nil
      end

    case IngestService.Client.insert_file(runtime.ingest_name, table_ref, path, rows,
           batch_id: batch_id
         ) do
      {:ok, result} ->
        # The buffer deletes the body once the flush has written it; nothing is
        # removed here, or a queued accumulator would lose the file it is holding.
        Json.send_json(conn, 200, %{
          "insertedRows" => result.inserted,
          "insertErrors" => []
        })

      {:error, reason} ->
        File.rm(path)

        insert_error(conn, reason)
    end
  end

  defp ndjson?(conn) do
    conn
    |> get_req_header("content-type")
    |> Enum.any?(&String.starts_with?(&1, "application/x-ndjson"))
  end

  defp write_body(conn, path, max_bytes) do
    File.mkdir_p!(Path.dirname(path))

    File.open!(path, [:write, :raw, :binary], fn file ->
      copy_body(conn, file, max_bytes, 0)
    end)
  end

  # Lines are counted while the bytes stream past, because the ack has to say how
  # many rows were accepted and counting them later would mean a second pass over
  # the file. A body whose last line has no newline still counts, hence the tail
  # adjustment in `rows/2`.
  defp copy_body(conn, file, budget, newlines) do
    case read_body(conn, length: 8_000_000) do
      {:ok, chunk, conn} ->
        if byte_size(chunk) > budget do
          {:error, conn, :too_large}
        else
          :ok = IO.binwrite(file, chunk)

          {:ok, conn, rows(newlines + count_newlines(chunk), chunk)}
        end

      {:more, chunk, conn} ->
        case budget - byte_size(chunk) do
          exhausted when exhausted < 0 ->
            {:error, conn, :too_large}

          remaining ->
            :ok = IO.binwrite(file, chunk)

            copy_body(conn, file, remaining, newlines + count_newlines(chunk))
        end
    end
  end

  defp rows(newlines, <<>>), do: newlines
  defp rows(newlines, chunk), do: if(String.ends_with?(chunk, "\n"), do: newlines, else: newlines + 1)

  defp count_newlines(chunk) do
    chunk |> :binary.matches("\n") |> length()
  end

  defp spool_path do
    dir = Application.get_env(:smolquery, :data_dir, System.tmp_dir!())

    Path.join([dir, "tmp", "insert-#{System.unique_integer([:positive])}"])
  end

  defp parsed(conn, {dataset, table}) do
    with {:ok, rows} <- payload(conn.body_params),
         {:ok, batch_id} <- insert_id(conn.body_params),
         {:ok, result} <- insert_rows(conn, {dataset, table}, rows, batch_id) do
      Json.send_json(conn, 200, %{
        "insertedRows" => result.inserted,
        "insertErrors" => errors_json(result.errors)
      })
    else
      {:error, reason} -> insert_error(conn, reason)
    end
  end

  @doc """
  Maps a whole-request write failure to the envelope; shared with batch loads.
  """
  @spec insert_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def insert_error(conn, :buffer_full) do
    conn
    |> put_resp_header("retry-after", "1")
    |> Errors.send_error(429, "RESOURCE_EXHAUSTED", "buffer full, retry later")
  end

  def insert_error(conn, {:overloaded, predicted_ms}) do
    conn
    |> put_resp_header("retry-after", Integer.to_string(max(ceil(predicted_ms / 1000), 1)))
    |> Errors.send_error(
      429,
      "RESOURCE_EXHAUSTED",
      "write path overloaded, ~#{predicted_ms} ms behind; retry later"
    )
  end

  def insert_error(conn, reason)
      when reason in [:buffer_service_unavailable, :ingest_service_unavailable] do
    Errors.send_error(conn, 503, "UNAVAILABLE", "the write path is not available here")
  end

  def insert_error(conn, reason)
      when reason in [:not_owner, :ownership_settling, :ring_config_stale, :draining] do
    conn
    |> put_resp_header("retry-after", "1")
    |> Errors.send_error(503, "UNAVAILABLE", "table ownership is moving; retry")
  end

  def insert_error(conn, reason), do: Errors.from_reason(conn, reason)

  defp insert_rows(conn, table_ref, rows, batch_id) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.insert(runtime.ingest_name, table_ref, rows, batch_id: batch_id)
  end

  defp insert_id(%{"insertId" => id}) when is_binary(id) and id != "" and byte_size(id) <= 128,
    do: {:ok, id}

  defp insert_id(%{"insertId" => _invalid}), do: {:error, {:invalid_param, "insertId"}}
  defp insert_id(_body), do: {:ok, nil}

  @doc """
  The `insertErrors` JSON shape of validator rejections; shared with loads.
  """
  @spec errors_json([Smolquery.IngestService.Validator.row_errors()]) :: [map()]
  def errors_json(errors) do
    Enum.map(errors, fn %{index: index, errors: messages} ->
      %{"index" => index, "errors" => Enum.map(messages, &%{"message" => &1.message})}
    end)
  end

  defp payload(%{"rows" => rows}) when is_list(rows), do: {:ok, rows}

  # The columnar body: one entry per column instead of one object per row, which
  # for a wide table is about half the bytes because the column names are sent
  # once rather than once per row. Same data, same rows, nothing dropped.
  #
  # `rowCount` is the client's own statement of how many rows it is sending, and
  # it is required rather than inferred from the longest column. Inferring it
  # would silently accept a body whose columns disagree — the failure that shape
  # is prone to — and turn a truncated upload into a batch of nulls.
  defp payload(%{"columns" => columns, "rowCount" => count})
       when is_map(columns) and is_integer(count) and count >= 0 do
    if Enum.all?(columns, fn {_name, values} -> is_list(values) end) do
      {:ok, {:columns, columns, count}}
    else
      {:error, {:invalid_param, "columns"}}
    end
  end

  defp payload(%{"columns" => _columns}), do: {:error, {:missing_field, "rowCount"}}
  defp payload(_body), do: {:error, {:missing_field, "rows"}}
end
