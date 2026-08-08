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

  @max_ndjson_bytes 8_000_000

  @doc """
  Inserts the body's rows into a table.

  Two body shapes, one contract. `application/json` carries
  `{"rows": [...]}`; `application/x-ndjson` carries one JSON object per
  line — the same bytes ClickHouse's `JSONEachRow` takes — and rides the
  columnar fast path (`Smolquery.IngestService.Client.insert_ndjson/4`),
  which never materializes valid rows as Elixir terms. NDJSON requests pass
  `insertId` as a query parameter, since there is no envelope to put it in.

  An optional `insertId` makes the request idempotent: retrying it — after a
  timeout, a dropped connection, or a 5xx whose write may still have landed —
  with the same id cannot double-count the rows. The id identifies *this
  batch*, so a retry must carry the same rows; BigQuery-style, best-effort
  scope: the window closes for as long as the batch's segment stays in the
  hot tier, which comfortably covers any retry loop.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset, "table" => table}) do
    case List.first(Plug.Conn.get_req_header(conn, "content-type")) do
      "application/x-ndjson" <> _params -> create_ndjson(conn, {dataset, table})
      _json -> create_json(conn, {dataset, table})
    end
  end

  defp create_json(conn, table_ref) do
    with {:ok, rows} <- rows(conn.body_params),
         {:ok, batch_id} <- insert_id(conn.body_params),
         {:ok, result} <- insert_rows(conn, table_ref, rows, batch_id) do
      respond(conn, result)
    else
      {:error, reason} -> insert_error(conn, reason)
    end
  end

  defp create_ndjson(conn, table_ref) do
    with {:ok, batch_id} <- insert_id(conn.query_params),
         {:ok, body, conn} <- read_ndjson(conn),
         {:ok, result} <- insert_ndjson(conn, table_ref, body, batch_id) do
      respond(conn, result)
    else
      {:error, :too_large} ->
        Errors.send_error(
          conn,
          413,
          "PAYLOAD_TOO_LARGE",
          "NDJSON insert bodies are limited to #{@max_ndjson_bytes} bytes; use /load for files"
        )

      {:error, reason} ->
        insert_error(conn, reason)
    end
  end

  defp respond(conn, result) do
    Json.send_json(conn, 200, %{
      "insertedRows" => result.inserted,
      "insertErrors" => errors_json(result.errors)
    })
  end

  defp read_ndjson(conn, acc \\ []) do
    case Plug.Conn.read_body(conn, length: @max_ndjson_bytes) do
      {:ok, chunk, conn} ->
        body = IO.iodata_to_binary(Enum.reverse([chunk | acc]))

        if byte_size(body) > @max_ndjson_bytes,
          do: {:error, :too_large},
          else: {:ok, body, conn}

      {:more, chunk, conn} ->
        case Enum.reduce(acc, byte_size(chunk), &(byte_size(&1) + &2)) do
          over when over > @max_ndjson_bytes -> {:error, :too_large}
          _within -> read_ndjson(conn, [chunk | acc])
        end
    end
  end

  defp insert_ndjson(conn, table_ref, body, batch_id) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.insert_ndjson(runtime.ingest_name, table_ref, body, batch_id: batch_id)
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

  defp rows(%{"rows" => rows}) when is_list(rows), do: {:ok, rows}
  defp rows(_body), do: {:error, {:missing_field, "rows"}}
end
