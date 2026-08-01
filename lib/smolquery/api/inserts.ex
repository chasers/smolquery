defmodule Smolquery.Api.Inserts do
  @moduledoc """
  The streaming-insert route's logic, over `Smolquery.IngestService.Client`.

  A 200 means the buffer service has every accepted row durable and queryable,
  and the body says which rows were not accepted (`insertErrors`, per-index) —
  partial failure is a successful response, BigQuery-style. Whole-request
  failures map to the envelope: an unknown table is a 404, a full buffer is a
  429 with `retry-after`, a service that is not running here is a 503.
  """

  import Plug.Conn, only: [put_resp_header: 3]

  alias Smolquery.Api.Errors
  alias Smolquery.Api.Json
  alias Smolquery.Api.Runtime
  alias Smolquery.IngestService

  @doc """
  Inserts the body's rows into a table.
  """
  @spec insert(Plug.Conn.t(), String.t(), String.t()) :: Plug.Conn.t()
  def insert(conn, dataset, table) do
    with {:ok, rows} <- rows(conn.body_params),
         {:ok, result} <- insert_rows(conn, {dataset, table}, rows) do
      Json.send_json(conn, 200, %{
        "insertedRows" => result.inserted,
        "insertErrors" => errors_json(result.errors)
      })
    else
      {:error, :buffer_full} ->
        conn
        |> put_resp_header("retry-after", "1")
        |> Errors.send_error(429, "RESOURCE_EXHAUSTED", "buffer full, retry later")

      {:error, reason}
      when reason in [:buffer_service_unavailable, :ingest_service_unavailable] ->
        Errors.send_error(conn, 503, "UNAVAILABLE", "the write path is not available here")

      {:error, reason} ->
        Errors.from_reason(conn, reason)
    end
  end

  defp insert_rows(conn, table_ref, rows) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.insert(runtime.ingest_name, table_ref, rows)
  end

  defp rows(%{"rows" => rows}) when is_list(rows), do: {:ok, rows}
  defp rows(_body), do: {:error, {:missing_field, "rows"}}

  defp errors_json(errors) do
    Enum.map(errors, fn %{index: index, errors: messages} ->
      %{"index" => index, "errors" => Enum.map(messages, &%{"message" => &1.message})}
    end)
  end
end
