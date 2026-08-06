defmodule SmolqueryApi.Errors do
  @moduledoc """
  The one JSON error envelope every route answers failures with (PL-8 D11).

      {"error": {"code": 404, "status": "NOT_FOUND", "message": "no such route"}}

  `code` repeats the HTTP status so a client parsing only the body still knows;
  `status` is a stable machine-readable symbol; `message` is for humans and
  never carries internals.
  """

  import Plug.Conn

  @doc """
  Sends the envelope and returns the conn.
  """
  @spec send_error(Plug.Conn.t(), pos_integer(), String.t(), String.t()) :: Plug.Conn.t()
  def send_error(conn, code, status, message) do
    body = JSON.encode!(%{"error" => %{"code" => code, "status" => status, "message" => message}})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, body)
  end

  @doc """
  Sends the envelope a failure reason maps to.

  The clauses cover what the catalog and schema layers actually return; any
  reason they do not is a 500 that names nothing — internals never leak
  through the envelope.
  """
  @spec from_reason(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def from_reason(conn, {:invalid_identifier, name}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "invalid identifier: #{inspect(name)}")
  end

  def from_reason(conn, {:unknown_table, {dataset, table}}) do
    send_error(conn, 404, "NOT_FOUND", "table #{dataset}.#{table} does not exist")
  end

  def from_reason(conn, {:unknown_dataset, dataset}) do
    send_error(conn, 404, "NOT_FOUND", "dataset #{dataset} does not exist")
  end

  def from_reason(conn, {:unsupported_type, type}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "unsupported type: #{inspect(type)}")
  end

  def from_reason(conn, :empty_schema) do
    send_error(conn, 400, "INVALID_ARGUMENT", "a schema needs at least one field")
  end

  def from_reason(conn, {:duplicate_columns, names}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "duplicate columns: #{Enum.join(names, ", ")}")
  end

  def from_reason(conn, {:invalid_field, field}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "invalid schema field: #{inspect(field)}")
  end

  def from_reason(conn, {:invalid_schema, _other}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "schema must be a list of fields")
  end

  def from_reason(conn, {:missing_field, name}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "missing required field: #{name}")
  end

  def from_reason(conn, {:invalid_param, name}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "invalid parameter: #{name}")
  end

  def from_reason(conn, {:invalid_retention, _retention}) do
    send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      ~s|retention must be null or {"column": <name>, "ttlMs": <positive integer>}|
    )
  end

  def from_reason(conn, {:unknown_retention_column, column}) do
    send_error(conn, 400, "INVALID_ARGUMENT", "retention column does not exist: #{column}")
  end

  def from_reason(conn, {:retention_column_not_temporal, column, _type}) do
    send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      "retention column must be a timestamp or date: #{column}"
    )
  end

  def from_reason(conn, {:invalid_clustering, _clustering}) do
    send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      ~s|clustering must be a list of unique column name strings|
    )
  end

  def from_reason(conn, {:unknown_clustering_column, column}) do
    send_error(conn, 422, "INVALID_ARGUMENT", "clustering column does not exist: #{column}")
  end

  def from_reason(conn, _reason) do
    send_error(conn, 500, "INTERNAL", "internal error")
  end
end
