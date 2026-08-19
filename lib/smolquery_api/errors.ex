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
  Sends the shed-load refusal every over-capacity route speaks: 429,
  `RESOURCE_EXHAUSTED`, and a `retry-after` of `seconds`.

  One function so the contract cannot drift between the buffer's refusals,
  the job ceiling, and ingest admission.
  """
  @spec send_resource_exhausted(Plug.Conn.t(), pos_integer(), String.t()) :: Plug.Conn.t()
  def send_resource_exhausted(conn, seconds, message) do
    conn
    |> put_resp_header("retry-after", Integer.to_string(seconds))
    |> send_error(429, "RESOURCE_EXHAUSTED", message)
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

  def from_reason(conn, {:invalid_partitions, _partitions}) do
    send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      "partitions must be an integer between 1 and #{Smolquery.Partitions.max_count()}"
    )
  end

  def from_reason(conn, {:partitions_not_raisable, current, requested}) do
    send_error(
      conn,
      422,
      "FAILED_PRECONDITION",
      "partitions is raise-only: the table holds #{current}, #{requested} would strand rows"
    )
  end

  def from_reason(conn, :commit_conflict) do
    send_error(
      conn,
      409,
      "ABORTED",
      "the catalog commit lost a race with a concurrent write; retry the request"
    )
  end

  def from_reason(conn, _reason) do
    send_error(conn, 500, "INTERNAL", "internal error")
  end
end
