defmodule SmolqueryApi.TableController do
  @moduledoc """
  The table routes' logic: list, create, and describe, over `Smolquery.Catalog`.

  Creation rides the catalog's `CREATE TABLE IF NOT EXISTS`, which makes an
  exact re-create idempotent — but silently keeps the existing table when the
  schemas differ. That silence would be a footgun, so `create/2` reads the
  schema back and answers 409 when what exists is not what was asked for.
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias SmolqueryApi.DatasetController
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime
  alias SmolqueryApi.TableSchema

  @doc """
  Every table in a dataset.
  """
  @spec index(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def index(conn, %{"dataset" => dataset}) do
    with :ok <- DatasetController.exists(conn, dataset),
         {:ok, tables} <- Catalog.list_tables(DatasetController.catalog(conn), dataset) do
      Json.send_json(conn, 200, %{"tables" => tables})
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Creates the table the body names, with the schema it carries.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset}) do
    catalog = DatasetController.catalog(conn)

    with {:ok, id} <- id(conn.body_params),
         {:ok, schema} <- TableSchema.from_json(conn.body_params["schema"]),
         :ok <- DatasetController.exists(conn, dataset),
         :ok <- Catalog.create_table(catalog, {dataset, id}, schema),
         :ok <- invalidate_schema_cache(conn, {dataset, id}),
         {:ok, existing} <- Catalog.table_schema(catalog, {dataset, id}) do
      if existing == schema do
        Json.send_json(conn, 200, %{"id" => id, "schema" => TableSchema.to_json(schema)})
      else
        Errors.send_error(
          conn,
          409,
          "ALREADY_EXISTS",
          "table #{dataset}.#{id} already exists with a different schema"
        )
      end
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  A table's schema and retention policy.
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"dataset" => dataset, "table" => table}) do
    catalog = DatasetController.catalog(conn)

    with {:ok, schema} <- Catalog.table_schema(catalog, {dataset, table}),
         {:ok, policy} <- Catalog.retention(catalog, {dataset, table}) do
      Json.send_json(conn, 200, table_body(table, schema, policy))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Updates a table's retention policy — the only mutable thing a table has.

  `{"retention": {"column": "ts", "ttlMs": 86400000}}` sets it,
  `{"retention": null}` clears it. The column must exist and carry a time
  type, checked here against the schema — the sweeper downstream is
  deliberately too conservative to complain, so this is where a typo gets
  caught by the person who made it.
  """
  @spec update(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def update(conn, %{"dataset" => dataset, "table" => table}) do
    catalog = DatasetController.catalog(conn)

    with {:ok, schema} <- Catalog.table_schema(catalog, {dataset, table}),
         {:ok, policy} <- retention_from_json(conn.body_params, schema),
         :ok <- Catalog.put_retention(catalog, {dataset, table}, policy) do
      Json.send_json(conn, 200, table_body(table, schema, policy))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  defp table_body(table, schema, policy) do
    %{
      "id" => table,
      "schema" => TableSchema.to_json(schema),
      "retention" => retention_to_json(policy)
    }
  end

  defp retention_to_json(nil), do: nil

  defp retention_to_json(%{column: column, ttl_ms: ttl_ms}),
    do: %{"column" => column, "ttlMs" => ttl_ms}

  defp retention_from_json(%{"retention" => nil}, _schema), do: {:ok, nil}

  defp retention_from_json(%{"retention" => %{"column" => column, "ttlMs" => ttl_ms}}, schema)
       when is_binary(column) and is_integer(ttl_ms) and ttl_ms > 0 do
    case Schema.field(schema, column) do
      {:ok, %{type: type}} when type in [:timestamp, :date] ->
        {:ok, %{column: column, ttl_ms: ttl_ms}}

      {:ok, %{type: type}} ->
        {:error, {:retention_column_not_temporal, column, type}}

      :error ->
        {:error, {:unknown_retention_column, column}}
    end
  end

  defp retention_from_json(%{"retention" => retention}, _schema),
    do: {:error, {:invalid_retention, retention}}

  defp retention_from_json(_body, _schema), do: {:error, {:missing_field, "retention"}}

  defp id(%{"id" => id}) when is_binary(id), do: {:ok, id}
  defp id(_body), do: {:error, {:missing_field, "id"}}

  defp invalidate_schema_cache(conn, table_ref) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.invalidate(runtime.ingest_name, table_ref)
  end
end
