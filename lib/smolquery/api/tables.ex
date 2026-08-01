defmodule Smolquery.Api.Tables do
  @moduledoc """
  The table routes' logic: list, create, and describe, over `Smolquery.Catalog`.

  Creation rides the catalog's `CREATE TABLE IF NOT EXISTS`, which makes an
  exact re-create idempotent — but silently keeps the existing table when the
  schemas differ. That silence would be a footgun, so `create/2` reads the
  schema back and answers 409 when what exists is not what was asked for.
  """

  alias Smolquery.Api.Datasets
  alias Smolquery.Api.Errors
  alias Smolquery.Api.Json
  alias Smolquery.Api.TableSchema
  alias Smolquery.Catalog

  @doc """
  Every table in a dataset.
  """
  @spec list(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def list(conn, dataset) do
    with :ok <- Datasets.exists(conn, dataset),
         {:ok, tables} <- Catalog.list_tables(Datasets.catalog(conn), dataset) do
      Json.send_json(conn, 200, %{"tables" => tables})
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Creates the table the body names, with the schema it carries.
  """
  @spec create(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def create(conn, dataset) do
    catalog = Datasets.catalog(conn)

    with {:ok, id} <- id(conn.body_params),
         {:ok, schema} <- TableSchema.from_json(conn.body_params["schema"]),
         :ok <- Datasets.exists(conn, dataset),
         :ok <- Catalog.create_table(catalog, {dataset, id}, schema),
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
  A table's schema.
  """
  @spec get(Plug.Conn.t(), String.t(), String.t()) :: Plug.Conn.t()
  def get(conn, dataset, table) do
    case Catalog.table_schema(Datasets.catalog(conn), {dataset, table}) do
      {:ok, schema} ->
        Json.send_json(conn, 200, %{"id" => table, "schema" => TableSchema.to_json(schema)})

      {:error, reason} ->
        Errors.from_reason(conn, reason)
    end
  end

  defp id(%{"id" => id}) when is_binary(id), do: {:ok, id}
  defp id(_body), do: {:error, {:missing_field, "id"}}
end
