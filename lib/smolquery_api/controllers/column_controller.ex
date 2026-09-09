defmodule SmolqueryApi.ColumnController do
  @moduledoc """
  The column routes (PL-61 L2): add a column, drop a column.

  Sub-resources rather than a full-schema `PATCH`, so a drop is always an
  explicit request — a client that re-sends yesterday's schema cannot drop a
  column by omission. Both routes answer the whole table body, as `PATCH`
  does, and both invalidate this node's ingest schema cache at once; every
  other node's drops the table when the change's broadcast lands
  (`Smolquery.Lifecycle`), with `schema_cache_ttl_ms` the backstop for a node
  the broadcast does not reach. A stale cache cannot lose data either way:
  the buffer that owns the table confirms a batch's column ids before writing
  them (T-439), so a write under a re-used name's old id is refused and
  retried, never stored under the wrong column.

  Every refusal is `Smolquery.Catalog.alter_table/3`'s, mapped to a status
  here: 404 for a table or column that does not exist; 409 for a name the
  table has; 422 for a column that cannot be added or dropped as asked, with
  the reason in the message. A name the table once had is not a refusal: the
  column that takes it is a new column, told apart from the old one by id in
  every file (PL-62).
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias Smolquery.Schema.Field
  alias SmolqueryApi.DatasetController
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.TableController
  alias SmolqueryApi.TableSchema

  @doc """
  Adds the column the body describes, appended last.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset, "table" => table}) do
    table_ref = {dataset, table}

    with {:ok, %Field{} = field} <- TableSchema.field_from_json(conn.body_params),
         :ok <- alter(conn, table_ref, {:add_column, field}) do
      answer(conn, table_ref)
    else
      {:error, {:duplicate_columns, [name]}} ->
        Errors.send_error(conn, 409, "ALREADY_EXISTS", "column #{name} already exists")

      {:error, reason} ->
        Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Drops the named column.
  """
  @spec delete(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def delete(conn, %{"dataset" => dataset, "table" => table, "column" => column}) do
    table_ref = {dataset, table}

    case alter(conn, table_ref, {:drop_column, column}) do
      :ok -> answer(conn, table_ref)
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  defp alter(conn, table_ref, change) do
    with :ok <- Catalog.alter_table(DatasetController.catalog(conn), table_ref, change) do
      TableController.invalidate_schema_cache(conn, table_ref)
    end
  end

  defp answer(conn, {_dataset, table} = table_ref) do
    catalog = DatasetController.catalog(conn)

    with {:ok, schema} <- Catalog.table_schema(catalog, table_ref),
         {:ok, policy} <- Catalog.retention(catalog, table_ref) do
      Json.send_json(conn, 200, TableController.table_body(table, schema, policy))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end
end
