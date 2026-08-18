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
      if existing.fields == schema.fields do
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
  A table's schema, retention policy, clustering key, and partition count.
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
  Updates a table's retention policy and/or clustering key.

  Retention: `{"retention": {"column": "ts", "ttlMs": 86400000}}` sets it,
  `{"retention": null}` clears it. The column must exist and carry a time
  type, checked here against the schema — the sweeper downstream is
  deliberately too conservative to complain, so this is where a typo gets
  caught by the person who made it.

  Clustering: `{"clustering": ["project_id", "ts"]}` sets the sort key for
  future writes; `{"clustering": []}` clears it. Columns must exist on the
  schema. Existing segments are not rewritten.

  Partitions: `{"partitions": 3}` sets the table's own write-partition count
  (T-304) — the effective count is never below the deployment's
  `write_partitions` (`Smolquery.Partitions.count/2`). Raise-only: a value
  below the stored count is refused, because partitions past it still hold
  hot rows a reader would no longer expand. There is no clear.

  A body carrying several applies them as one change
  (`Smolquery.Catalog.put_table_options/3`): an error response means none
  stuck.
  """
  @spec update(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def update(conn, %{"dataset" => dataset, "table" => table}) do
    catalog = DatasetController.catalog(conn)
    table_ref = {dataset, table}

    with {:ok, schema} <- Catalog.table_schema(catalog, table_ref),
         {:ok, updates} <- patch_from_json(conn.body_params, schema),
         :ok <- Catalog.put_table_options(catalog, table_ref, updates),
         :ok <- invalidate_schema_cache(conn, table_ref),
         {:ok, schema} <- Catalog.table_schema(catalog, table_ref),
         {:ok, policy} <- Catalog.retention(catalog, table_ref) do
      Json.send_json(conn, 200, table_body(table, schema, policy))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  defp table_body(table, schema, policy) do
    %{
      "id" => table,
      "schema" => TableSchema.to_json(schema),
      "retention" => retention_to_json(policy),
      "clustering" => schema.clustering,
      "partitions" => schema.partitions
    }
  end

  defp patch_from_json(body, schema) do
    has_retention = Map.has_key?(body, "retention")
    has_clustering = Map.has_key?(body, "clustering")
    has_partitions = Map.has_key?(body, "partitions")

    if has_retention or has_clustering or has_partitions do
      with {:ok, updates} <- maybe_retention(body, schema, has_retention, %{}),
           {:ok, updates} <- maybe_clustering(body, schema, has_clustering, updates) do
        maybe_partitions(body, schema, has_partitions, updates)
      end
    else
      {:error, {:missing_field, "retention"}}
    end
  end

  defp maybe_retention(_body, _schema, false, updates), do: {:ok, updates}

  defp maybe_retention(body, schema, true, updates) do
    with {:ok, policy} <- retention_from_json(body, schema) do
      {:ok, Map.put(updates, :retention, policy)}
    end
  end

  defp maybe_clustering(_body, _schema, false, updates), do: {:ok, updates}

  defp maybe_clustering(body, schema, true, updates) do
    with {:ok, clustering} <- clustering_from_json(body, schema) do
      {:ok, Map.put(updates, :clustering, clustering)}
    end
  end

  defp maybe_partitions(_body, _schema, false, updates), do: {:ok, updates}

  defp maybe_partitions(body, schema, true, updates) do
    with {:ok, count} <- partitions_from_json(body, schema) do
      {:ok, Map.put(updates, :partitions, count)}
    end
  end

  defp partitions_from_json(%{"partitions" => count}, schema)
       when is_integer(count) and count > 0 do
    cond do
      count > Smolquery.Partitions.max_count() ->
        {:error, {:invalid_partitions, count}}

      is_integer(schema.partitions) and count < schema.partitions ->
        {:error, {:partitions_not_raisable, schema.partitions, count}}

      true ->
        {:ok, count}
    end
  end

  defp partitions_from_json(%{"partitions" => count}, _schema),
    do: {:error, {:invalid_partitions, count}}

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

  defp clustering_from_json(%{"clustering" => columns}, schema) when is_list(columns) do
    malformed = {:error, {:invalid_clustering, columns}}

    columns
    |> Enum.reduce_while({:ok, MapSet.new()}, fn
      column, {:ok, seen} when is_binary(column) ->
        cond do
          MapSet.member?(seen, column) ->
            {:halt, malformed}

          Schema.field(schema, column) == :error ->
            {:halt, {:error, {:unknown_clustering_column, column}}}

          true ->
            {:cont, {:ok, MapSet.put(seen, column)}}
        end

      _column, _acc ->
        {:halt, malformed}
    end)
    |> case do
      {:ok, _seen} -> {:ok, columns}
      {:error, reason} -> {:error, reason}
    end
  end

  defp clustering_from_json(%{"clustering" => clustering}, _schema),
    do: {:error, {:invalid_clustering, clustering}}

  defp id(%{"id" => id}) when is_binary(id) do
    if Smolquery.Partitions.reserved?(id) do
      {:error, {:invalid_param, "id"}}
    else
      {:ok, id}
    end
  end

  defp id(_body), do: {:error, {:missing_field, "id"}}

  defp invalidate_schema_cache(conn, table_ref) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.invalidate(runtime.ingest_name, table_ref)
  end
end
