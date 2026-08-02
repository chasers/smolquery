defmodule SmolqueryApi.DatasetController do
  @moduledoc """
  The dataset routes' logic: list and create, over `Smolquery.Catalog`.

  Creation is idempotent — the catalog's `CREATE SCHEMA IF NOT EXISTS` makes
  creating an existing dataset a 200, not a conflict, and there is no schema
  attached to a dataset that could silently differ.
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime

  @doc """
  Every dataset in the catalog.
  """
  @spec index(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def index(conn, _params) do
    case Catalog.list_datasets(catalog(conn)) do
      {:ok, datasets} -> Json.send_json(conn, 200, %{"datasets" => datasets})
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Creates the dataset the body names.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, _params) do
    with {:ok, id} <- id(conn.body_params),
         :ok <- Catalog.create_dataset(catalog(conn), id) do
      Json.send_json(conn, 200, %{"id" => id})
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Whether `dataset` exists — `{:error, {:unknown_dataset, dataset}}` when not,
  so callers can thread it straight into `Errors.from_reason/2`.
  """
  @spec exists(Plug.Conn.t(), String.t()) :: :ok | {:error, term()}
  def exists(conn, dataset) do
    with {:ok, datasets} <- Catalog.list_datasets(catalog(conn)) do
      if dataset in datasets, do: :ok, else: {:error, {:unknown_dataset, dataset}}
    end
  end

  @doc """
  The catalog handle requests answer through.

  Auth already proved the runtime is published, so a miss here is a broken
  invariant, not a case to handle.
  """
  @spec catalog(Plug.Conn.t()) :: Catalog.t()
  def catalog(conn) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    runtime.catalog
  end

  defp id(%{"id" => id}) when is_binary(id), do: {:ok, id}
  defp id(_body), do: {:error, {:missing_field, "id"}}
end
