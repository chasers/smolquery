defmodule SmolqueryApi.DatasetController do
  @moduledoc """
  The dataset routes' logic: list, create, show, and update, over
  `Smolquery.Catalog`.

  Creation is idempotent — re-creating a dataset with the same settings is a
  200, not a conflict. Re-creating it with different settings is a 409: a
  dataset's catalog and storage are its identity (PL-51), and there is no
  silent way to change them. The body names the dataset with `id`, as it
  always has; `catalog` and `storage` are optional, and either left out
  means the deployment default.

  `update/2` is credentials only. `Smolquery.Catalog.Dataset.update/2`
  refuses every other field, so the route never has to know which fields
  those are.
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
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
  Creates the dataset the body describes.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, _params) do
    catalog = catalog(conn)

    with {:ok, id} <- id(conn.body_params),
         {:ok, dataset} <- Dataset.new(Map.put(conn.body_params, "name", id)),
         :ok <- Catalog.create_dataset(catalog, dataset) do
      Json.send_json(conn, 200, stored_json(catalog, dataset))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  One dataset, without its secrets.
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"dataset" => name}) do
    case Catalog.dataset(catalog(conn), name) do
      {:ok, dataset} -> Json.send_json(conn, 200, json(dataset))
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Rotates a dataset's credentials. Every other field is immutable.
  """
  @spec update(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def update(conn, %{"dataset" => name}) do
    catalog = catalog(conn)

    with {:ok, current} <- Catalog.dataset(catalog, name),
         {:ok, updated} <- Dataset.update(current, conn.body_params),
         :ok <- Catalog.update_dataset(catalog, updated),
         {:ok, stored} <- Catalog.dataset(catalog, name) do
      Json.send_json(conn, 200, json(stored))
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

  defp stored_json(catalog, %Dataset{} = dataset) do
    case Catalog.dataset(catalog, dataset.name) do
      {:ok, stored} -> json(stored)
      {:error, _unsupported} -> json(dataset)
    end
  end

  defp json(%Dataset{} = dataset) do
    dataset |> Dataset.to_json() |> Map.put("id", dataset.name)
  end

  defp id(%{"id" => id}) when is_binary(id), do: {:ok, id}
  defp id(_body), do: {:error, {:missing_field, "id"}}
end
