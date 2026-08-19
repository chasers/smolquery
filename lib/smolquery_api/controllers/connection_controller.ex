defmodule SmolqueryApi.ConnectionController do
  @moduledoc """
  The federated-connection routes: list, create, show, update, delete, and a
  connectivity check (T-323).

  Reads and writes go through `Smolquery.Catalog`, the same layering the
  dataset and table routes use.

  ## The password is write-only

  Every read answers through `Smolquery.Catalog.Connection.to_json/1`, which
  names the fields it returns rather than dropping the ones it does not. A
  field can therefore only reach a client by being added there on purpose, and
  neither the password nor the sealed secret is.

  `PATCH` without a `password` key leaves the stored secret untouched. That is
  what lets a caller correct a host or a port without re-entering a credential
  it can never read back — and it is why an empty-string password is a
  validation error rather than a clear: clearing a password would leave a
  connection that cannot open.

  ## Creation replaces, and says so in the status

  `POST` to an existing name replaces it, like `PUT`. The alternative — a 409
  and a separate `PATCH` — would make a config-management client read before
  every write to stay idempotent. The response is 200 for a replacement and
  201 for a new connection, so a caller that cares can still tell.
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Federation
  alias SmolqueryApi.DatasetController
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json

  @doc """
  Every registered connection, by name.
  """
  @spec index(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def index(conn, _params) do
    case Catalog.list_connections(DatasetController.catalog(conn)) do
      {:ok, connections} ->
        Json.send_json(conn, 200, %{"connections" => Enum.map(connections, &Connection.to_json/1)})

      {:error, reason} ->
        Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Registers the connection the body describes, replacing one of the same name.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, _params) do
    catalog = DatasetController.catalog(conn)
    body = conn.body_params

    with {:ok, connection} <- Connection.new(body),
         {:ok, status} <- creation_status(catalog, connection.name),
         :ok <- Catalog.put_connection(catalog, connection),
         {:ok, stored} <- Catalog.connection(catalog, connection.name) do
      Json.send_json(conn, status, Connection.to_json(stored))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  One connection, without its secret.
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"name" => name}) do
    case Catalog.connection(DatasetController.catalog(conn), name) do
      {:ok, connection} -> Json.send_json(conn, 200, Connection.to_json(connection))
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Applies the body's fields to an existing connection.
  """
  @spec update(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def update(conn, %{"name" => name}) do
    catalog = DatasetController.catalog(conn)

    with {:ok, current} <- Catalog.connection(catalog, name),
         {:ok, updated} <- Connection.update(current, conn.body_params),
         :ok <- Catalog.put_connection(catalog, updated),
         {:ok, stored} <- Catalog.connection(catalog, name) do
      Json.send_json(conn, 200, Connection.to_json(stored))
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Removes a connection. Removing one that is already absent is a 200 — the
  caller asked for its absence, and it is absent.
  """
  @spec delete(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def delete(conn, %{"name" => name}) do
    case Catalog.delete_connection(DatasetController.catalog(conn), name) do
      :ok -> Json.send_json(conn, 200, %{"name" => name, "deleted" => true})
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Opens the connection and reads one row through it.

  A failure here is the remote database's, not the request's, so it answers
  422 with the scrubbed reason rather than 400: the stored connection is
  well-formed and something on the other end is not answering.
  """
  @spec test(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def test(conn, %{"name" => name}) do
    with {:ok, connection} <- Catalog.connection(DatasetController.catalog(conn), name),
         :ok <- Federation.probe(connection) do
      Json.send_json(conn, 200, %{"name" => name, "ok" => true})
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  defp creation_status(catalog, name) do
    case Catalog.connection(catalog, name) do
      {:ok, _existing} -> {:ok, 200}
      {:error, {:unknown_connection, _name}} -> {:ok, 201}
      {:error, reason} -> {:error, reason}
    end
  end
end
