defmodule Smolquery.Api.Router do
  @moduledoc """
  The HTTP front door — one listener, every external surface (PL-8 D1).

  A peer of the services, not part of one: routes dispatch only to service
  client modules and `Smolquery.Catalog`, never into a service's internals, so
  the split-out rules hold by construction. Started by the `:api` role.

  The pipeline is telemetry → match → auth → dispatch:
  `Plug.Telemetry` emits `[:smolquery, :api, :start | :stop]` around every
  request, and `Smolquery.Api.Auth` runs after `:match` so an unauthenticated
  request is answered 401 before any route logic — including the 404 catch-all —
  can reveal what exists.

  v1 surface so far:

      GET  /healthz                                liveness, no auth
      GET  /v1/datasets                            list datasets
      POST /v1/datasets                            create a dataset
      GET  /v1/datasets/:ds/tables                 list a dataset's tables
      POST /v1/datasets/:ds/tables                 create a table
      GET  /v1/datasets/:ds/tables/:table          a table's schema
      POST /v1/datasets/:ds/tables/:table/insert   streaming insert
      POST /v1/queries                             sync query, first page inline
      POST /v1/jobs                                async query job
      GET  /v1/jobs/:id                            job status + stats (history fallback)
      GET  /v1/jobs/:id/results                    page a finished job's rows
      DELETE /v1/jobs/:id                          cancel

  Failures speak `Smolquery.Api.Errors`' envelope, and nothing else — including
  what `Plug.Parsers` raises for a body that is not JSON, via
  `Plug.ErrorHandler`. Bodies parse after auth, so unauthenticated payloads are
  never read.
  """

  use Plug.Router
  use Plug.ErrorHandler

  alias Smolquery.Api.Datasets
  alias Smolquery.Api.Errors
  alias Smolquery.Api.Inserts
  alias Smolquery.Api.Jobs
  alias Smolquery.Api.Queries
  alias Smolquery.Api.Runtime
  alias Smolquery.Api.Tables

  plug(Plug.Telemetry, event_prefix: [:smolquery, :api])
  plug(:match)
  plug(Smolquery.Api.Auth)
  plug(Plug.Parsers, parsers: [:json], json_decoder: JSON, pass: [])
  plug(:dispatch)

  @impl Plug
  def init(name), do: name

  @impl Plug
  def call(conn, name) do
    conn
    |> put_private(:smolquery_api, name)
    |> super([])
  end

  get "/healthz" do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(%{"status" => "ok"}))
  end

  get "/v1/datasets" do
    Datasets.list(conn)
  end

  post "/v1/datasets" do
    Datasets.create(conn)
  end

  get "/v1/datasets/:dataset/tables" do
    Tables.list(conn, dataset)
  end

  post "/v1/datasets/:dataset/tables" do
    Tables.create(conn, dataset)
  end

  get "/v1/datasets/:dataset/tables/:table" do
    Tables.get(conn, dataset, table)
  end

  post "/v1/datasets/:dataset/tables/:table/insert" do
    Inserts.insert(conn, dataset, table)
  end

  post "/v1/queries" do
    Queries.create(conn)
  end

  post "/v1/jobs" do
    Jobs.create(conn)
  end

  get "/v1/jobs/:id/results" do
    Jobs.results(conn, id)
  end

  get "/v1/jobs/:id" do
    Jobs.get(conn, id)
  end

  delete "/v1/jobs/:id" do
    Jobs.cancel(conn, id)
  end

  match _ do
    Errors.send_error(conn, 404, "NOT_FOUND", "no such route")
  end

  @impl Plug.ErrorHandler
  def handle_errors(conn, %{reason: %Plug.Parsers.ParseError{}}) do
    Errors.send_error(conn, 400, "INVALID_ARGUMENT", "request body is not valid JSON")
  end

  def handle_errors(conn, %{reason: %Plug.Parsers.UnsupportedMediaTypeError{}}) do
    Errors.send_error(
      conn,
      415,
      "UNSUPPORTED_MEDIA_TYPE",
      "request body must be application/json"
    )
  end

  def handle_errors(conn, _error) do
    Errors.send_error(conn, 500, "INTERNAL", "internal error")
  end

  @doc """
  The pid of a running instance's HTTP listener.

  Not needed to serve requests — only for discovering the port Bandit bound
  when `port` is `0`, which is how tests run instances side by side.
  """
  @spec listener(atom()) :: pid() | nil
  def listener(name) do
    case Process.whereis(Runtime.supervisor(name)) do
      nil -> nil
      supervisor -> find_listener(supervisor, Runtime.listener(name))
    end
  end

  @doc """
  The base URL a running instance is listening on.
  """
  @spec base_url(atom()) :: String.t()
  def base_url(name) do
    {:ok, {_address, port}} = name |> listener() |> ThousandIsland.listener_info()

    "http://127.0.0.1:#{port}"
  end

  defp find_listener(supervisor, id) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^id, pid, _type, _modules} -> pid
      _child -> nil
    end)
  end
end
