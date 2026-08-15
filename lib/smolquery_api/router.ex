defmodule SmolqueryApi.Router do
  @moduledoc """
  The HTTP front door for the API.

  Every request enters an instance-specific pipeline. API routes then run in
  the order instance, authentication, closed route capability authorization,
  parsers, and controller. The authenticated catch-all keeps absent routes
  indistinguishable from real routes until credentials have been accepted.
  """

  use Phoenix.Router

  pipeline :authenticated do
    plug :put_instance
    plug SmolqueryApi.Auth
  end

  pipeline :query do
    plug :put_instance
    plug SmolqueryApi.Auth
    plug SmolqueryApi.Authorization, capability: :query
    plug SmolqueryApi.Parsers
  end

  pipeline :ingest do
    plug :put_instance
    plug SmolqueryApi.Auth
    plug SmolqueryApi.Authorization, capability: :ingest
    plug SmolqueryApi.Parsers
  end

  pipeline :catalog_manage do
    plug :put_instance
    plug SmolqueryApi.Auth
    plug SmolqueryApi.Authorization, capability: :catalog_manage
    plug SmolqueryApi.Parsers
  end

  pipeline :public do
    plug :put_instance
  end

  pipeline :metrics do
    plug :put_instance
    plug SmolqueryApi.Auth
    plug SmolqueryApi.Parsers
  end

  scope "/", SmolqueryApi do
    pipe_through :public

    get "/healthz", HealthController, :show
  end

  scope "/", SmolqueryApi do
    pipe_through :metrics

    get "/metrics", MetricsController, :show
  end

  scope "/", SmolqueryApi do
    pipe_through :query

    get "/v1/datasets", DatasetController, :index
    get "/v1/datasets/:dataset/tables", TableController, :index
    get "/v1/datasets/:dataset/tables/:table", TableController, :show
    post "/v1/queries", QueryController, :create
    post "/v1/jobs", JobController, :create
    get "/v1/jobs/:id/results", JobController, :results
    get "/v1/jobs/:id", JobController, :show
    delete "/v1/jobs/:id", JobController, :delete
  end

  scope "/", SmolqueryApi do
    pipe_through :ingest

    post "/v1/datasets/:dataset/tables/:table/insert", InsertController, :create
    post "/v1/datasets/:dataset/tables/:table/load", LoadController, :create
  end

  scope "/", SmolqueryApi do
    pipe_through :catalog_manage

    post "/v1/datasets", DatasetController, :create
    post "/v1/datasets/:dataset/tables", TableController, :create
    patch "/v1/datasets/:dataset/tables/:table", TableController, :update
  end

  scope "/", SmolqueryApi do
    pipe_through :authenticated

    match :*, "/*path", NoRouteController, :not_found
  end

  defp put_instance(conn, _opts) do
    case conn.private do
      %{smolquery_api: _name} -> conn
      _absent -> Plug.Conn.put_private(conn, :smolquery_api, SmolqueryApi)
    end
  end
end
