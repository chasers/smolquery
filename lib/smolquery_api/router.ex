defmodule SmolqueryApi.Router do
  @moduledoc """
  The HTTP front door — one listener, every external surface (PL-8 D1,
  Phoenix since PL-16).

  A peer of the services, not part of one: routes dispatch only to service
  client modules and `Smolquery.Catalog`, never into a service's internals, so
  the split-out rules hold by construction. Started by the `:api` role.

  One pipeline, and its order is the contract: instance → auth → admission →
  parsers. `SmolqueryApi.Auth` runs before `SmolqueryApi.Parsers`, so an
  unauthenticated body is never read; `SmolqueryApi.Admission` sits between
  them, so an ingest body is counted against the in-flight limit before its
  first byte is read (T-245); the catch-all route at the bottom runs
  *inside* the authed pipeline, so an unauthenticated request is answered 401
  whether its path exists or not — 404 never reveals which routes are real.

  v1 surface so far:

      GET  /healthz                                liveness, no auth
      GET  /metrics                                Prometheus text (internal secret)
      GET  /v1/datasets                            list datasets
      POST /v1/datasets                            create a dataset
      GET  /v1/datasets/:ds/tables                 list a dataset's tables
      POST /v1/datasets/:ds/tables                 create a table
      GET  /v1/datasets/:ds/tables/:table          a table's schema + retention
      PATCH /v1/datasets/:ds/tables/:table         set/clear retention policy
      POST /v1/datasets/:ds/tables/:table/insert   streaming insert
      POST /v1/datasets/:ds/tables/:table/load     batch load (NDJSON/CSV/Parquet body)
      POST /v1/queries                             sync query, first page inline
      POST /v1/jobs                                async query job
      GET  /v1/jobs/:id                            job status + stats (history fallback)
      GET  /v1/jobs/:id/results                    page a finished job's rows
      DELETE /v1/jobs/:id                          cancel

  Failures speak `SmolqueryApi.Errors`' envelope, and nothing else — including
  what body parsing rejects (`SmolqueryApi.Parsers`) and anything a route
  raises (`SmolqueryApi.ErrorJSON`).

  The instance plug fills `conn.private.smolquery_api` only when absent —
  the seam that lets a test publish a uniquely-named runtime, inject its name
  with `put_private/3`, and dispatch through the one endpoint concurrently.
  """

  use Phoenix.Router

  pipeline :api do
    plug :put_instance
    plug SmolqueryApi.Auth
    plug :admit_ingest
    plug SmolqueryApi.Parsers
  end

  scope "/", SmolqueryApi do
    pipe_through :api

    get "/healthz", HealthController, :show
    get "/metrics", MetricsController, :show

    get "/v1/datasets", DatasetController, :index
    post "/v1/datasets", DatasetController, :create
    get "/v1/datasets/:dataset/tables", TableController, :index
    post "/v1/datasets/:dataset/tables", TableController, :create
    get "/v1/datasets/:dataset/tables/:table", TableController, :show
    patch "/v1/datasets/:dataset/tables/:table", TableController, :update
    post "/v1/datasets/:dataset/tables/:table/insert", InsertController, :create
    post "/v1/datasets/:dataset/tables/:table/load", LoadController, :create
    post "/v1/queries", QueryController, :create
    post "/v1/jobs", JobController, :create
    get "/v1/jobs/:id/results", JobController, :results
    get "/v1/jobs/:id", JobController, :show
    delete "/v1/jobs/:id", JobController, :delete

    match :*, "/*path", NoRouteController, :not_found
  end

  defp put_instance(conn, _opts) do
    case conn.private do
      %{smolquery_api: _name} -> conn
      _absent -> put_private(conn, :smolquery_api, SmolqueryApi)
    end
  end

  defp admit_ingest(conn, _opts), do: SmolqueryApi.Admission.admit_conn(conn)
end
