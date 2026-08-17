defmodule SmolqueryApi.Supervisor do
  @moduledoc """
  Top-level subtree for the `:api` role.

  Started only on nodes whose roles include `:api` (see `Smolquery.Roles`).
  Resolving the runtime happens in `start_link/1`, so a node missing its
  static API key or OIDC provider foundation fails the boot right here — fail
  closed — rather than starting a listener that would wave requests through.

  The strategy is `rest_for_one`, catalog engine first, because the endpoint
  answers CRUD through it; a catalog engine that dies takes the endpoint down
  with it rather than leaving routes serving through a dead handle. The same
  shape as `SmolqueryWeb.Supervisor` — both surfaces are Phoenix endpoints
  under a role-gated subtree now (PL-16).
  """

  use Supervisor

  alias Smolquery.Auth.OIDC
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Runtime.Publisher
  alias SmolqueryApi.Runtime

  @doc """
  Starts the API.

  Takes any `SmolqueryApi.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    children =
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        OIDC.children(
          runtime.auth_mode,
          runtime.oidc,
          runtime.name,
          runtime.oidc_provider_http_client
        ) ++
        [
          {Publisher, {Runtime, runtime}},
          {SmolqueryApi.Admission, runtime},
          SmolqueryApi.Endpoint
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
