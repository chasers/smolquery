defmodule SmolqueryWeb.Supervisor do
  @moduledoc """
  Top-level subtree for the `:web` role.

  Started only on nodes whose roles include `:web` (see `Smolquery.Roles`).
  The strategy is `rest_for_one`, catalog engine first, because every LiveView
  resolves tables through it; a catalog engine that dies takes the endpoint
  down with it rather than leaving pages rendering through a dead handle.

  `SmolqueryWeb.Runtime.new/1` gates the boot: it requires the basic-auth
  credential and a session secret of at least 64 bytes. The gate runs here at
  `start_link/1`, not at config-evaluation time — the image build also
  evaluates `config/runtime.exs`, and no secret exists at build time.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias SmolqueryWeb.Runtime

  @doc """
  Starts the web UI.

  Takes any `SmolqueryWeb.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children =
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        [
          {Phoenix.PubSub, name: Smolquery.PubSub},
          SmolqueryWeb.Endpoint
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
