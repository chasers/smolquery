defmodule SmolqueryVictoriaMetrics.Supervisor do
  @moduledoc """
  Top-level subtree for the `:victoriametrics` role (PL-70, T-562).

  Started only on nodes whose roles include `:victoriametrics` (see
  `Smolquery.Roles`). Resolving the runtime happens in `start_link/1`, so a
  node missing its password fails the boot right here — fail closed — rather
  than starting a listener that would wave writes through.

  The children, `rest_for_one`: the engine of the edge's own lake, when it
  reads one (`Smolquery.Catalog.DuckLake.children/2`), then the edge's own
  ingest admission counter (`SmolqueryApi.Admission`), then a Bandit listener
  serving `SmolqueryVictoriaMetrics.Router` for this instance. A bare Bandit
  plug rather than a Phoenix endpoint, as `SmolqueryClickHouse.Supervisor`
  has, so a test can run an edge beside the application's own.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias SmolqueryVictoriaMetrics.Runtime

  @doc """
  Starts the edge.

  Takes any `SmolqueryVictoriaMetrics.Runtime` option; application config
  supplies whatever is not passed.
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
      DuckLake.children(runtime.catalog_opts, Runtime.lake_engine(runtime.name)) ++
        [
          {SmolqueryApi.Admission,
           name: runtime.name, limit: SmolqueryApi.Runtime.insert_max_in_flight_bytes(runtime)},
          {Bandit,
           plug: {SmolqueryVictoriaMetrics.Router, runtime.name},
           ip: runtime.ip,
           port: runtime.port,
           startup_log: false,
           thousand_island_options: [supervisor_options: [name: Runtime.listener(runtime.name)]]}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  The address the instance's listener is bound to.

  With a configured port of `0`, this is how a caller learns the port the
  operating system chose.
  """
  @spec bound(atom()) :: {:ok, {:inet.ip_address(), :inet.port_number()}} | :error
  def bound(name), do: ThousandIsland.listener_info(Runtime.listener(name))
end
