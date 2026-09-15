defmodule SmolqueryClickHouse.Supervisor do
  @moduledoc """
  Top-level subtree for the `:clickhouse` role (T-477).

  Started only on nodes whose roles include `:clickhouse` (see
  `Smolquery.Roles`). Resolving the runtime happens in `start_link/1`, so a
  node missing its password fails the boot right here — fail closed — rather
  than starting a listener that would wave inserts through.

  The children, `rest_for_one`: the edge's own ingest admission counter
  (`SmolqueryApi.Admission`), then a Bandit listener serving
  `SmolqueryClickHouse.Router` for this instance. A bare Bandit plug rather
  than a Phoenix endpoint, because an endpoint is a singleton and the
  instance name is what lets a test run an edge beside the application's
  own, as `SmolqueryPg.Supervisor` does with its `ThousandIsland` server.
  """

  use Supervisor

  alias SmolqueryClickHouse.Runtime

  @doc """
  Starts the edge.

  Takes any `SmolqueryClickHouse.Runtime` option; application config
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

    children = [
      {SmolqueryApi.Admission,
       name: runtime.name, limit: SmolqueryApi.Runtime.insert_max_in_flight_bytes(runtime)},
      {Bandit,
       plug: {SmolqueryClickHouse.Router, runtime.name},
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
