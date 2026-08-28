defmodule SmolqueryPg.Supervisor do
  @moduledoc """
  Top-level subtree for the `:pg` role (PL-58).

  Started only on nodes whose roles include `:pg` (see `Smolquery.Roles`).
  Resolving the runtime happens in `start_link/1`, so a node missing its
  password fails the boot right here — fail closed — rather than starting a
  listener that would wave connections through.

  The children, `rest_for_one`: a catalog engine when the runtime owns one,
  the cancel registry (`SmolqueryPg.Runtime.cancels/1`), the `pg_catalog`
  emulation (`SmolqueryPg.PgCatalog`), then a `ThousandIsland` server whose
  `SmolqueryPg.Handler` owns each connection. The read timeout is infinite on purpose: an idle `psql`
  session holds its connection open for hours, and Thousand Island's
  default would close it after a minute of silence.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias SmolqueryPg.Runtime

  @doc """
  Starts the edge.

  Takes any `SmolqueryPg.Runtime` option; application config supplies
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
      DuckLake.children(runtime.catalog_opts, Runtime.lake_engine(runtime.name)) ++
        [
          {Registry, keys: :unique, name: Runtime.cancels(runtime.name)},
          {SmolqueryPg.PgCatalog, runtime},
          {ThousandIsland,
           port: runtime.port,
           transport_options: [ip: runtime.ip],
           handler_module: SmolqueryPg.Handler,
           handler_options: runtime,
           read_timeout: :infinity,
           supervisor_options: [name: Runtime.listener(runtime.name)]}
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
