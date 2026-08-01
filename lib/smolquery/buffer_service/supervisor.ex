defmodule Smolquery.BufferService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:buffer` role.

  Started only on nodes whose roles include `:buffer` (see `Smolquery.Roles`).
  Holds the hot tier: the manifest index, the table-to-buffer registry, and the
  partitioned supervisor that `TableBuffer` processes start under on demand.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. The manifest owns the ETS index every buffer writes into, so if it dies the
  registry and the buffers that hold stale handles must go with it — and each
  buffer rebuilds its table's entries from the log when it restarts. A single
  buffer crashing, by contrast, disturbs nothing else.

  `Adopter` comes next, once the pieces it needs are up: it starts a buffer for
  every owned table that already has a manifest log, so an unsealed tail is not
  stranded waiting for a write that may never come.

  `HotServer` is last — it only reads the manifest and the store, so nothing
  beneath it depends on it being up.
  """

  use Supervisor

  alias Smolquery.BufferService.Adopter
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Runtime

  @doc """
  Starts the buffer service.

  Takes any `Smolquery.BufferService.Runtime` option; application config supplies
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

    children = [
      {HotManifest, name: Runtime.manifest(runtime.name)},
      {Registry, keys: :unique, name: Runtime.registry(runtime.name)},
      {PartitionSupervisor, child_spec: DynamicSupervisor, name: Runtime.buffers(runtime.name)},
      {Adopter, runtime},
      Supervisor.child_spec(
        {Bandit,
         plug: {HotServer, runtime.name},
         ip: runtime.hot_server_ip,
         port: runtime.hot_server_port,
         startup_log: false},
        id: Runtime.hot_server(runtime.name)
      )
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
