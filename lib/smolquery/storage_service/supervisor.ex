defmodule Smolquery.StorageService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:storage` role.

  Started only on nodes whose roles include `:storage` (see `Smolquery.Roles`).
  Holds the sealed tier's workers: an engine to merge through, a task supervisor
  the merges run under, and the sealer that answers seal signals.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. A seal attempt merges through the engine and runs as a task, so both must
  be up before the sealer accepts a signal; the sealer crashing disturbs neither.
  Losing the engine restarts the sealer too, which is what abandons in-flight
  attempts — safe, because a level-triggered re-signal brings every unsealed table
  back and a claim fixes the input set.

  Nothing here holds durable state. The catalog and the sealed store do, which is
  what makes a storage node disposable: it can die mid-seal and another node (or
  this one, restarted) reconciles from what the catalog says.

  `GC` joins this subtree in Milestone 4 L5.
  """

  use Supervisor

  alias Smolquery.Engine
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer

  @doc """
  Starts the storage service.

  Takes any `Smolquery.StorageService.Runtime` option; application config supplies
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
      {Engine, name: Runtime.engine(runtime.name), extensions: runtime.engine_extensions},
      {Task.Supervisor, name: Runtime.seals(runtime.name)},
      {Sealer, runtime}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
