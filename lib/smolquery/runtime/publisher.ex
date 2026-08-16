defmodule Smolquery.Runtime.Publisher do
  @moduledoc """
  Publishes a resolved role runtime only while its supervising subtree is alive.

  Placing this child after fail-closed dependencies prevents a role that never
  finished starting from appearing available through its runtime registry.
  """

  use GenServer

  @type runtime_module :: module()
  @type runtime :: %{required(:name) => atom()}

  @spec child_spec({runtime_module(), runtime()}) :: Supervisor.child_spec()
  def child_spec({runtime_module, %{name: name} = runtime}) do
    %{
      id: {__MODULE__, runtime_module, name},
      start: {__MODULE__, :start_link, [{runtime_module, runtime}]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  @spec start_link({runtime_module(), runtime()}) :: GenServer.on_start()
  def start_link({runtime_module, runtime}) do
    GenServer.start_link(__MODULE__, {runtime_module, runtime})
  end

  @impl GenServer
  def init({runtime_module, runtime}) do
    runtime_module.put(runtime)
    {:ok, {runtime_module, runtime.name}}
  end

  @impl GenServer
  def terminate(_reason, {runtime_module, name}) do
    runtime_module.delete(name)
    :ok
  end
end
