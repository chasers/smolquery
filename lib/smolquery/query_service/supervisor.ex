defmodule Smolquery.QueryService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:query` role.

  Started only on nodes whose roles include `:query` (see `Smolquery.Roles`).
  Holds what every job needs and no job owns: the catalog engine plans resolve
  through (snapshot pins, segment lists, table schemas), the registry runners
  are looked up in by job id, and the partitioned dynamic supervisor they start
  under. The runners themselves — each job a process with its own private
  engine — arrive with Milestone 5's later layers.

  The catalog engine is deliberately not an engine queries run on. An
  `Adbc.Connection` serializes the queries it is given, so planning must never
  queue behind whatever ten-second scan happens to be in flight; each job
  brings its own engine and takes it down with it.

  The strategy is `rest_for_one`, catalog first, because the dependency runs
  one way: a runner plans through the catalog, so losing the catalog restarts
  everything that depends on it — safe, a query is a retryable read — while a
  crashed runner subtree disturbs the catalog not at all.

  A deployment that manages the catalog elsewhere passes a
  `%Smolquery.Catalog{}` in configuration, and then this subtree starts none.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService.History
  alias Smolquery.QueryService.Runtime

  @doc """
  Starts the query service.

  Takes any `Smolquery.QueryService.Runtime` option; application config
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
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        [
          {Registry, keys: :unique, name: Runtime.registry(runtime.name)},
          {PartitionSupervisor,
           child_spec: DynamicSupervisor, name: Runtime.runners(runtime.name)}
        ] ++ history(runtime)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp history(%Runtime{history_metadata: nil}), do: []
  defp history(%Runtime{} = runtime), do: [{History, runtime}]
end
