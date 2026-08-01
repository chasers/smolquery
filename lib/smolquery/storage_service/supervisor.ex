defmodule Smolquery.StorageService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:storage` role.

  Started only on nodes whose roles include `:storage` (see `Smolquery.Roles`).
  Holds the sealed tier's workers: a catalog to commit through, an engine to merge
  through, a task supervisor the merges run under, the sealer that answers seal
  signals, the compactor that re-merges undersized sealed segments, the retention
  sweeper that drops segments past their table's TTL and expires old snapshots,
  and the GC that deletes uploads whose commit never followed.

  The catalog and the merge run on separate engines deliberately. An
  `Adbc.Connection` serializes the queries it is given, so sharing one would put
  every catalog commit behind whatever multi-gigabyte `COPY` happened to be in
  flight.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. A seal attempt commits through the catalog and merges through the engine and
  runs as a task, so all three must be up before the sealer accepts a signal; the
  sealer crashing disturbs none of them. Losing the catalog restarts everything
  above it, which is what abandons in-flight attempts — safe, because a
  level-triggered re-signal brings every unsealed table back and a claim fixes the
  input set.

  A deployment that commits through a catalog it manages elsewhere passes a
  `%Smolquery.Catalog{}` in configuration, and then this subtree starts none.

  Nothing here holds durable state. The catalog and the sealed store do, which is
  what makes a storage node disposable: it can die mid-seal and another node (or
  this one, restarted) reconciles from what the catalog says.

  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.StorageService.Compactor
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Retention
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

    children =
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        [
          {Engine,
           name: Runtime.engine(runtime.name),
           extensions: runtime.engine_extensions,
           statements: hot_tier_secret(runtime)},
          {Task.Supervisor, name: Runtime.seals(runtime.name)},
          {Sealer, runtime},
          {Compactor, runtime},
          {Retention, runtime},
          {GC, runtime}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp hot_tier_secret(%Runtime{} = runtime) do
    if :httpfs in runtime.engine_extensions do
      [Smolquery.InternalSecret.create_secret_statement(runtime.buffer_base_url)]
    else
      []
    end
  end
end
