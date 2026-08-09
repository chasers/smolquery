defmodule Smolquery.IngestService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:ingest` role.

  Started only on nodes whose roles include `:ingest` (see `Smolquery.Roles`).
  The service is deliberately small — the ingest edge is stateless, so the
  subtree is a catalog engine (when the runtime resolved one) and the schema
  cache that reads through it. `rest_for_one`, catalog first, because the
  cache is only as good as the catalog behind it.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.IngestService.Runtime
  alias Smolquery.IngestService.SchemaCache

  @doc """
  Starts the ingest service.

  Takes any `Smolquery.IngestService.Runtime` option; application config
  supplies whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)
    Smolquery.DeployedShape.announce(runtime)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children =
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        [{SchemaCache, runtime}]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
