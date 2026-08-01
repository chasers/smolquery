defmodule Smolquery.QueryService.Runtime do
  @moduledoc """
  A running query service's resolved configuration.

  The same shape as `Smolquery.StorageService.Runtime`, and for the same
  reason: configuration becomes handles once at boot and lands in
  `:persistent_term`, so answering "is the query service running here" and
  reading its catalog handle cost nothing per job.

  Naming derives from one instance name, so a test can run an isolated query
  service beside the application's own.

  ## Configuration

      config :smolquery, Smolquery.QueryService,
        buffer_base_url: "http://127.0.0.1:4001",
        buffer_timeout_ms: 30_000,
        engine_extensions: [:httpfs],
        max_concurrent_jobs: 8,
        default_timeout_ms: 60_000,
        job_memory_limit: "1GB",
        result_ttl_ms: 300_000

  `catalog` is where plans resolve tables: snapshot pins, segment lists,
  schemas. Given options (or nothing), the service starts its own
  `Smolquery.Catalog.DuckLake` engine and reads through it; given a
  `%Smolquery.Catalog{}` outright, it reads through that and starts nothing:

      catalog: [metadata: "postgres:dbname=smolquery", data_path: "/mnt/bulk/lake"]

  `buffer_base_url` is where the planner reaches `BufferService.HotServer` for
  a table's hot manifest, and where the job engines it plans for read
  micro-segment bytes. Configuration is honest for a single-node deployment; a
  cluster resolves the owning node from the ownership ring instead, which
  arrives with Milestone 8.

  `buffer_name` is the buffer service instance ownership questions go to —
  `BufferService.Client.owner/2` answers even on a node running no buffer.

  `engine_extensions` are loaded into each job's private engine. `httpfs` is
  not optional in a real deployment — hot-tier micro-segments are read over
  HTTP — so it is the default rather than something a deployment has to
  remember. Tests that never touch the hot tier set it to `[]` and skip the
  extension download.

  `max_concurrent_jobs` bounds jobs in flight on this node; a submission past
  the bound is refused, not queued invisibly. `default_timeout_ms` is the sync
  path's wait and every job's runtime ceiling unless the caller says
  otherwise. `job_memory_limit` is handed to each job engine's DuckDB
  `memory_limit`. `result_ttl_ms` is how long a finished job holds its result
  frame for an async caller to fetch.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :catalog]
  defstruct [
    :name,
    :catalog,
    :catalog_opts,
    buffer_name: Smolquery.BufferService,
    buffer_base_url: "http://127.0.0.1:4001",
    buffer_timeout_ms: 30_000,
    engine_extensions: [:httpfs],
    max_concurrent_jobs: 8,
    default_timeout_ms: 60_000,
    job_memory_limit: "1GB",
    result_ttl_ms: 300_000
  ]

  @type t :: %__MODULE__{
          name: atom(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          buffer_name: atom(),
          buffer_base_url: String.t(),
          buffer_timeout_ms: timeout(),
          engine_extensions: [atom() | String.t()],
          max_concurrent_jobs: pos_integer(),
          default_timeout_ms: pos_integer(),
          job_memory_limit: String.t(),
          result_ttl_ms: pos_integer()
        }

  @limits [
    :buffer_name,
    :buffer_base_url,
    :buffer_timeout_ms,
    :engine_extensions,
    :max_concurrent_jobs,
    :default_timeout_ms,
    :job_memory_limit,
    :result_ttl_ms
  ]

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.QueryService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.QueryService, []), opts)
    name = Keyword.get(config, :name, Smolquery.QueryService)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{name: name, catalog: catalog, catalog_opts: catalog_opts}
    |> struct!(Keyword.take(config, @limits))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The engine instance plans resolve through.

  Separate from the job engines because an `Adbc.Connection` serializes its
  queries: a snapshot pin waiting behind someone's ten-second scan would make
  planning as slow as the largest query in flight.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")

  @doc """
  The registry job runners are looked up in by job id.
  """
  @spec registry(atom()) :: atom()
  def registry(name), do: Module.concat(name, "Registry")

  @doc """
  The partitioned dynamic supervisor job runners start under.
  """
  @spec runners(atom()) :: atom()
  def runners(name), do: Module.concat(name, "Runners")
end
