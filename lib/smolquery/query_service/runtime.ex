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

  `job_bootstrap` is the SQL each job engine runs before its first query —
  by default, the `ATTACH` that binds the same lake the `catalog`
  configuration names (falling back to the application's
  `Smolquery.Catalog.DuckLake` settings). A deployment passing a
  `%Smolquery.Catalog{}` handle must pass the bootstrap too, or job engines
  see no sealed tier.

  `history_metadata` is where `QueryService.History` durably records terminal
  jobs (PL-8 D8) — by default the same `sqlite:` database the catalog
  configuration names, resolved the same way `job_bootstrap` is: a deployment
  passing a `%Smolquery.Catalog{}` handle gets no history unless it passes
  `history_metadata` too, and `history_metadata: nil` switches history off.

  `lockdown` (PL-8 D7) is whether each job engine, after planning, disables
  DuckDB's external access for the user's SQL — leaving exactly
  `allowed_directories` plus the plan's own micro-segment URLs readable. On
  by default: SQL arriving over the HTTP API must not read arbitrary files.
  `allowed_directories` defaults to the node's `:data_dir` (expanded) plus
  whatever the catalog configuration names; a deployment whose sealed
  segments live elsewhere must say so here, or its queries will honestly
  fail to read them.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :catalog]
  defstruct [
    :name,
    :catalog,
    :catalog_opts,
    :history_metadata,
    :allowed_directories,
    lockdown: true,
    buffer_name: Smolquery.BufferService,
    buffer_base_url: "http://127.0.0.1:4001",
    buffer_timeout_ms: 30_000,
    engine_extensions: [:httpfs],
    job_bootstrap: [],
    max_concurrent_jobs: 8,
    default_timeout_ms: 60_000,
    job_memory_limit: "1GB",
    result_ttl_ms: 300_000
  ]

  @type t :: %__MODULE__{
          name: atom(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          history_metadata: String.t() | nil,
          allowed_directories: [String.t()],
          lockdown: boolean(),
          buffer_name: atom(),
          buffer_base_url: String.t(),
          buffer_timeout_ms: timeout(),
          engine_extensions: [atom() | String.t()],
          job_bootstrap: [String.t()],
          max_concurrent_jobs: pos_integer(),
          default_timeout_ms: pos_integer(),
          job_memory_limit: String.t(),
          result_ttl_ms: pos_integer()
        }

  @limits [
    :lockdown,
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

    %__MODULE__{
      name: name,
      catalog: catalog,
      catalog_opts: catalog_opts,
      job_bootstrap: Keyword.get_lazy(config, :job_bootstrap, fn -> bootstrap(catalog_opts) end),
      history_metadata:
        Keyword.get_lazy(config, :history_metadata, fn -> history_metadata(catalog_opts) end),
      allowed_directories:
        Keyword.get_lazy(config, :allowed_directories, fn -> allowed_directories(catalog_opts) end)
    }
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

  @doc """
  The history store terminal jobs are recorded in.
  """
  @spec history(atom()) :: atom()
  def history(name), do: Module.concat(name, "History")

  defp bootstrap(nil), do: []

  defp bootstrap(catalog_opts) do
    merged =
      Keyword.merge(Application.get_env(:smolquery, Catalog.DuckLake, []), catalog_opts)

    with {:ok, metadata} <- Keyword.fetch(merged, :metadata),
         {:ok, data_path} <- Keyword.fetch(merged, :data_path) do
      lake = Keyword.get(merged, :catalog, Catalog.DuckLake.default_catalog())

      [Catalog.DuckLake.attach_statement(lake, metadata, data_path)]
    else
      :error -> []
    end
  end

  defp history_metadata(nil), do: nil

  defp history_metadata(catalog_opts) do
    merged =
      Keyword.merge(Application.get_env(:smolquery, Catalog.DuckLake, []), catalog_opts)

    case Keyword.get(merged, :metadata) do
      "sqlite:" <> _path = metadata -> metadata
      _absent_or_not_sqlite -> nil
    end
  end

  defp allowed_directories(catalog_opts) do
    merged =
      Keyword.merge(Application.get_env(:smolquery, Catalog.DuckLake, []), catalog_opts || [])

    [
      Application.get_env(:smolquery, :data_dir),
      Keyword.get(merged, :data_path),
      case Keyword.get(merged, :metadata) do
        "sqlite:" <> path -> Path.dirname(path)
        _not_a_file -> nil
      end,
      ".tmp"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&Path.expand/1)
    |> Enum.uniq()
  end
end
