defmodule SmolqueryWeb.Runtime do
  @moduledoc """
  A running web UI's resolved configuration.

  The same shape as `SmolqueryApi.Runtime`: configuration becomes a struct
  once at boot and lands in `:persistent_term`, so every LiveView mount reads
  its handles for free.

  ## Configuration

      config :smolquery, SmolqueryWeb,
        catalog: [metadata: "sqlite:...", data_path: "..."]

  `catalog` is where the UI resolves datasets, tables, and schemas — the same
  seam `SmolqueryApi` uses. Given options (or nothing), the UI starts its own
  `Smolquery.Catalog.DuckLake` engine; given a `%Smolquery.Catalog{}` outright,
  it reads through that and starts nothing.

  The endpoint's listener (ip, port, secret) is Phoenix's own concern and lives
  under `config :smolquery, SmolqueryWeb.Endpoint`.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :catalog]
  defstruct [
    :name,
    :catalog,
    :catalog_opts,
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService
  ]

  @type t :: %__MODULE__{
          name: atom(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ingest_name: atom(),
          query_name: atom()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryWeb` supplies the defaults; `opts`
  overrides them.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, SmolqueryWeb, []), opts)
    name = Keyword.get(config, :name, SmolqueryWeb)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{name: name, catalog: catalog, catalog_opts: catalog_opts}
    |> struct!(Keyword.take(config, [:ingest_name, :query_name]))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The engine instance the UI resolves the catalog through.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")
end
