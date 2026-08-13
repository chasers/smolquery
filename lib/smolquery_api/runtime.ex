defmodule SmolqueryApi.Runtime do
  @moduledoc """
  A running API's resolved configuration.

  The same shape as the other services' runtimes: configuration becomes a
  struct once at boot and lands in `:persistent_term`, so every request reads
  the key and the instance handles for free. Naming derives from one instance
  name, so a test can run an isolated API beside the application's own.

  ## Configuration

      config :smolquery, SmolqueryApi,
        api_key: "..."

  `api_key` is the one static Bearer key every `/v1` route requires (PL-8 D5).
  There is no default and no fallback: a node holding the `:api` role with no
  key configured refuses to boot rather than serve an open API. Multi-key and
  rotation are explicitly later.

  The listener (ip, port) is Phoenix's own concern and lives under
  `config :smolquery, SmolqueryApi.Endpoint` — the same split
  `SmolqueryWeb.Runtime` made. `SMOLQUERY_API_IP`/`SMOLQUERY_API_PORT` still
  land there, and the ip still defaults to loopback: exposing the listener
  beyond the node is a deliberate act, not a default.

  `catalog` is where the CRUD routes resolve datasets, tables, and schemas —
  the same seam `Smolquery.QueryService` uses. Given options (or nothing), the
  API starts its own `Smolquery.Catalog.DuckLake` engine; given a
  `%Smolquery.Catalog{}` outright, it reads through that and starts nothing.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :api_key, :catalog]
  @derive {Inspect, except: [:api_key]}
  defstruct [
    :name,
    :api_key,
    :catalog,
    :catalog_opts,
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService,
    load_max_bytes: 268_435_456
  ]

  @type t :: %__MODULE__{
          name: atom(),
          api_key: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ingest_name: atom(),
          query_name: atom(),
          load_max_bytes: pos_integer()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryApi` supplies the defaults; `opts`
  overrides them. Raises if no non-empty `api_key` is present in either.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, SmolqueryApi, []), opts)
    name = Keyword.get(config, :name, SmolqueryApi)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{
      name: name,
      api_key:
        Smolquery.Runtime.fetch_required!(config, :api_key,
          service: "the API",
          missing: "an API key",
          env_var: "SMOLQUERY_API_KEY",
          scope: SmolqueryApi,
          role: :api
        ),
      catalog: catalog,
      catalog_opts: catalog_opts
    }
    |> struct!(Keyword.take(config, [:ingest_name, :query_name, :load_max_bytes]))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The engine instance CRUD routes resolve the catalog through.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")
end
