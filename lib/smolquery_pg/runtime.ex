defmodule SmolqueryPg.Runtime do
  @moduledoc """
  A running Postgres wire edge's resolved configuration (PL-58).

  The same shape as `SmolqueryApi.Runtime`: configuration becomes a struct
  once at boot and lands in `:persistent_term`, so every connection reads the
  password and the query-service name for free. Naming derives from one
  instance name, so a test can run an isolated edge beside the application's
  own.

  ## Configuration

      config :smolquery, SmolqueryPg,
        password: "...",
        ip: {127, 0, 0, 1},
        port: 5432

  `password` is what every client must present at startup. It defaults to
  the API key (`config :smolquery, SmolqueryApi, api_key: ...`), so one
  credential opens both front doors. `SMOLQUERY_PG_PASSWORD` sets a separate
  one. There is no fallback past that: a node holding the `:pg` role with
  neither configured refuses to boot rather than serve an open listener.

  The listener binds loopback by default. Layer 1 authenticates with a
  cleartext password, so exposing the port beyond the node is a deliberate
  act that belongs behind TLS (`SmolqueryPg` layer 5) or a TLS terminator.

  `query_name` is the `Smolquery.QueryService` instance every `SELECT` runs
  through.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :password]
  @derive {Inspect, except: [:password]}
  defstruct [
    :name,
    :password,
    :catalog,
    :catalog_opts,
    query_name: Smolquery.QueryService,
    ip: {127, 0, 0, 1},
    port: 5432
  ]

  @type t :: %__MODULE__{
          name: atom(),
          password: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          query_name: atom(),
          ip: :inet.ip_address(),
          port: :inet.port_number()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryPg` supplies the defaults; `opts`
  overrides them. Raises if no non-empty password is present in either, or
  as the API key.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config =
      Application.get_env(:smolquery, SmolqueryPg, [])
      |> Keyword.merge(opts)
      |> Keyword.put_new_lazy(:password, &api_key/0)

    name = Keyword.get(config, :name, SmolqueryPg)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), lake_engine(name))

    %__MODULE__{
      name: name,
      catalog: catalog,
      catalog_opts: catalog_opts,
      password:
        Smolquery.Runtime.fetch_required!(config, :password,
          service: "the Postgres wire edge",
          missing: "a password",
          env_var: "SMOLQUERY_PG_PASSWORD (or SMOLQUERY_API_KEY)",
          scope: SmolqueryPg,
          role: :pg
        )
    }
    |> struct!(Keyword.take(config, [:query_name, :ip, :port]))
  end

  defp api_key, do: Keyword.get(Application.get_env(:smolquery, SmolqueryApi, []), :api_key)

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The `ThousandIsland` server for an instance, as its supervisor is named.
  """
  @spec listener(atom()) :: atom()
  def listener(name), do: Module.concat(name, "Listener")

  @doc """
  The registry that maps a session's backend key to the job it is running,
  for `CancelRequest`.
  """
  @spec cancels(atom()) :: atom()
  def cancels(name), do: Module.concat(name, "Cancels")

  @doc """
  The `SmolqueryPg.PgCatalog` server for an instance.
  """
  @spec pg_catalog(atom()) :: atom()
  def pg_catalog(name), do: Module.concat(name, "PgCatalog")

  @doc """
  The DuckDB engine the catalog emulation runs in.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "CatalogEngine")

  @doc """
  The engine a runtime-owned `Smolquery.Catalog.DuckLake` reads through.
  """
  @spec lake_engine(atom()) :: atom()
  def lake_engine(name), do: Module.concat(name, "Lake")
end
