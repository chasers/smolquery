defmodule Smolquery.Api.Runtime do
  @moduledoc """
  A running API's resolved configuration.

  The same shape as the other services' runtimes: configuration becomes a
  struct once at boot and lands in `:persistent_term`, so every request reads
  the key and the instance handles for free. Naming derives from one instance
  name, so a test can run an isolated API beside the application's own.

  ## Configuration

      config :smolquery, Smolquery.Api,
        ip: {127, 0, 0, 1},
        port: 4000,
        api_key: "..."

  `api_key` is the one static Bearer key every `/v1` route requires (PL-8 D5).
  There is no default and no fallback: a node holding the `:api` role with no
  key configured refuses to boot rather than serve an open API. Multi-key and
  rotation are explicitly later.

  `ip` defaults to loopback for the same reason — exposing the listener beyond
  the node is a deliberate act (`SMOLQUERY_API_IP`), not a default.

  `catalog` is where the CRUD routes resolve datasets, tables, and schemas —
  the same seam `Smolquery.QueryService` uses. Given options (or nothing), the
  API starts its own `Smolquery.Catalog.DuckLake` engine; given a
  `%Smolquery.Catalog{}` outright, it reads through that and starts nothing.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :api_key, :catalog]
  defstruct [:name, :api_key, :catalog, :catalog_opts, ip: {127, 0, 0, 1}, port: 4000]

  @type t :: %__MODULE__{
          name: atom(),
          api_key: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ip: :inet.socket_address(),
          port: :inet.port_number()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.Api` supplies the defaults; `opts`
  overrides them. Raises if no non-empty `api_key` is present in either.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.Api, []), opts)
    name = Keyword.get(config, :name, Smolquery.Api)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{
      name: name,
      api_key: fetch_api_key!(config),
      catalog: catalog,
      catalog_opts: catalog_opts
    }
    |> struct!(Keyword.take(config, [:ip, :port]))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The Bandit listener's child id under an instance's supervisor.
  """
  @spec listener(atom()) :: atom()
  def listener(name), do: Module.concat(name, "Listener")

  @doc """
  The engine instance CRUD routes resolve the catalog through.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")

  defp fetch_api_key!(config) do
    case Keyword.get(config, :api_key) do
      key when is_binary(key) and key != "" ->
        key

      _absent ->
        raise ArgumentError,
              "the API refuses to boot without an API key: set SMOLQUERY_API_KEY " <>
                "(or config :smolquery, Smolquery.Api, api_key: ...) on every node " <>
                "running the :api role"
    end
  end
end
