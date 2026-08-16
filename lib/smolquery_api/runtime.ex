defmodule SmolqueryApi.Runtime do
  @moduledoc """
  A running API's resolved configuration.

  The same shape as the other services' runtimes: configuration becomes a
  struct once at boot and lands in `:persistent_term`, so every request reads
  the key and the instance handles for free. Naming derives from one instance
  name, so a test can run an isolated API beside the application's own.

  ## Configuration

      config :smolquery, SmolqueryApi,
        auth_mode: :static,
        api_key: "..."

  `auth_mode: :static` explicitly selects the static Bearer-key adapter, while
  `:oidc` validates and starts the OIDC provider cache. There is no default or
  fallback: a node holding the `:api` role with missing mode or OIDC settings
  refuses to boot rather than serve an open API. Request token verification is
  added by T-232.

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

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Mode
  alias Smolquery.Auth.OIDC
  alias Smolquery.Auth.OIDC.Config, as: OIDCConfig
  alias Smolquery.Auth.Static
  alias Smolquery.Catalog

  @enforce_keys [:name, :auth_mode, :api_key, :context, :catalog, :oidc]
  @derive {Inspect, except: [:api_key, :oidc]}
  defstruct [
    :name,
    :auth_mode,
    :api_key,
    :context,
    :catalog,
    :catalog_opts,
    :oidc,
    :oidc_provider_http_client,
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService,
    load_max_bytes: 268_435_456,
    insert_max_in_flight_bytes: nil
  ]

  @type t :: %__MODULE__{
          name: atom(),
          auth_mode: :static | :oidc,
          api_key: String.t() | nil,
          context: Context.t() | nil,
          catalog: Catalog.t(),
          oidc: OIDCConfig.t() | nil,
          oidc_provider_http_client: Smolquery.Auth.OIDC.Discovery.http_client() | nil,
          catalog_opts: keyword() | nil,
          ingest_name: atom(),
          query_name: atom(),
          load_max_bytes: pos_integer(),
          insert_max_in_flight_bytes: pos_integer() | nil
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryApi` supplies the defaults; `opts`
  overrides them. Raises if the authentication mode is missing, or if static
  mode has no non-empty `api_key`. OIDC mode validates its provider foundation
  and verifies API bearer tokens through the supervised provider cache; per-route
  capability checks are added by T-233.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, SmolqueryApi, []), opts)
    name = Keyword.get(config, :name, SmolqueryApi)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    auth_mode = Mode.runtime_mode!(config, "the API", :api)

    {api_key, context, oidc, oidc_provider_http_client} =
      case auth_mode do
        :static ->
          key =
            Smolquery.Runtime.fetch_required!(config, :api_key,
              service: "the API",
              missing: "an API key",
              env_var: "SMOLQUERY_API_KEY",
              scope: SmolqueryApi,
              role: :api
            )

          {key, Static.api_context(), nil, nil}

        :oidc ->
          {nil, nil, OIDCConfig.new(config, :api), OIDC.provider_http_client!(config)}
      end

    %__MODULE__{
      name: name,
      auth_mode: auth_mode,
      api_key: api_key,
      context: context,
      oidc: oidc,
      oidc_provider_http_client: oidc_provider_http_client,
      catalog: catalog,
      catalog_opts: catalog_opts
    }
    |> struct!(
      Keyword.take(config, [
        :ingest_name,
        :query_name,
        :load_max_bytes,
        :insert_max_in_flight_bytes
      ])
    )
    |> validate_insert_max_in_flight_bytes()
  end

  defp validate_insert_max_in_flight_bytes(
         %__MODULE__{insert_max_in_flight_bytes: bytes} = runtime
       )
       when (is_integer(bytes) and bytes > 0) or is_nil(bytes),
       do: runtime

  defp validate_insert_max_in_flight_bytes(%__MODULE__{insert_max_in_flight_bytes: bytes}) do
    raise ArgumentError,
          "unsupported insert_max_in_flight_bytes: #{inspect(bytes)} " <>
            "(expected a positive integer, or nil to derive it from the " <>
            "container's cgroup memory limit)"
  end

  @in_flight_fallback 268_435_456

  @doc """
  The most ingest-body bytes `SmolqueryApi.Admission` admits at once (T-245).

  An explicit `insert_max_in_flight_bytes` wins. Left `nil`, the limit
  derives as a quarter of the container's cgroup memory limit — in-flight
  bodies are resident heap, and the write path needs the rest of the budget
  for encode buffers and the accumulators — floored at one NDJSON body so a
  small container still ingests. Without a cgroup limit the fallback is
  #{@in_flight_fallback} bytes.
  """
  @spec insert_max_in_flight_bytes(t(), {:ok, pos_integer()} | :none) :: pos_integer()
  def insert_max_in_flight_bytes(runtime, cgroup \\ Smolquery.CgroupMemory.limit_bytes())

  def insert_max_in_flight_bytes(%__MODULE__{insert_max_in_flight_bytes: bytes}, _cgroup)
      when is_integer(bytes),
      do: bytes

  def insert_max_in_flight_bytes(%__MODULE__{}, {:ok, bytes}),
    do: max(div(bytes, 4), SmolqueryApi.InsertController.max_ndjson_bytes())

  def insert_max_in_flight_bytes(%__MODULE__{}, :none), do: @in_flight_fallback

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
