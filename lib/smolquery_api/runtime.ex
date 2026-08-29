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

  `max_ndjson_bytes` caps a `POST .../insert` body. A larger body is a 413.
  The cap is also what `SmolqueryApi.Admission` reserves when a request
  declares no `content-length`. `SMOLQUERY_INSERT_MAX_NDJSON_BYTES` sets it
  in a release.

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
    max_ndjson_bytes: 8_000_000,
    insert_max_in_flight_bytes: nil
  ]

  @type t :: %__MODULE__{
          name: atom(),
          api_key: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ingest_name: atom(),
          query_name: atom(),
          max_ndjson_bytes: pos_integer(),
          insert_max_in_flight_bytes: pos_integer() | nil
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
    |> struct!(
      Keyword.take(config, [
        :ingest_name,
        :query_name,
        :max_ndjson_bytes,
        :insert_max_in_flight_bytes
      ])
    )
    |> validate_max_ndjson_bytes()
    |> validate_insert_max_in_flight_bytes()
  end

  defp validate_max_ndjson_bytes(%__MODULE__{max_ndjson_bytes: bytes} = runtime)
       when is_integer(bytes) and bytes > 0,
       do: runtime

  defp validate_max_ndjson_bytes(%__MODULE__{max_ndjson_bytes: bytes}) do
    raise ArgumentError,
          "unsupported max_ndjson_bytes: #{inspect(bytes)} (expected a positive integer)"
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
  for encode buffers and the accumulators — floored at `max_ndjson_bytes` so a
  small container still ingests. Without a cgroup limit the fallback is
  #{@in_flight_fallback} bytes.
  """
  @spec insert_max_in_flight_bytes(t(), {:ok, pos_integer()} | :none) :: pos_integer()
  def insert_max_in_flight_bytes(runtime, cgroup \\ Smolquery.CgroupMemory.limit_bytes())

  def insert_max_in_flight_bytes(%__MODULE__{insert_max_in_flight_bytes: bytes}, _cgroup)
      when is_integer(bytes),
      do: bytes

  def insert_max_in_flight_bytes(%__MODULE__{max_ndjson_bytes: ndjson}, {:ok, bytes}),
    do: max(div(bytes, 4), ndjson)

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
