defmodule SmolqueryClickHouse.Runtime do
  @moduledoc """
  A running ClickHouse HTTP edge's resolved configuration (T-477).

  The same shape as `SmolqueryPg.Runtime`: configuration becomes a struct
  once at boot and lands in `:persistent_term`, so every request reads the
  password and the service names for free. Naming derives from one
  instance name, so a test can run an isolated edge beside the application's
  own.

  ## Configuration

      config :smolquery, SmolqueryClickHouse,
        password: "...",
        ip: {127, 0, 0, 1},
        port: 8123

  `password` is what every client must present, in any of the forms
  ClickHouse takes (`SmolqueryClickHouse.Auth`). It defaults to the API key
  (`config :smolquery, SmolqueryApi, api_key: ...`), so one credential opens
  every front door. `SMOLQUERY_CLICKHOUSE_PASSWORD` sets a separate one.
  There is no fallback past that: a node holding the `:clickhouse` role with
  neither configured refuses to boot rather than serve an open listener.

  The listener binds loopback by default and speaks plain HTTP. Binding it
  beyond the node belongs behind a TLS terminator.

  `max_ndjson_bytes` and `insert_max_in_flight_bytes` default to the API's
  (`SMOLQUERY_INSERT_MAX_NDJSON_BYTES`, `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES`):
  the largest body an insert reads, and the bytes this edge's own
  `SmolqueryApi.Admission` counter admits at once, derived as
  `SmolqueryApi.Runtime.insert_max_in_flight_bytes/2` derives the API's. A
  node running both the `:api` and `:clickhouse` roles holds two counters,
  each with that limit.

  `ingest_name` is the `Smolquery.IngestService` instance every insert goes
  through, and `query_name` the `Smolquery.QueryService` instance every
  query runs through (T-478).
  """

  @enforce_keys [:name, :password]
  @derive {Inspect, except: [:password]}
  defstruct [
    :name,
    :password,
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService,
    max_ndjson_bytes: 8_000_000,
    insert_max_in_flight_bytes: nil,
    ip: {127, 0, 0, 1},
    port: 8123
  ]

  @type t :: %__MODULE__{
          name: atom(),
          password: String.t(),
          ingest_name: atom(),
          query_name: atom(),
          max_ndjson_bytes: pos_integer(),
          insert_max_in_flight_bytes: pos_integer() | nil,
          ip: :inet.ip_address(),
          port: :inet.port_number()
        }

  @api_defaults [:max_ndjson_bytes, :insert_max_in_flight_bytes]

  @doc """
  Resolves configuration into a runtime.

  The API's body limits, then application config for `SmolqueryClickHouse`,
  then `opts`, each overriding the last. Raises if no non-empty password is
  present in either, or as the API key.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    api = Application.get_env(:smolquery, SmolqueryApi, [])

    config =
      api
      |> Keyword.take(@api_defaults)
      |> Keyword.merge(Application.get_env(:smolquery, SmolqueryClickHouse, []))
      |> Keyword.merge(opts)
      |> Keyword.put_new_lazy(:password, fn -> Keyword.get(api, :api_key) end)

    %__MODULE__{
      name: Keyword.get(config, :name, SmolqueryClickHouse),
      password:
        Smolquery.Runtime.fetch_required!(config, :password,
          service: "the ClickHouse HTTP edge",
          missing: "a password",
          env_var: "SMOLQUERY_CLICKHOUSE_PASSWORD (or SMOLQUERY_API_KEY)",
          scope: SmolqueryClickHouse,
          role: :clickhouse
        )
    }
    |> struct!(Keyword.take(config, [:ingest_name, :query_name, :ip, :port | @api_defaults]))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The `ThousandIsland` server under an instance's Bandit listener.
  """
  @spec listener(atom()) :: atom()
  def listener(name), do: Module.concat(name, "Listener")
end
