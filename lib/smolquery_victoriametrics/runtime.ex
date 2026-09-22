defmodule SmolqueryVictoriaMetrics.Runtime do
  @moduledoc """
  A running VictoriaMetrics edge's resolved configuration (PL-70, T-562).

  The same shape as `SmolqueryClickHouse.Runtime`: configuration becomes a
  struct once at boot and lands in `:persistent_term`, so every request reads
  the password, the table and the service names for free. Naming derives from
  one instance name, so a test can run an isolated edge beside the
  application's own.

  ## Configuration

      config :smolquery, SmolqueryVictoriaMetrics,
        password: "...",
        ip: {127, 0, 0, 1},
        port: 8428,
        table: "metrics.samples",
        lookback_ms: 300_000,
        max_series: 10_000,
        max_samples: 20_000_000,
        max_points_per_series: 30_000

  `password` is what every client must present, as a `Bearer` token or HTTP
  basic auth (`SmolqueryVictoriaMetrics.Auth`). It defaults to the API key
  (`config :smolquery, SmolqueryApi, api_key: ...`), so one credential opens
  every front door. `SMOLQUERY_VICTORIAMETRICS_PASSWORD` sets a separate one.
  There is no fallback past that: a node holding the `:victoriametrics` role
  with neither configured refuses to boot rather than serve an open listener.

  The listener binds loopback by default and speaks plain HTTP. Binding it
  beyond the node belongs behind a TLS terminator.

  `table` is where every sample lands, `{dataset, table}`, given as that
  tuple or as the text `dataset.table` (`SMOLQUERY_VICTORIAMETRICS_TABLE`,
  `metrics.samples`). Both names must be identifiers
  (`Smolquery.Identifier`); anything else fails the boot. The edge creates the
  dataset and the table on its first write when they are missing
  (`SmolqueryVictoriaMetrics.Write`).

  `lookback_ms` is how far back an instant selector looks for a sample
  (`SMOLQUERY_VICTORIAMETRICS_LOOKBACK_MS`, `300_000`): the lookback of
  MetricsQL's `default_rollup`, used as `max(step, lookback_ms)`.

  `max_series`, `max_samples` and `max_points_per_series` are the ceilings on
  one query (`SMOLQUERY_VICTORIAMETRICS_MAX_SERIES`, `_MAX_SAMPLES`,
  `_MAX_POINTS_PER_SERIES`; `10_000`, `20_000_000`, `30_000`): the series one
  selector may match, the raw samples one selector may read into the BEAM,
  and the points of one query's step grid, which is VictoriaMetrics'
  `-search.maxPointsPerTimeseries`. A query past any of them is refused
  rather than run (`SmolqueryVictoriaMetrics.Query`).

  `max_ndjson_bytes` and `insert_max_in_flight_bytes` default to the API's
  (`SMOLQUERY_INSERT_MAX_NDJSON_BYTES`, `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES`):
  the largest body a write reads, compressed or not, and the bytes this edge's
  own `SmolqueryApi.Admission` counter admits at once, derived as
  `SmolqueryApi.Runtime.insert_max_in_flight_bytes/2` derives the API's.

  `catalog` is what the edge creates its table through: a `Smolquery.Catalog`
  given outright, or the options of a lake the edge reads through its own
  engine, as the ClickHouse edge's is.

  `ingest_name` is the `Smolquery.IngestService` instance every write goes
  through, and `query_name` the `Smolquery.QueryService` instance every query
  runs through.
  """

  alias Smolquery.Catalog
  alias Smolquery.Identifier

  @enforce_keys [:name, :password]
  @derive {Inspect, except: [:password]}
  defstruct [
    :name,
    :password,
    :catalog,
    :catalog_opts,
    table: {"metrics", "samples"},
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService,
    max_ndjson_bytes: 8_000_000,
    insert_max_in_flight_bytes: nil,
    lookback_ms: 300_000,
    max_series: 10_000,
    max_samples: 20_000_000,
    max_points_per_series: 30_000,
    ip: {127, 0, 0, 1},
    port: 8428
  ]

  @type t :: %__MODULE__{
          name: atom(),
          password: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          table: {String.t(), String.t()},
          ingest_name: atom(),
          query_name: atom(),
          max_ndjson_bytes: pos_integer(),
          insert_max_in_flight_bytes: pos_integer() | nil,
          lookback_ms: pos_integer(),
          max_series: pos_integer(),
          max_samples: pos_integer(),
          max_points_per_series: pos_integer(),
          ip: :inet.ip_address(),
          port: :inet.port_number()
        }

  @api_defaults [:max_ndjson_bytes, :insert_max_in_flight_bytes]

  @doc """
  Resolves configuration into a runtime.

  The API's body limits, then application config for
  `SmolqueryVictoriaMetrics`, then `opts`, each overriding the last. Raises if
  no non-empty password is present in either, or as the API key, and if
  `table` does not name a dataset and a table.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    api = Application.get_env(:smolquery, SmolqueryApi, [])

    config =
      api
      |> Keyword.take(@api_defaults)
      |> Keyword.merge(Application.get_env(:smolquery, SmolqueryVictoriaMetrics, []))
      |> Keyword.merge(opts)
      |> Keyword.put_new_lazy(:password, fn -> Keyword.get(api, :api_key) end)

    name = Keyword.get(config, :name, SmolqueryVictoriaMetrics)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), lake_engine(name))

    %__MODULE__{
      name: name,
      catalog: catalog,
      catalog_opts: catalog_opts,
      password:
        Smolquery.Runtime.fetch_required!(config, :password,
          service: "the VictoriaMetrics edge",
          missing: "a password",
          env_var: "SMOLQUERY_VICTORIAMETRICS_PASSWORD (or SMOLQUERY_API_KEY)",
          scope: SmolqueryVictoriaMetrics,
          role: :victoriametrics
        )
    }
    |> struct!(
      Keyword.take(config, [
        :ingest_name,
        :query_name,
        :lookback_ms,
        :max_series,
        :max_samples,
        :max_points_per_series,
        :ip,
        :port | @api_defaults
      ])
    )
    |> table(Keyword.get(config, :table))
  end

  defp table(runtime, nil), do: runtime

  defp table(runtime, table) do
    case parse_table(table) do
      {:ok, ref} ->
        %{runtime | table: ref}

      {:error, {:invalid_table, given}} ->
        raise ArgumentError,
              "the VictoriaMetrics edge refuses to boot: SMOLQUERY_VICTORIAMETRICS_TABLE " <>
                "(or config :smolquery, SmolqueryVictoriaMetrics, table: ...) must be " <>
                "dataset.table, two identifiers, got: #{inspect(given)}"
    end
  end

  @doc """
  Reads a table setting: `{dataset, table}`, or the text `dataset.table`.
  Both names must be identifiers (`Smolquery.Identifier.valid?/1`).
  """
  @spec parse_table(term()) :: {:ok, {String.t(), String.t()}} | {:error, term()}
  def parse_table({dataset, table}) do
    if Identifier.valid?(dataset) and Identifier.valid?(table),
      do: {:ok, {dataset, table}},
      else: {:error, {:invalid_table, {dataset, table}}}
  end

  def parse_table(text) when is_binary(text) do
    case String.split(text, ".") do
      [dataset, table] -> parse_table({dataset, table})
      _other -> {:error, {:invalid_table, text}}
    end
  end

  def parse_table(other), do: {:error, {:invalid_table, other}}

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The engine a runtime-owned `Smolquery.Catalog.DuckLake` reads through.
  """
  @spec lake_engine(atom()) :: atom()
  def lake_engine(name), do: Module.concat(name, "Lake")

  @doc """
  The `ThousandIsland` server under an instance's Bandit listener.
  """
  @spec listener(atom()) :: atom()
  def listener(name), do: Module.concat(name, "Listener")
end
