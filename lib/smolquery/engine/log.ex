defmodule Smolquery.Engine.Log do
  @moduledoc """
  DuckDB's own log for one engine, switched on at boot or at runtime, and
  drained into the application's log (T-599).

  Where a `metrics.samples` swap spent 120 s was answered by DuckDB's log,
  turned on by hand from a remote console: it timestamped every statement
  the swap ran, DuckLake's internal ones included (T-598). This makes that a
  switch. `Smolquery.Engine.log/3` turns it on for one engine for a set
  time, without a restart; a restart would change what is being measured,
  since the compactor's backoffs and row caps live in memory.
  `SMOLQUERY_DUCKDB_LOG` turns it on at boot for the storage engines it
  names (`boot_engines/2`).

  ## One engine, drained

  `enable_logging` is per DuckDB instance, so it covers every connection of
  the engine it runs on, and each row carries its `connection_id`. Logs are
  kept in memory, so this process reads `duckdb_logs` every
  `:interval_ms`, writes each row to `Logger`, its line naming the engine and the
  connection, transaction and query ids, and truncates. At most `:max_rows` rows leave
  per drain; the rest are counted in a warning and dropped with the
  truncation, so a burst cannot flood the log pipeline. Rows written between
  a drain's read and its truncation are lost; a log for diagnosis, not an
  audit trail. The drain's own statements are left out. When the engine's
  instance is rebuilt, the next drain turns logging on again on the new one.

  ## Nothing secret leaves

  DuckDB logs what it runs verbatim, and two things it runs carry
  credentials:

    * an `HTTP` row records each request's headers, including the S3
      `Authorization` signature and the temporary `x-amz-security-token`.
      An `HTTP` row is therefore rebuilt from an allowlist, the method, the
      URL without its query string, the range, the status and the duration,
      and nothing else of it is logged;
    * a `QueryLog` row of a `CREATE SECRET` holds the key, the secret and the
      session token, and `Smolquery.EngineSecrets` runs one per engine. Such
      a row is logged as `CREATE SECRET <redacted>`.

  `HTTP` is never on unless asked for by name, and `redact/2` is applied to
  every row either way.
  """

  use GenServer

  require Logger

  alias Smolquery.Engine

  @interval_ms 5_000
  @max_rows 1_000
  @roles %{"catalog" => :catalog, "merge" => :merge, "compact" => :compact}

  @type option ::
          {:engine, atom()}
          | {:types, [String.t()]}
          | {:for_ms, pos_integer() | :infinity}
          | {:interval_ms, pos_integer()}
          | {:max_rows, pos_integer()}

  @doc """
  Turns DuckDB logging on for `engine` and drains it, under
  `Smolquery.Engine.LogSupervisor`, replacing any drain the engine already
  has. Turned off again after `:for_ms`.
  """
  @spec start(atom(), [String.t()], [option()]) :: DynamicSupervisor.on_start_child()
  def start(engine, types, opts \\ []) do
    :ok = stop(engine)

    DynamicSupervisor.start_child(
      Smolquery.Engine.LogSupervisor,
      Supervisor.child_spec({__MODULE__, [engine: engine, types: types] ++ opts},
        restart: :temporary
      )
    )
  end

  @doc "Stops `engine`'s drain, turning its logging off, if it has one."
  @spec stop(atom()) :: :ok
  def stop(engine) do
    case Process.whereis(name(engine)) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  @doc false
  def child_spec(opts),
    do: %{id: name(Keyword.fetch!(opts, :engine)), start: {__MODULE__, :start_link, [opts]}}

  @doc "Starts a drain for `:engine`, logging `:types`."
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :engine)

    GenServer.start_link(__MODULE__, opts, name: name(engine))
  end

  @doc """
  The drains `SMOLQUERY_DUCKDB_LOG` asks for, as `{role, types}`: a
  comma-separated list of `role:Type`, role one of `catalog`, `merge` or
  `compact`. Raises on anything else, at boot.

      iex> Smolquery.Engine.Log.parse("catalog:QueryLog,compact:QueryLog,catalog:HTTP")
      [catalog: ["QueryLog", "HTTP"], compact: ["QueryLog"]]

  """
  @spec parse(String.t() | nil) :: [{atom(), [String.t()]}]
  def parse(nil), do: []
  def parse(""), do: []

  def parse(spec) do
    spec
    |> String.split(",", trim: true)
    |> Enum.map(fn entry ->
      with [role, type] <- entry |> String.trim() |> String.split(":", parts: 2),
           {:ok, role} <- Map.fetch(@roles, role),
           true <- valid_type?(type) do
        {role, type}
      else
        _invalid ->
          raise ArgumentError,
                "SMOLQUERY_DUCKDB_LOG entry #{inspect(entry)} is not role:Type " <>
                  "(role one of catalog, compact, merge)"
      end
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {role, types} -> {role, Enum.uniq(types)} end)
    |> Enum.sort()
  end

  @doc """
  Child specs for the drains the application config's `:boot` asks for,
  given the engine each role names.
  """
  @spec boot_engines(%{atom() => atom()}, [{atom(), [String.t()]}]) :: [{module(), keyword()}]
  def boot_engines(engines, boot \\ configured_boot()) do
    for {role, types} <- boot, Map.has_key?(engines, role) do
      {__MODULE__, engine: Map.fetch!(engines, role), types: types}
    end
  end

  defp configured_boot do
    :smolquery
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:boot)
    |> parse()
  end

  @doc """
  What of a log row may leave DuckDB: an `HTTP` row rebuilt from its method,
  URL without query string, range, status and duration; a `CREATE SECRET`
  replaced whole; anything else as it is.
  """
  @spec redact(String.t(), String.t()) :: String.t()
  def redact("HTTP", message) do
    fields = [
      method: ~r/'type': (\w+)/,
      url: ~r/'url': '([^'?]*)/,
      range: ~r/Range='?(bytes=[\d-]+)/i,
      status: ~r/'status': (\w+)/,
      duration_ms: ~r/'duration_ms': (\d+)/
    ]

    Enum.map_join(fields, " ", fn {key, pattern} ->
      case Regex.run(pattern, message) do
        [_match, value] -> "#{key}=#{value}"
        nil -> "#{key}=-"
      end
    end)
  end

  def redact(_type, message) do
    if Regex.match?(
         ~r/\A\s*CREATE\s+(OR\s+REPLACE\s+)?(PERSISTENT\s+|TEMPORARY\s+)?SECRET\b/i,
         message
       ),
       do: "CREATE SECRET <redacted>",
       else: message
  end

  @impl true
  def init(opts) do
    types = Keyword.fetch!(opts, :types)

    unless types != [] and Enum.all?(types, &valid_type?/1) do
      raise ArgumentError, "unsupported DuckDB log types: #{inspect(types)}"
    end

    state = %{
      engine: Keyword.fetch!(opts, :engine),
      types: types,
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      max_rows: Keyword.get(opts, :max_rows, @max_rows),
      instance: nil
    }

    Process.flag(:trap_exit, true)

    case Keyword.get(opts, :for_ms, :infinity) do
      :infinity -> :ok
      ms -> Process.send_after(self(), :expire, ms)
    end

    {:ok, state, {:continue, :enable}}
  end

  @impl true
  def handle_continue(:enable, state) do
    Process.send_after(self(), :drain, state.interval_ms)
    {:noreply, enabled(state)}
  end

  @impl true
  def handle_info(:drain, state) do
    state = state |> enabled() |> drained()
    Process.send_after(self(), :drain, state.interval_ms)
    {:noreply, state}
  end

  def handle_info(:expire, state) do
    {:stop, :normal, drained(state)}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _off = Engine.try_query(state.engine, "CALL disable_logging()")
    _cleared = Engine.try_query(state.engine, "CALL truncate_duckdb_logs()")
    :ok
  end

  defp enabled(state) do
    instance = Process.whereis(Engine.database_name(state.engine))

    if instance == state.instance or is_nil(instance) do
      state
    else
      types = Enum.map_join(state.types, ", ", &"'#{&1}'")

      case Engine.try_query(
             state.engine,
             "CALL enable_logging([#{types}], level := 'trace', storage := 'memory')"
           ) do
        {:ok, _result} ->
          Logger.info(
            "DuckDB logging #{Enum.join(state.types, ", ")} on #{inspect(state.engine)}"
          )

          %{state | instance: instance}

        {:error, error} ->
          Logger.warning("DuckDB logging on #{inspect(state.engine)} failed: #{inspect(error)}")
          state
      end
    end
  end

  defp drained(%{instance: nil} = state), do: state

  defp drained(state) do
    types = Enum.map_join(state.types, ", ", &"'#{&1}'")
    own = "message NOT LIKE '%duckdb_logs%' AND message NOT LIKE '%_logging(%'"

    with {:ok, %{rows: [[total]]}} <-
           Engine.try_query(
             state.engine,
             "SELECT count(*) FROM duckdb_logs WHERE type IN (#{types}) AND #{own}"
           ),
         {:ok, %{rows: rows}} <-
           Engine.try_query(
             state.engine,
             "SELECT type, log_level, connection_id, transaction_id, query_id, message " <>
               "FROM duckdb_logs WHERE type IN (#{types}) AND #{own} " <>
               "ORDER BY timestamp LIMIT #{state.max_rows}"
           ),
         {:ok, _cleared} <- Engine.try_query(state.engine, "CALL truncate_duckdb_logs()") do
      Enum.each(rows, &emit(state.engine, &1))
      dropped(state.engine, total - length(rows))
    else
      failure ->
        Logger.warning("DuckDB log drain on #{inspect(state.engine)} failed: #{inspect(failure)}")
    end

    state
  end

  defp emit(engine, [type, level, connection, transaction, query, message]) do
    Logger.info(
      "duckdb #{inspect(engine)} #{type} #{level} conn=#{connection} txn=#{transaction} " <>
        "query=#{query}: #{redact(type, message || "")}"
    )
  end

  defp dropped(_engine, 0), do: :ok

  defp dropped(engine, count),
    do:
      Logger.warning(
        "DuckDB log drain on #{inspect(engine)} dropped #{count} row(s) over its cap"
      )

  defp valid_type?(type), do: is_binary(type) and Regex.match?(~r/\A[A-Za-z]+\z/, type)

  defp name(engine), do: Module.concat(engine, "Log")
end
