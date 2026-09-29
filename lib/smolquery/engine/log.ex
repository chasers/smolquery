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
  names (`boot_engines/2`). A runtime call on an engine that already has a
  drain retargets that drain for the set time, and a boot drain then goes
  back to its own types.

  ## One engine, drained on a connection of its own

  `enable_logging` is per DuckDB instance, so it covers every connection of
  the engine it runs on, and each row carries its `connection_id`. The drain
  opens its own connection to the instance rather than using the engine's:
  an engine connection runs one statement at a time, so a drain there would
  wait behind a multi-minute merge, time out, never truncate, and delay the
  seal it queued in front of. Every `:interval_ms` the drain reads the rows
  of `duckdb_logs` newer than the last one it wrote, and writes each to
  `Logger`, its line naming the engine and the connection, transaction and
  query ids. At most `:max_rows` rows leave per drain; the rest are skipped
  past, counted in a warning from the same read, so a burst cannot flood the
  log pipeline or build a backlog. Logs are kept in memory, so the drain
  truncates them once they pass 10,000 rows; a row written between that
  read and that truncation is lost. Truncating on every drain lost rows on
  nearly every drain of a busy engine. A log for diagnosis, not an audit
  trail. A drain that stops, at the end of its time or by
  `stop/1`, drains once more first. The drain's own statements are left out. When the
  engine's instance is rebuilt, the drain connects to the new one and turns
  logging on there.

  ## Nothing secret leaves

  DuckDB logs what it runs verbatim, and what the engines run carries
  credentials:

    * an `HTTP` row records each request's headers, including the S3
      `Authorization` signature and the temporary `x-amz-security-token`.
      An `HTTP` row is therefore rebuilt from an allowlist, the method, the
      URL without its query string, the range, the status and the duration,
      and nothing else of it is logged;
    * a `QueryLog` row of a `CREATE SECRET` holds the key, the secret and the
      session token, and `Smolquery.EngineSecrets` runs one per engine. Such
      a row is logged as `CREATE SECRET <redacted>`;
    * a catalog engine's `ATTACH` holds the metadata database's connection
      string, password included (`Smolquery.DatabaseUrl`). In every other
      `QueryLog` row a `password=` value, the credentials of a URL and an S3
      key setting are replaced by `<redacted>`.

  `HTTP` is never on unless asked for by name, and `redact/2` is applied to
  every row either way.
  """

  use GenServer

  require Logger

  alias Smolquery.Engine
  alias Smolquery.Engine.Result

  @interval_ms 5_000
  @max_rows 1_000
  @truncate_after 10_000
  @roles %{"catalog" => :catalog, "merge" => :merge, "compact" => :compact}

  @type option ::
          {:engine, atom()}
          | {:types, [String.t()]}
          | {:for_ms, pos_integer() | :infinity}
          | {:interval_ms, pos_integer()}
          | {:max_rows, pos_integer()}

  @doc """
  Turns DuckDB logging on for `engine` and drains it for `:for_ms`. With no
  drain on the engine, one starts under `Smolquery.Engine.LogSupervisor` and
  stops when the time is up. With one already there, that drain logs
  `types` for the time instead, and a boot drain then returns to its own.
  """
  @spec start(atom(), [String.t()], [option()]) :: {:ok, pid()} | {:error, term()}
  def start(engine, types, opts \\ []) do
    case Process.whereis(name(engine)) do
      nil ->
        DynamicSupervisor.start_child(
          Smolquery.Engine.LogSupervisor,
          Supervisor.child_spec({__MODULE__, [engine: engine, types: types, base: false] ++ opts},
            restart: :temporary
          )
        )

      pid ->
        with :ok <- GenServer.call(pid, {:override, types, Keyword.get(opts, :for_ms, :infinity)}),
             do: {:ok, pid}
    end
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
  @spec start_link([option() | {:base, boolean()}]) :: GenServer.on_start()
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
  replaced whole; any other statement with its passwords, URL credentials
  and S3 key settings replaced.
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
       ) do
      "CREATE SECRET <redacted>"
    else
      message
      |> String.replace(~r/(\bpassword\s*=\s*)('[^']*'|"[^"]*"|[^\s'",)]+)/i, "\\1<redacted>")
      |> String.replace(~r/(:\/\/[^:\/@\s'"]+:)[^@\s'"]+@/, "\\1<redacted>@")
      |> String.replace(
        ~r/(\bs3_(secret_access_key|session_token|access_key_id)\s*=\s*)('[^']*'|[^\s;]+)/i,
        "\\1<redacted>"
      )
    end
  end

  @impl true
  def init(opts) do
    types = Keyword.fetch!(opts, :types)
    :ok = valid_types!(types)
    Process.flag(:trap_exit, true)

    state = %{
      engine: Keyword.fetch!(opts, :engine),
      base: if(Keyword.get(opts, :base, true), do: types),
      types: types,
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      max_rows: Keyword.get(opts, :max_rows, @max_rows),
      conn: nil,
      instance: nil,
      mark: nil,
      expiry: nil
    }

    Process.send_after(self(), :drain, state.interval_ms)

    {:ok, state |> connected() |> expire_after(Keyword.get(opts, :for_ms, :infinity))}
  end

  @impl true
  def handle_call({:override, types, for_ms}, _from, state) do
    :ok = valid_types!(types)
    {:reply, :ok, state |> drained() |> retarget(types) |> expire_after(for_ms)}
  end

  @impl true
  def handle_info(:drain, state) do
    state = state |> connected() |> drained()
    Process.send_after(self(), :drain, state.interval_ms)
    {:noreply, state}
  end

  def handle_info({:expire, ref}, %{expiry: ref, base: nil} = state),
    do: {:stop, :normal, drained(state)}

  def handle_info({:expire, ref}, %{expiry: ref} = state),
    do: {:noreply, %{retarget(drained(state), state.base) | expiry: nil}}

  def handle_info({:expire, _stale}, state), do: {:noreply, state}

  def handle_info({:EXIT, conn, _reason}, %{conn: conn} = state),
    do: {:noreply, %{state | conn: nil, instance: nil}}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _drained = drained(state)
    _off = run(state, "CALL disable_logging()")
    _cleared = run(state, "CALL truncate_duckdb_logs()")
    :ok
  end

  defp expire_after(state, :infinity), do: %{state | expiry: nil}

  defp expire_after(state, ms) do
    ref = make_ref()
    Process.send_after(self(), {:expire, ref}, ms)
    %{state | expiry: ref}
  end

  defp retarget(state, types) do
    _off = run(state, "CALL disable_logging()")
    enable(%{state | types: types})
  end

  defp connected(state) do
    instance = Process.whereis(Engine.database_name(state.engine))

    cond do
      is_nil(instance) -> state
      instance == state.instance and is_pid(state.conn) -> state
      true -> reconnect(state, instance)
    end
  end

  defp reconnect(state, instance) do
    close(state.conn)

    case Adbc.Connection.start_link(database: instance) do
      {:ok, conn} ->
        enable(%{state | conn: conn, instance: instance, mark: nil})

      {:error, error} ->
        Logger.warning(
          "DuckDB log drain on #{inspect(state.engine)} cannot connect: #{inspect(error)}"
        )

        %{state | conn: nil, instance: nil}
    end
  end

  defp close(nil), do: :ok

  defp close(conn) do
    Process.unlink(conn)
    Process.exit(conn, :shutdown)
  end

  defp enable(%{conn: nil} = state), do: state

  defp enable(state) do
    types = Enum.map_join(state.types, ", ", &"'#{&1}'")

    case run(state, "CALL enable_logging([#{types}], level := 'trace', storage := 'memory')") do
      {:ok, _result} ->
        Logger.info("DuckDB logging #{Enum.join(state.types, ", ")} on #{inspect(state.engine)}")

      {:error, error} ->
        Logger.warning("DuckDB logging on #{inspect(state.engine)} failed: #{inspect(error)}")
    end

    state
  end

  defp drained(%{conn: nil} = state), do: state

  defp drained(state) do
    types = Enum.map_join(state.types, ", ", &"'#{&1}'")

    newer = if state.mark, do: "AND epoch_us(timestamp) > #{state.mark} ", else: ""

    with {:ok, %Result{rows: rows}} <-
           run(
             state,
             "SELECT type, log_level, connection_id, transaction_id, query_id, message, " <>
               "epoch_us(timestamp), count(*) OVER (), max(epoch_us(timestamp)) OVER () " <>
               "FROM duckdb_logs WHERE type IN (#{types}) " <>
               "AND message NOT LIKE '%duckdb_logs%' AND message NOT LIKE '%_logging(%' " <>
               newer <> "ORDER BY timestamp LIMIT #{state.max_rows}"
           ),
         :ok <- trimmed(state) do
      Enum.each(rows, &emit(state.engine, &1))
      %{state | mark: marked(state, rows)}
    else
      failure ->
        Logger.warning("DuckDB log drain on #{inspect(state.engine)} failed: #{inspect(failure)}")
        state
    end
  end

  defp marked(state, []), do: state.mark

  defp marked(
         state,
         [[_type, _level, _conn, _txn, _query, _message, _at, total, last] | _] = rows
       ) do
    over = total - length(rows)

    if over > 0,
      do:
        Logger.warning(
          "DuckDB log drain on #{inspect(state.engine)} dropped #{over} row(s) over its cap"
        )

    last
  end

  defp trimmed(state) do
    case run(state, "SELECT count(*) FROM duckdb_logs") do
      {:ok, %Result{rows: [[count]]}} when count > @truncate_after ->
        with {:ok, _cleared} <- run(state, "CALL truncate_duckdb_logs()"), do: :ok

      {:ok, _small} ->
        :ok

      {:error, _error} = failed ->
        failed
    end
  end

  defp run(%{conn: nil}, _sql), do: {:error, :not_connected}

  defp run(%{conn: conn}, sql) do
    case Adbc.Connection.query(conn, sql) do
      {:ok, result} -> {:ok, Result.from_adbc(result)}
      {:error, _error} = failed -> failed
    end
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp emit(engine, [type, level, connection, transaction, query, message, _at, _total, _last]) do
    Logger.info(
      "duckdb #{inspect(engine)} #{type} #{level} conn=#{connection} txn=#{transaction} " <>
        "query=#{query}: #{redact(type, message || "")}"
    )
  end

  defp valid_types!(types) do
    if types != [] and Enum.all?(types, &valid_type?/1),
      do: :ok,
      else: raise(ArgumentError, "unsupported DuckDB log types: #{inspect(types)}")
  end

  defp valid_type?(type), do: is_binary(type) and Regex.match?(~r/\A[A-Za-z]+\z/, type)

  defp name(engine), do: Module.concat(engine, "Log")
end
