defmodule Smolquery.Engine.Connection do
  @moduledoc """
  A bootstrapped DuckDB connection.

  Wraps an `Adbc.Connection` so that a connection is never observable before its
  extensions and session settings are applied: `init/1` starts the ADBC
  connection linked to this process and runs the bootstrap statements before
  returning. A connection that fails to bootstrap fails to start.

  Because the ADBC connection is linked, its death takes this wrapper with it
  and the supervisor rebuilds both — a restarted connection is a bootstrapped
  connection.

  ## Fatal errors take the database down

  DuckDB answers a fatal or internal error by invalidating the whole database:
  every later query on it fails with "database has been invalidated ... must be
  restarted", forever. Returning that as an ordinary `{:error, _}` would leave
  the engine serving a corpse, so `query/4` replies with the error and then
  kills the `Adbc.Database` it is connected to. The engine subtree is
  `rest_for_one` with the database first, so killing it rebuilds the database
  and every connection after it — a restart being the only recovery DuckDB
  offers.

  DuckDB's other catastrophic mode, an assertion failure, kills the ADBC
  connection process outright instead of returning an error. That path needs no
  policy: this wrapper is linked to it, so both die and the supervisor rebuilds
  the subtree the same way.

  ## Spill isolation

  Connections default to distinct children of the application `:spill_dir`
  (`.tmp` unless configured). DuckDB temp-file names are not instance-specific,
  so live instances sharing a directory can corrupt each other's spilled sorts.
  `:temp_directory` overrides the default.

  Isolation lives here because `Smolquery.QueryService.Runner` creates
  connections directly rather than through `Smolquery.Engine`.
  """

  use GenServer

  require Logger

  alias Smolquery.Engine.Params
  alias Smolquery.Engine.Result
  alias Smolquery.Engine.ResultTooLarge
  alias Smolquery.Identifier

  @default_spill_root ".tmp"

  @fatal_markers ["database has been invalidated", "FATAL Error", "INTERNAL Error"]

  @type option ::
          {:database, GenServer.server()}
          | {:name, GenServer.name()}
          | {:extensions, [atom() | String.t()]}
          | {:settings, keyword()}
          | {:statements, [String.t()]}
          | {:max_rows, pos_integer() | :infinity}
          | {:temp_directory, Path.t()}
          | {:max_temp_directory_size, String.t()}

  @doc """
  Starts a bootstrapped connection to the ADBC database in `:database`.

  ## Options

    * `:database` (required) — the `Adbc.Database` process to connect to
    * `:name` — process name to register under
    * `:extensions` — DuckDB extensions to `INSTALL` and `LOAD`
    * `:settings` — `SET key = value` pairs applied to the session
    * `:statements` — SQL run after extensions and settings, in order
    * `:max_rows` — most rows `query/4` will convert to Elixir terms before
      refusing with `Smolquery.Engine.ResultTooLarge`. `:infinity` disables the
      ceiling.
    * `:temp_directory` — spill directory. Defaults to a distinct child of the
      application `:spill_dir` (`.tmp` unless configured).
    * `:max_temp_directory_size` — per-instance spill limit (e.g. `"10GiB"`).
      Defaults to the application key of the same name; unset uses DuckDB's
      default of 90% of free space.

  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs `sql` with positional `params` (`$1..$n`), returning a `Result`.

  Parameters are bound through `Smolquery.Engine.Params`, which types timestamps
  to match the columns rather than letting ADBC infer them — an inferred
  timestamp silently costs every query its file pruning.

  A result over `:max_rows` is refused with `Smolquery.Engine.ResultTooLarge`
  instead of converted, because the conversion is what would hurt.
  """
  @spec query(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:query, sql, Params.normalize(params)}, timeout)
  end

  @doc """
  The columns `sql` would produce, as `{name, type}` pairs in result order —
  a `DESCRIBE`, which binds the statement without running it.
  """
  @spec describe(GenServer.server(), String.t(), timeout()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, Exception.t()}
  def describe(conn, sql, timeout \\ 30_000) do
    with {:ok, result} <- query(conn, "DESCRIBE " <> sql, [], timeout) do
      {:ok,
       Enum.map(result.rows, fn row ->
         fields = result.columns |> Enum.zip(row) |> Map.new()

         {fields["column_name"], fields["column_type"]}
       end)}
    end
  end

  @doc """
  Runs `sql` and returns the result as an `Explorer.DataFrame`.

  The Arrow stream goes straight to Polars in Rust, so no row is ever built as an
  Elixir term. This runs inside the connection process rather than against the ADBC
  pid directly, so a frame read is subject to the same fatal-error policy as
  `query/4` and holds its connection for the duration — which is what it is
  actually doing.

  Explorer 0.12.0 reaches ADBC through a callback shape adbc 0.12.1 deprecated, so
  every call here prints a deprecation warning from inside Explorer. The warning is
  cosmetic and the fix belongs upstream. Routing around it through Arrow IPC —
  `Adbc.StreamResult.to_ipc_stream/1` into `Explorer.DataFrame.load_ipc_stream/2` —
  works and is quiet, but measured 38% slower on five million rows (388 ms against
  282 ms) and copies the whole result through a 226 MiB binary on the way, which is
  a poor trade for silencing a log line.
  """
  @spec frame(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Exception.t()}
  def frame(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:frame, sql, Params.normalize(params)}, timeout)
  end

  @doc """
  Runs `statements` in order inside one transaction: all commit, or the first
  failure rolls every prior statement back and is returned.

  The whole transaction executes within one call to this process, and that is
  what makes it a transaction at all. An engine has a single connection, so a
  statement from another caller landing between `BEGIN` and `COMMIT` would
  silently join — and be rolled back with — a transaction it knows nothing
  about; serializing through this process closes that interleaving by
  construction.

  Statements carry no parameters. A transaction's SQL is built by trusted
  callers from validated identifiers and escaped literals
  (`Smolquery.Identifier`), and offering bindings here would suggest
  user-supplied values belong in one. They do not.
  """
  @spec transaction(GenServer.server(), [String.t()], timeout()) ::
          :ok | {:error, Exception.t()}
  def transaction(conn, statements, timeout \\ 30_000) do
    case GenServer.call(conn, {:transaction, statements}, timeout) do
      {:ok, :committed} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  The underlying `Adbc.Connection` pid, for callers that need ADBC directly.
  """
  @spec adbc_connection(GenServer.server()) :: pid()
  def adbc_connection(conn), do: GenServer.call(conn, :adbc_connection)

  @doc """
  Whether an error means DuckDB has invalidated the database behind it.

  Fatal and internal errors are unrecoverable in place: DuckDB refuses every
  subsequent query on that database until it is restarted.
  """
  @spec fatal?(Exception.t() | String.t()) :: boolean()
  def fatal?(error) do
    message = if is_binary(error), do: error, else: Exception.message(error)

    Enum.any?(@fatal_markers, &String.contains?(message, &1))
  end

  @impl true
  def init(opts) do
    database = Keyword.fetch!(opts, :database)
    extensions = Keyword.get(opts, :extensions, [])
    settings = spill_settings(opts) ++ Keyword.get(opts, :settings, [])
    statements = Keyword.get(opts, :statements, [])
    max_rows = Keyword.get(opts, :max_rows, :infinity)

    with {:ok, adbc} <- Adbc.Connection.start_link(database: database),
         :ok <- apply_settings(adbc, extension_directory()),
         :ok <- load_extensions(adbc, extensions),
         :ok <- apply_settings(adbc, settings),
         :ok <- run_statements(adbc, statements) do
      {:ok, %{adbc: adbc, database: database, max_rows: max_rows}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, state) do
    state.adbc
    |> Adbc.Connection.query(sql, params)
    |> case do
      {:ok, result} -> convert(result, state.max_rows)
      {:error, error} -> {:error, error}
    end
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call({:frame, sql, params}, _from, state) do
    state.adbc
    |> Explorer.DataFrame.from_query(sql, params)
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call({:transaction, statements}, _from, state) do
    state.adbc
    |> run_transaction(statements)
    |> reply_or_stop(state)
  end

  @impl true
  def handle_call(:adbc_connection, _from, state) do
    {:reply, state.adbc, state}
  end

  defp reply_or_stop({:ok, value}, state), do: {:reply, {:ok, value}, state}

  defp reply_or_stop({:error, error}, state) do
    if fatal?(error) do
      invalidate(state, error)
      {:stop, {:database_invalidated, error}, {:error, error}, state}
    else
      {:reply, {:error, error}, state}
    end
  end

  defp run_transaction(adbc, statements) do
    with {:ok, _begun} <- Adbc.Connection.query(adbc, "BEGIN TRANSACTION"),
         :ok <- apply_or_rollback(adbc, statements),
         {:ok, _committed} <- Adbc.Connection.query(adbc, "COMMIT") do
      {:ok, :committed}
    end
  end

  defp apply_or_rollback(adbc, statements) do
    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case Adbc.Connection.query(adbc, sql) do
        {:ok, _result} -> {:cont, :ok}
        {:error, error} -> {:halt, rollback(adbc, error)}
      end
    end)
  end

  defp rollback(adbc, error) do
    case Adbc.Connection.query(adbc, "ROLLBACK") do
      {:ok, _rolled_back} ->
        {:error, error}

      {:error, rollback_error} ->
        {:error, if(fatal?(rollback_error), do: rollback_error, else: error)}
    end
  end

  defp convert(result, max_rows) do
    case Result.from_adbc(result, max_rows) do
      {:ok, converted} -> {:ok, converted}
      {:error, :too_many_rows} -> {:error, %ResultTooLarge{max: max_rows}}
    end
  end

  defp load_extensions(adbc, extensions) do
    bootstrap(extensions, fn extension ->
      name = to_string(extension)

      case install_and_load(adbc, name) do
        :ok -> :ok
        {:error, reason} -> {:error, {:extension_failed, name, reason}}
      end
    end)
  end

  defp install_and_load(adbc, name) do
    with {:ok, _installed} <- Adbc.Connection.query(adbc, "INSTALL #{name}"),
         {:ok, _loaded} <- Adbc.Connection.query(adbc, "LOAD #{name}") do
      :ok
    end
  end

  defp apply_settings(adbc, settings) do
    bootstrap(settings, fn {key, value} -> apply_setting(adbc, key, value) end)
  end

  defp extension_directory do
    case Application.get_env(:smolquery, :extension_directory) do
      nil -> []
      directory -> [extension_directory: directory]
    end
  end

  # DuckDB refuses to switch temp_directory once the current one has been
  # used, even to the same value — and a connection-only restart re-runs
  # settings against the surviving database. Skip the SET when it would
  # change nothing.
  defp apply_setting(adbc, :temp_directory = key, value) do
    if current_setting(adbc, key) == to_string(value) do
      :ok
    else
      set_setting(adbc, key, value)
    end
  end

  defp apply_setting(adbc, key, value), do: set_setting(adbc, key, value)

  defp set_setting(adbc, key, value) do
    case Adbc.Connection.query(adbc, "SET #{key} = #{quote_setting(value)}") do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:setting_failed, key, reason}}
    end
  end

  defp current_setting(adbc, key) do
    with {:ok, raw} <- Adbc.Connection.query(adbc, "SELECT current_setting('#{key}')"),
         {:ok, result} <- Result.from_adbc(raw, :infinity) do
      Result.one!(result)
    else
      _unreadable -> nil
    end
  end

  defp run_statements(adbc, statements) do
    bootstrap(statements, fn sql ->
      case Adbc.Connection.query(adbc, sql) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, {:statement_failed, safe_sql(sql), reason}}
      end
    end)
  end

  defp safe_sql(sql) do
    if sql =~ ~r/password|secret|key_id/i do
      prefix = sql |> String.split(["'", "("], parts: 2) |> hd() |> String.trim()

      prefix <> " … [statement redacted: carries credentials]"
    else
      sql
    end
  end

  defp invalidate(state, error) do
    Logger.error("""
    DuckDB invalidated the database; restarting the engine subtree.
    #{Exception.message(error)}
    """)

    case GenServer.whereis(state.database) do
      nil -> :ok
      database -> Process.exit(database, :kill)
    end
  end

  defp bootstrap(steps, fun) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      case fun.(step) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp quote_setting(value) when is_integer(value), do: Integer.to_string(value)
  defp quote_setting(value), do: value |> to_string() |> Identifier.sql_string()

  # Caller settings run later and may override these defaults. The mkdir is
  # best-effort on purpose: a bad spill root should fail the first spill, at
  # query time, not every boot of an engine that may never spill.
  defp spill_settings(opts) do
    directory = Keyword.get_lazy(opts, :temp_directory, &default_temp_directory/0)

    File.mkdir_p(Path.dirname(directory))

    [temp_directory: directory] ++ temp_directory_size(opts)
  end

  defp temp_directory_size(opts) do
    opts
    |> Keyword.get_lazy(:max_temp_directory_size, fn ->
      Application.get_env(:smolquery, :max_temp_directory_size)
    end)
    |> case do
      nil -> []
      size -> [max_temp_directory_size: size]
    end
  end

  defp default_temp_directory do
    Path.join(Application.get_env(:smolquery, :spill_dir, @default_spill_root), instance_token())
  end

  # Named engines reuse their leaf after a supervised restart. The OS pid
  # keeps two VMs sharing one spill root out of each other's directories;
  # DuckDB temp-file names are not instance-specific.
  defp instance_token do
    case Process.info(self(), :registered_name) do
      {:registered_name, name} when is_atom(name) -> registered_instance_token(name)
      _unregistered -> "connection-#{System.unique_integer([:positive])}-os#{System.pid()}"
    end
  end

  defp registered_instance_token(name) do
    encoded = name |> Atom.to_string() |> URI.encode(&URI.char_unreserved?/1)

    "#{encoded}-os#{System.pid()}"
  end
end
