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
  """

  use GenServer

  require Logger

  alias Smolquery.Engine.Params
  alias Smolquery.Engine.Result

  @fatal_markers ["database has been invalidated", "FATAL Error", "INTERNAL Error"]

  @type option ::
          {:database, GenServer.server()}
          | {:name, GenServer.name()}
          | {:extensions, [atom() | String.t()]}
          | {:settings, keyword()}
          | {:statements, [String.t()]}

  @doc """
  Starts a bootstrapped connection to the ADBC database in `:database`.

  ## Options

    * `:database` (required) — the `Adbc.Database` process to connect to
    * `:name` — process name to register under
    * `:extensions` — DuckDB extensions to `INSTALL` and `LOAD`
    * `:settings` — `SET key = value` pairs applied to the session
    * `:statements` — SQL run after extensions and settings, in order

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
  """
  @spec query(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:query, sql, Params.normalize(params)}, timeout)
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
  @spec fatal?(Exception.t()) :: boolean()
  def fatal?(error) do
    message = Exception.message(error)

    Enum.any?(@fatal_markers, &String.contains?(message, &1))
  end

  @impl true
  def init(opts) do
    database = Keyword.fetch!(opts, :database)
    extensions = Keyword.get(opts, :extensions, [])
    settings = Keyword.get(opts, :settings, [])
    statements = Keyword.get(opts, :statements, [])

    with {:ok, adbc} <- Adbc.Connection.start_link(database: database),
         :ok <- load_extensions(adbc, extensions),
         :ok <- apply_settings(adbc, settings),
         :ok <- run_statements(adbc, statements) do
      {:ok, %{adbc: adbc, database: database}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, state) do
    case Adbc.Connection.query(state.adbc, sql, params) do
      {:ok, result} ->
        {:reply, {:ok, Result.from_adbc(result)}, state}

      {:error, error} ->
        if fatal?(error) do
          invalidate(state, error)
          {:stop, {:database_invalidated, error}, {:error, error}, state}
        else
          {:reply, {:error, error}, state}
        end
    end
  end

  @impl true
  def handle_call(:adbc_connection, _from, state) do
    {:reply, state.adbc, state}
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
    bootstrap(settings, fn {key, value} ->
      case Adbc.Connection.query(adbc, "SET #{key} = #{quote_setting(value)}") do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, {:setting_failed, key, reason}}
      end
    end)
  end

  defp run_statements(adbc, statements) do
    bootstrap(statements, fn sql ->
      case Adbc.Connection.query(adbc, sql) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, {:statement_failed, sql, reason}}
      end
    end)
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
  defp quote_setting(value), do: "'#{value}'"
end
