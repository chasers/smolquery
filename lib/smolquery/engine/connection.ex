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
  """

  use GenServer

  alias Smolquery.Engine.Result

  @type option ::
          {:database, GenServer.server()}
          | {:name, GenServer.name()}
          | {:extensions, [atom() | String.t()]}
          | {:settings, keyword()}

  @doc """
  Starts a bootstrapped connection to the ADBC database in `:database`.

  ## Options

    * `:database` (required) — the `Adbc.Database` process to connect to
    * `:name` — process name to register under
    * `:extensions` — DuckDB extensions to `INSTALL` and `LOAD`
    * `:settings` — `SET key = value` pairs applied to the session

  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs `sql` with positional `params` (`$1..$n`), returning a `Result`.
  """
  @spec query(GenServer.server(), String.t(), [term()], timeout()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, sql, params \\ [], timeout \\ 30_000) do
    GenServer.call(conn, {:query, sql, params}, timeout)
  end

  @doc """
  The underlying `Adbc.Connection` pid, for callers that need ADBC directly.
  """
  @spec adbc_connection(GenServer.server()) :: pid()
  def adbc_connection(conn), do: GenServer.call(conn, :adbc_connection)

  @impl true
  def init(opts) do
    database = Keyword.fetch!(opts, :database)
    extensions = Keyword.get(opts, :extensions, [])
    settings = Keyword.get(opts, :settings, [])

    with {:ok, adbc} <- Adbc.Connection.start_link(database: database),
         :ok <- load_extensions(adbc, extensions),
         :ok <- apply_settings(adbc, settings) do
      {:ok, %{adbc: adbc}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, state) do
    case Adbc.Connection.query(state.adbc, sql, params) do
      {:ok, result} -> {:reply, {:ok, Result.from_adbc(result)}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:adbc_connection, _from, state) do
    {:reply, state.adbc, state}
  end

  defp load_extensions(adbc, extensions) do
    Enum.reduce_while(extensions, :ok, fn extension, :ok ->
      name = to_string(extension)

      case install_and_load(adbc, name) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:extension_failed, name, reason}}}
      end
    end)
  end

  defp install_and_load(adbc, name) do
    with {:ok, _} <- Adbc.Connection.query(adbc, "INSTALL #{name}"),
         {:ok, _} <- Adbc.Connection.query(adbc, "LOAD #{name}") do
      :ok
    end
  end

  defp apply_settings(adbc, settings) do
    Enum.reduce_while(settings, :ok, fn {key, value}, :ok ->
      case Adbc.Connection.query(adbc, "SET #{key} = #{quote_setting(value)}") do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:setting_failed, key, reason}}}
      end
    end)
  end

  defp quote_setting(value) when is_integer(value), do: Integer.to_string(value)
  defp quote_setting(value), do: "'#{value}'"
end
