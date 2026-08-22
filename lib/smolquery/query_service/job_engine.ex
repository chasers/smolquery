defmodule Smolquery.QueryService.JobEngine do
  @moduledoc """
  The lifecycle of one disposable DuckDB engine: an `Adbc.Database` and a
  bootstrapped `Smolquery.Engine.Connection`, linked to the caller, killed
  on teardown.

  `Smolquery.QueryService.Runner` (the job engine) and
  `Smolquery.QueryService.PartialWorker` (a shard engine) share exactly this
  mechanic and nothing else — each composes its own extensions, settings,
  and bootstrap statements. Keeping the start/kill choreography in one place
  is what stops the two from drifting (PL-49 review): a half-started engine
  is killed rather than leaked, and `stop/1` unlinks before it kills, so a
  non-trapping caller — the worker runs inside an `:erpc`-spawned process —
  never races its own teardown's exit signal against delivering its result.
  """

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection

  @type t :: %{database: pid(), connection: pid()}

  @doc """
  Starts a database and a connection bootstrapped with `opts`
  (`Smolquery.Engine.Connection.start_link/1` options, minus `:database`).

  A connection that fails to bootstrap takes the database with it.
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts) do
    with {:ok, database} <- DuckDB.start_link() do
      case Connection.start_link([{:database, database} | opts]) do
        {:ok, connection} ->
          {:ok, %{database: database, connection: connection}}

        {:error, reason} ->
          Process.exit(database, :kill)

          {:error, reason}
      end
    end
  end

  @doc """
  Unlinks and kills both engine processes; DuckDB's in-flight work dies with
  its connection.
  """
  @spec stop(t() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(%{database: database, connection: connection}) do
    Process.unlink(connection)
    Process.unlink(database)
    Process.exit(connection, :kill)
    Process.exit(database, :kill)

    :ok
  end
end
