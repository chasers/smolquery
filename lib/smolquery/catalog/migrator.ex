defmodule Smolquery.Catalog.Migrator do
  @moduledoc """
  Brings a Postgres metadata database up to date before anything reads it:
  one boot step, run once per node, before `Smolquery.Cluster` and every
  service (T-609).

  Everything smolquery keeps in the metadata database besides DuckLake's own
  tables is an Ecto migration under `Smolquery.Catalog.Migrations`, listed in
  `migrations/0`: the table-option side tables, retention, the ring
  configuration, and the indexes `Smolquery.Catalog.DuckLake.Reader` reads by.
  They used to be `CREATE TABLE IF NOT EXISTS` statements run by whichever
  code touched a table first, on every node at once, and `IF NOT EXISTS` is
  not atomic against a concurrent create: two nodes creating one table
  collide in Postgres's type catalog.

  ## The step

  `prepare/1` runs, on one Postgrex connection holding a session advisory
  lock, so nodes booting together take turns. A waiting node polls
  `pg_try_advisory_lock` rather than blocking in `pg_advisory_lock`, as Ecto's
  own migration lock does: a blocked `pg_advisory_lock` is a statement still
  running, and the holder's `CREATE INDEX CONCURRENTLY` waits for every
  running statement to finish, so the two would wait on each other forever,
  a cycle Postgres does not detect as a deadlock.

    1. **Attach.** A throwaway engine attaches the lake as every role does, so
       DuckLake creates its tables on a fresh database, and migrates its own
       format when `automatic_migration` allows it
       (`Smolquery.Catalog.DuckLake.attach_statement/4`).
    2. **Migrate.** Every pending migration runs through a
       `Smolquery.Catalog.Repo` started for the run, which takes Ecto's own
       advisory lock besides and records versions in
       `smolquery_schema_migrations`.

  The first node does the work; the rest wait on the lock and find nothing
  to do. `Smolquery.Application` starts the step as a child that returns
  `:ignore` once the database is ready, so the children after it start only
  then, and one that fails fails the boot: a node whose side tables or ring
  configuration table cannot be made would fail its first read or its first
  ring change anyway, later and less clearly. A SQLite lake, or a metadata
  string Postgrex would not connect to as libpq does
  (`Smolquery.Catalog.DuckLake.Reader.libpq_options/1`), has nothing to
  migrate and the step is `:ignore` at once.

  The migrations are compiled modules, not `.exs` files Ecto compiles at run
  time, and a migration is immutable once applied: a change is a new module
  with a new version.

  ## One database

  The step migrates the lake's metadata database. The ring configuration
  lives in the database `Smolquery.Cluster` connects to, which is the same
  one whenever both come from `CATALOG_DATABASE_URL`; a `SMOLQUERY_CATALOG`
  naming another database leaves the cluster's without its table, and
  `Smolquery.Cluster.ConfigStore.Postgres.setup/1` says so.
  """

  require Logger

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.DuckLake.Reader
  alias Smolquery.Catalog.Migrations
  alias Smolquery.Catalog.Repo

  @lock "smolquery_catalog_prepare"
  @lock_retry_ms 500

  @doc """
  The child `Smolquery.Application` starts before the cluster and the
  services: it prepares the lake the application configuration names.
  """
  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_arg),
    do:
      Supervisor.child_spec(%{id: __MODULE__, start: {__MODULE__, :start_link, []}},
        restart: :temporary
      )

  @doc """
  Prepares the configured lake, answering `:ignore` once it is ready or has
  nothing to prepare, and the error that fails the boot otherwise.
  """
  @spec start_link() :: :ignore | {:error, term()}
  def start_link do
    case prepare(Application.get_env(:smolquery, DuckLake, [])) do
      :ok -> :ignore
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Attaches the lake `config` names and applies every pending migration, under
  the fleet-wide lock; `:ok` too when its metadata is not Postgres. `config`
  holds `:metadata`, `:data_path` and optionally `:catalog` and
  `:automatic_migration`, as `Smolquery.Catalog.DuckLake.start_link/1` takes
  them.
  """
  @spec prepare(keyword()) :: :ok | {:error, term()}
  def prepare(config) do
    case options(Keyword.get(config, :metadata)) do
      {:ok, options} -> locked(options, fn -> attach_and_migrate(config, options) end)
      :none -> :ok
    end
  end

  @doc """
  The repo options for `metadata`'s database, or `:none` when there is nothing
  to migrate: SQLite metadata, or a `postgres:` string Postgrex would not
  connect to as libpq does.
  """
  @spec options(String.t() | nil) :: {:ok, keyword()} | :none
  def options("postgres:" <> libpq) do
    case Reader.libpq_options(libpq) do
      {:ok, connection} ->
        {:ok, connection ++ [pool_size: 2, parameters: [search_path: "public"], log: false]}

      :error ->
        :none
    end
  end

  def options(_metadata), do: :none

  @doc """
  Applies `migrations` that are pending through a repo started with
  `options`, answering the versions applied. `prepare/1` runs every
  migration; a caller that only needs some, such as a test of one table,
  names them.
  """
  @spec migrate(keyword(), [{non_neg_integer(), module()}]) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def migrate(options, migrations \\ migrations()) do
    case Repo.start_link([name: nil] ++ options) do
      {:ok, repo} -> run(repo, migrations)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The catalog migrations, by version, oldest first.
  """
  @spec migrations() :: [{non_neg_integer(), module()}]
  def migrations do
    [
      {20_260_930_110_000, Migrations.SmolqueryTables},
      {20_260_930_120_000, Migrations.DucklakeReadIndexes}
    ]
  end

  defp run(repo, migrations) do
    {:ok, Ecto.Migrator.run(Repo, migrations, :up, all: true, dynamic_repo: repo, log: :info)}
  rescue
    error in [Postgrex.Error, DBConnection.ConnectionError] -> {:error, error}
  after
    Supervisor.stop(repo)
  end

  defp attach_and_migrate(config, options) do
    with :ok <- attach(config),
         {:ok, versions} <- migrate(options) do
      unless versions == [], do: Logger.info("catalog migrations applied: #{inspect(versions)}")
      :ok
    end
  end

  defp attach(config) do
    lake = Module.concat(__MODULE__, "Lake#{System.unique_integer([:positive])}")

    lake_config =
      [name: lake] ++
        Keyword.take(config, [:metadata, :data_path, :catalog, :automatic_migration])

    case DuckLake.start_link(lake_config) do
      {:ok, pid} -> Supervisor.stop(pid)
      {:error, reason} -> {:error, {:attach_failed, reason}}
    end
  catch
    :exit, reason -> {:error, {:attach_failed, reason}}
  end

  defp locked(options, fun) do
    connection = Keyword.drop(options, [:pool_size, :log])

    with {:ok, conn} <- Postgrex.start_link(connection) do
      try do
        await_lock(conn)
        fun.()
      rescue
        error in [Postgrex.Error, DBConnection.ConnectionError] -> {:error, error}
      after
        GenServer.stop(conn)
      end
    end
    |> tap(&log_failure/1)
  end

  defp await_lock(conn) do
    case Postgrex.query!(conn, "SELECT pg_try_advisory_lock(hashtext($1))", [@lock]) do
      %{rows: [[true]]} ->
        :ok

      %{rows: [[false]]} ->
        Process.sleep(@lock_retry_ms)
        await_lock(conn)
    end
  end

  defp log_failure(:ok), do: :ok

  defp log_failure({:error, reason}) do
    Logger.error("the catalog metadata database could not be prepared: #{inspect(reason)}")
  end
end
