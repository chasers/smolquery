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

  `prepare/1` first reads which migrations the database has recorded, on one
  plain connection, and when none is pending it is done: a node booting into
  a migrated database takes no lock and attaches nothing. Otherwise, holding
  a session advisory lock so nodes booting together take turns, it:

    1. **Attaches** the lake with a throwaway engine, as every role does, so
       DuckLake creates its tables on a fresh database, and migrates its own
       format when `automatic_migration` allows it
       (`Smolquery.Catalog.DuckLake.attach_statement/4`).
    2. **Migrates**, through a `Smolquery.Catalog.Repo` started for the run,
       recording versions in `smolquery_schema_migrations`.

  The outer lock is what serializes the attach, and the creation of the
  version table: Ecto creates that with `CREATE TABLE IF NOT EXISTS` before
  it takes its own migration lock, so two first runs would collide on it.
  Both locks are taken by polling `pg_try_advisory_lock`, never by blocking
  in `pg_advisory_lock`: a blocked `pg_advisory_lock` is a statement still
  running, and the holder's `CREATE INDEX CONCURRENTLY` waits for every
  running statement to finish, so the two would wait on each other forever,
  a cycle Postgres does not detect as a deadlock. A node waiting for the lock
  logs so every ten seconds.

  `Smolquery.Application` starts the step as a child that returns `:ignore`
  once the database is ready, so the children after it start only then, and
  one that fails fails the boot, logged as "the catalog metadata database
  could not be prepared": a node whose side tables or ring configuration
  table cannot be made would fail its first read or its first ring change
  anyway, later and less clearly.

  ## What it prepares, and what it leaves to the old path

  The step prepares the lake the application configuration names
  (`Smolquery.Catalog.DuckLake`'s `:metadata`) when it is Postgres and
  Postgrex can connect to it as libpq does
  (`Smolquery.Catalog.DuckLake.Reader.libpq_options/1`); `prepared?/1` says
  which metadata that is. Such a lake's engines skip the side-table
  bootstrap. Any other lake, SQLite, a string Postgrex cannot use, or a
  service's own `catalog:` override, keeps the bootstrap
  `CREATE TABLE IF NOT EXISTS` statements it always had.

  When clustering is on, the step also applies the tables migration to the
  database `Smolquery.Cluster` connects to, which holds the ring
  configuration. That is the lake's database whenever both come from
  `CATALOG_DATABASE_URL`, and then it finds the migration recorded.

  The migrations are compiled modules, not `.exs` files Ecto compiles at run
  time, and a migration is immutable once applied: a change is a new module
  with a new version.
  """

  require Logger

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.DuckLake.Reader
  alias Smolquery.Catalog.Migrations
  alias Smolquery.Catalog.Repo

  @lock "smolquery_catalog_prepare"
  @lock_retry_ms 500
  @lock_log_every 20
  @failures [
    Postgrex.Error,
    Postgrex.QueryError,
    DBConnection.ConnectionError,
    Ecto.MigrationError,
    ArgumentError
  ]

  @doc """
  The child `Smolquery.Application` starts before the cluster and the
  services: it prepares the configured lake and the cluster's database.
  """
  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_arg) do
    Supervisor.child_spec(%{id: __MODULE__, start: {__MODULE__, :start_link, []}},
      restart: :temporary
    )
  end

  @doc """
  Prepares the configured lake, then the cluster's database, answering
  `:ignore` once both are ready or have nothing to prepare, and the error
  that fails the boot otherwise.
  """
  @spec start_link() :: :ignore | {:error, term()}
  def start_link do
    with :ok <- prepare(Application.get_env(:smolquery, DuckLake, [])),
         :ok <- prepare_cluster(Application.get_env(:smolquery, Smolquery.Cluster, [])) do
      :ignore
    end
  end

  @doc """
  Attaches the lake `config` names and applies every pending migration,
  under the fleet-wide lock; `:ok` at once when nothing is pending or its
  metadata is not one this step prepares. `config` holds `:metadata`,
  `:data_path` and optionally `:catalog` and `:automatic_migration`, as
  `Smolquery.Catalog.DuckLake.start_link/1` takes them.
  """
  @spec prepare(keyword()) :: :ok | {:error, term()}
  def prepare(config) do
    case options(Keyword.get(config, :metadata)) do
      {:ok, options} -> ready(options, migrations(), fn -> attach(config) end)
      :none -> :ok
    end
  end

  @doc """
  Applies the tables migration, whose ring configuration table the cluster
  fences with, to the database `config` (`Smolquery.Cluster`'s) names when
  clustering is on; `:ok` otherwise.
  """
  @spec prepare_cluster(keyword()) :: :ok | {:error, term()}
  def prepare_cluster(config) do
    with true <- Keyword.get(config, :enabled, false),
         [_ | _] = postgres <- Keyword.get(config, :postgres) do
      ready(repo_options(postgres), Enum.take(migrations(), 1), fn -> :ok end)
    else
      _off -> :ok
    end
  end

  @doc """
  Whether engines for `metadata` can leave smolquery's tables to this step:
  it is the metadata the application configuration names, and one the step
  prepares.
  """
  @spec prepared?(String.t() | nil) :: boolean()
  def prepared?(metadata) do
    metadata == Keyword.get(Application.get_env(:smolquery, DuckLake, []), :metadata) and
      options(metadata) != :none
  end

  @doc """
  The repo options for `metadata`'s database, or `:none` when there is nothing
  to migrate: SQLite metadata, or a `postgres:` string Postgrex would not
  connect to as libpq does.
  """
  @spec options(String.t() | nil) :: {:ok, keyword()} | :none
  def options("postgres:" <> libpq) do
    case Reader.libpq_options(libpq) do
      {:ok, connection} -> {:ok, repo_options(connection)}
      :error -> :none
    end
  end

  def options(_metadata), do: :none

  @doc """
  Applies `migrations` that are pending through a repo started with
  `options`, answering the versions applied. It takes no lock of its own
  beyond Ecto's; `prepare/1` runs it under the fleet-wide one.
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
      {20_260_930_120_000, Migrations.DucklakeReadIndexes},
      {20_261_001_100_000, Migrations.ConnectionKinds}
    ]
  end

  defp repo_options(connection),
    do: connection ++ [pool_size: 2, parameters: [search_path: "public"], log: false]

  defp ready(options, migrations, before) do
    case pending(options, migrations) do
      {:ok, []} ->
        :ok

      _pending_or_unknown ->
        locked(options, fn -> apply_pending(options, migrations, before) end)
    end
  end

  defp apply_pending(options, migrations, before) do
    with :ok <- before.(),
         {:ok, versions} <- migrate(options, migrations) do
      unless versions == [], do: Logger.info("catalog migrations applied: #{inspect(versions)}")
      :ok
    end
  end

  defp pending(options, migrations) do
    with_connection(options, fn conn ->
      case Postgrex.query(conn, "SELECT version FROM public.smolquery_schema_migrations", []) do
        {:ok, %{rows: rows}} ->
          applied = MapSet.new(rows, &hd/1)
          {:ok, for({version, _module} <- migrations, version not in applied, do: version)}

        {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} ->
          {:ok, Enum.map(migrations, &elem(&1, 0))}

        {:error, error} ->
          {:error, error}
      end
    end)
  end

  defp run(repo, migrations) do
    {:ok, Ecto.Migrator.run(Repo, migrations, :up, all: true, dynamic_repo: repo, log: :info)}
  rescue
    error in @failures -> {:error, error}
  after
    Supervisor.stop(repo)
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
    with_connection(options, fn conn ->
      await_lock(conn, 1)
      fun.()
    end)
    |> tap(&log_failure/1)
  end

  defp with_connection(options, fun) do
    with {:ok, conn} <- Postgrex.start_link(Keyword.drop(options, [:pool_size, :log])) do
      try do
        fun.(conn)
      rescue
        error in @failures -> {:error, error}
      after
        GenServer.stop(conn)
      end
    end
  end

  defp await_lock(conn, attempt) do
    case Postgrex.query!(conn, "SELECT pg_try_advisory_lock(hashtext($1))", [@lock]) do
      %{rows: [[true]]} ->
        :ok

      %{rows: [[false]]} ->
        if rem(attempt, @lock_log_every) == 0 do
          Logger.info(
            "waiting for another node to prepare the catalog metadata database " <>
              "(advisory lock #{@lock}, #{div(attempt * @lock_retry_ms, 1000)} s so far)"
          )
        end

        Process.sleep(@lock_retry_ms)
        await_lock(conn, attempt + 1)
    end
  end

  defp log_failure(:ok), do: :ok

  defp log_failure({:error, reason}) do
    Logger.error("the catalog metadata database could not be prepared: #{inspect(reason)}")
  end
end
