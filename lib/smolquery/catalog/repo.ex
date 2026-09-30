defmodule Smolquery.Catalog.Repo do
  @moduledoc """
  An Ecto repo on a Postgres-backed lake's metadata database, used only to
  run `Smolquery.Catalog.Migrator`'s migrations (T-609).

  It is started per migration run, as a dynamic repo, from the connection the
  lake's `postgres:` metadata string names, and stopped when the run ends: no
  query goes through it, so it holds no pool between runs. `init/2` fixes what
  every run needs whatever it was started with:

    * `migration_lock: :pg_advisory_lock`. Ecto's default Postgres lock is a
      table lock held in the migration's transaction, and a migration that
      builds an index `CONCURRENTLY` must run outside one, which turns that
      lock off. The advisory lock is held by the session instead, so nodes
      booting together take turns even through a concurrent index build: the
      first builds, the rest find the migration applied.
    * `migration_source: "smolquery_schema_migrations"`, so the version
      table sits beside the smolquery side tables and does not claim a name
      another tool on the same database might use.
    * `migration_default_prefix: "public"`, where DuckLake keeps its tables.
  """

  use Ecto.Repo, otp_app: :smolquery, adapter: Ecto.Adapters.Postgres

  @impl Ecto.Repo
  def init(_context, config) do
    {:ok,
     Keyword.merge(config,
       migration_lock: :pg_advisory_lock,
       migration_source: "smolquery_schema_migrations",
       migration_default_prefix: "public"
     )}
  end
end
