defmodule Smolquery.Test.Postgres do
  @moduledoc """
  The Postgres the integration suite federates to, and its connection details.

  `Smolquery.Catalog.DuckLakePostgresTest` documents why the suite owns a
  database rather than sharing `postgres`: DuckLake creates stats tables
  without primary keys, and a developer database carrying a `FOR ALL TABLES`
  publication then refuses every commit after a table's first one.

  The database is created on demand because CI's Postgres service starts with
  only the default databases. A test that connects without creating it first
  passes locally, where an earlier run left the database behind, and fails on
  a fresh runner — which is exactly how this module came to exist.
  """

  @database "smolquery_test"

  @doc """
  Connection options for the federation database, overridable through the
  `TEST_POSTGRES_*` environment the DuckLake suite already uses.
  """
  @spec connection() :: keyword()
  def connection do
    [
      hostname: System.get_env("TEST_POSTGRES_HOST", "localhost"),
      port: System.get_env("TEST_POSTGRES_PORT", "5432") |> String.to_integer(),
      username: System.get_env("TEST_POSTGRES_USER", "postgres"),
      password: System.get_env("TEST_POSTGRES_PASSWORD", "postgres"),
      database: System.get_env("TEST_POSTGRES_DATABASE", @database)
    ]
  end

  @doc """
  The string-keyed `catalog` map `Smolquery.Catalog.Dataset.new/1` takes for
  a dataset whose own catalog is this Postgres — `:database` overridable,
  because one Postgres database serves one dataset (PL-51).
  """
  @spec catalog_params(keyword()) :: map()
  def catalog_params(overrides \\ []) do
    options = Keyword.merge(connection(), overrides)

    %{
      "host" => options[:hostname],
      "port" => options[:port],
      "database" => options[:database],
      "username" => options[:username],
      "password" => options[:password],
      "sslmode" => "disable"
    }
  end

  @doc """
  Drops every `ducklake_*` and `smolquery_*` table in `database`'s default
  schema, so a test starts from an empty lake. A DuckLake's identity is its
  metadata database, so two tests sharing one database cannot otherwise get
  a clean catalog each.
  """
  @spec reset_lake!(String.t()) :: :ok
  def reset_lake!(database) do
    {:ok, conn} = Postgrex.start_link(Keyword.put(connection(), :database, database))
    Postgrex.query!(conn, drop_lake_tables_sql(), [])
    GenServer.stop(conn)
  end

  defp drop_lake_tables_sql do
    """
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT tablename FROM pg_tables
                WHERE schemaname = current_schema()
                  AND (tablename LIKE 'ducklake\\_%' ESCAPE '\\'
                       OR tablename LIKE 'smolquery\\_%' ESCAPE '\\')
      LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
      END LOOP;
    END $$;
    """
  end

  @doc """
  Creates the database if it does not exist, and answers its connection
  options either way.
  """
  @spec ensure_database!() :: keyword()
  def ensure_database!, do: ensure_database!(connection()[:database])

  @doc """
  `ensure_database!/0` for a database of another name — a second dataset
  needs a second database (PL-51).
  """
  @spec ensure_database!(String.t()) :: keyword()
  def ensure_database!(database) do
    options = Keyword.put(connection(), :database, database)
    {:ok, conn} = Postgrex.start_link(Keyword.put(options, :database, "postgres"))

    result = Postgrex.query(conn, ~s|CREATE DATABASE "#{options[:database]}"|, [])
    GenServer.stop(conn)

    case result do
      {:ok, _created} -> options
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_database}}} -> options
      {:error, error} -> raise error
    end
  end
end
