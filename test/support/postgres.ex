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
  Creates the database if it does not exist, and answers its connection
  options either way.
  """
  @spec ensure_database!() :: keyword()
  def ensure_database! do
    options = connection()
    {:ok, conn} = Postgrex.start_link(Keyword.put(options, :database, "postgres"))

    result = Postgrex.query(conn, ~s|CREATE DATABASE "#{options[:database]}"|, [])
    GenServer.stop(conn)

    case result do
      {:ok, _created} -> options
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_database}}} -> options
      {:error, error} -> raise error
    end
  end

  @doc """
  Drops every DuckLake metadata table and smolquery side table in the
  connection's database, so the next attach starts a clean catalog. A
  catalog's identity lives in its metadata database, so the Postgres suites
  share one and reset it rather than each creating their own. The suites
  also reset on exit: the federation suite ranks this database's tables by
  size, and a lake left behind outranks its fixture table.
  """
  @spec reset_ducklake!(keyword()) :: :ok
  def reset_ducklake!(connection) do
    {:ok, conn} = Postgrex.start_link(connection)

    Postgrex.query!(
      conn,
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
      """,
      []
    )

    GenServer.stop(conn)
  end
end
