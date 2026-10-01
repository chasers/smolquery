defmodule Smolquery.Catalog.Migrations.ConnectionKinds do
  @moduledoc """
  Gives `smolquery_connections` what a DuckLake connection needs (T-610):

    * `kind`, `'postgres'` for every connection made before it;
    * `options`, JSON text holding a DuckLake connection's data path and the
      S3 settings that are not secret;
    * `storage_secret`, its S3 secret, sealed by `Smolquery.Secrets` like
      the password.

  `ADD COLUMN IF NOT EXISTS`, so a table a node's bootstrap already altered
  (`Smolquery.Catalog.DuckLake.alter_connections_statements/1`) is unchanged.
  Every statement on the table names its columns, so a node from before this
  migration keeps reading and writing Postgres connections through it; see
  the 0.21.0 rollout note in `docs/deployment.md` for DuckLake ones.
  """

  use Ecto.Migration

  def up do
    alter table(:smolquery_connections) do
      add_if_not_exists :kind, :varchar, default: "postgres"
      add_if_not_exists :options, :varchar
      add_if_not_exists :storage_secret, :varchar
    end
  end

  def down do
    alter table(:smolquery_connections) do
      remove_if_exists :kind, :varchar
      remove_if_exists :options, :varchar
      remove_if_exists :storage_secret, :varchar
    end
  end
end
