defmodule Smolquery.Catalog.Migrations.SmolqueryTables do
  @moduledoc """
  The tables smolquery keeps beside DuckLake's in a Postgres metadata
  database (T-609), each as it was created before migrations existed:

    * `smolquery_clustering`, `smolquery_partitions`,
      `smolquery_materialized`, `smolquery_required_columns`,
      `smolquery_connections` and `smolquery_retention`, the table options
      `Smolquery.Catalog.DuckLake` reads and writes, which its DuckDB
      bootstrap created per connection with `CREATE TABLE IF NOT EXISTS`, and
      retention lazily on first use;
    * `smolquery_ring_config`, the ring configuration
      `Smolquery.Cluster.ConfigStore.Postgres` fences membership with, which
      its `setup/1` created.

  Every table is `IF NOT EXISTS`, so a database whose tables those paths
  already made is unchanged by this migration and only records it. The
  column types are the ones DuckDB's postgres extension gave the bootstrap's
  `VARCHAR`, `INTEGER` and `BIGINT`, so a fresh database matches an old one.
  The primary keys are each table's replica identity, which a published
  metadata database needs for `UPDATE` and `DELETE`; see
  `Smolquery.Catalog.DuckLake.create_clustering_statement/1`.

  A SQLite metadata database has no migrations; its bootstrap still creates
  these tables, except the ring configuration, which only Postgres holds.
  """

  use Ecto.Migration

  def up do
    options(:smolquery_clustering, fn ->
      add :column_name, :varchar, null: false
      add :position, :integer, null: false, primary_key: true
    end)

    options(:smolquery_partitions, fn ->
      add :partition_count, :integer, null: false
    end)

    options(:smolquery_materialized, fn ->
      add :column_id, :bigint, null: false, primary_key: true
      add :expression, :varchar, null: false
      add :canonical, :varchar, null: false
      add :sources, :varchar, null: false
    end)

    options(:smolquery_required_columns, fn ->
      add :column_id, :bigint, null: false, primary_key: true
    end)

    options(:smolquery_retention, fn ->
      add :column_name, :varchar, null: false
      add :ttl_ms, :bigint, null: false
    end)

    create_if_not_exists table(:smolquery_connections, primary_key: false) do
      add :name, :varchar, primary_key: true
      add :host, :varchar, null: false
      add :port, :integer, null: false
      add :database_name, :varchar, null: false
      add :username, :varchar, null: false
      add :secret, :varchar, null: false
      add :sslmode, :varchar, null: false
      add :created_at, :bigint, null: false
      add :updated_at, :bigint, null: false
    end

    create_if_not_exists table(:smolquery_ring_config, primary_key: false) do
      add :scope, :text, primary_key: true
      add :epoch, :bigint, null: false
      add :members, :text, null: false
      add :prev_members, :text
      add :changed_at, :timestamptz, null: false, default: fragment("now()")
    end
  end

  def down do
    for table <-
          ~w(smolquery_clustering smolquery_partitions smolquery_materialized
             smolquery_required_columns smolquery_retention smolquery_connections
             smolquery_ring_config)a do
      drop_if_exists table(table)
    end
  end

  defp options(table, columns) do
    create_if_not_exists table(table, primary_key: false) do
      add :dataset, :varchar, null: false, primary_key: true
      add :table_name, :varchar, null: false, primary_key: true
      columns.()
    end
  end
end
