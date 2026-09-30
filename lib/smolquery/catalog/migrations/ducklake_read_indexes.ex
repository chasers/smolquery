defmodule Smolquery.Catalog.Migrations.DucklakeReadIndexes do
  @moduledoc """
  The indexes `Smolquery.Catalog.DuckLake.Reader` reads by (T-609). DuckLake
  creates primary keys on its Postgres metadata and no other index, so its
  reads by `table_id` scan `ducklake_data_file`, `ducklake_column` and
  `ducklake_file_column_stats` whole.

  Each index is built `CONCURRENTLY`, so a lake in use keeps committing while
  it builds, which is why the migration runs outside a transaction. A failed
  concurrent build leaves an INVALID index behind, which `IF NOT EXISTS`
  would then skip on every later run; so an invalid index of the same name is
  dropped before the build.
  """

  use Ecto.Migration

  @disable_ddl_transaction true

  @indexes [
    {:ducklake_data_file, [:table_id, :begin_snapshot], :smolquery_data_file_table},
    {:ducklake_file_column_stats, [:table_id, :data_file_id], :smolquery_file_column_stats_table},
    {:ducklake_column, [:table_id], :smolquery_column_table}
  ]

  def up do
    for {table, columns, name} <- @indexes do
      if invalid?(name), do: drop(index(table, columns, name: name, concurrently: true))
      create_if_not_exists(index(table, columns, name: name, concurrently: true))
    end
  end

  def down do
    for {table, columns, name} <- @indexes do
      drop_if_exists(index(table, columns, name: name, concurrently: true))
    end
  end

  defp invalid?(name) do
    %{rows: rows} =
      repo().query!(
        "SELECT NOT i.indisvalid FROM pg_index i " <>
          "JOIN pg_class c ON c.oid = i.indexrelid " <>
          "JOIN pg_namespace n ON n.oid = c.relnamespace " <>
          "WHERE n.nspname = 'public' AND c.relname = $1",
        [Atom.to_string(name)],
        log: false
      )

    rows == [[true]]
  end
end
