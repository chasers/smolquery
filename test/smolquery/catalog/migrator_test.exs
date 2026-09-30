defmodule Smolquery.Catalog.MigratorTest do
  @moduledoc """
  The boot step that prepares a Postgres metadata database (T-609), over a
  real one: a fresh database gets DuckLake's tables and every smolquery table
  and index; nodes booting together do the work once; an invalid index a
  failed concurrent build left is rebuilt; a database the old per-connection
  bootstrap made is only recorded; a step that cannot run says so.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.Migrations
  alias Smolquery.Catalog.Migrator
  alias Smolquery.Engine
  alias Smolquery.Test.Postgres

  @versions [20_260_930_110_000, 20_260_930_120_000]
  @tables ~w(smolquery_clustering smolquery_connections smolquery_materialized
             smolquery_partitions smolquery_required_columns smolquery_retention
             smolquery_ring_config)
  @indexes [
    "smolquery_column_table",
    "smolquery_data_file_table",
    "smolquery_file_column_stats_table"
  ]

  describe "options/1" do
    test "prepares Postgres metadata the reader would connect to, with room for the lock" do
      assert {:ok, options} = Migrator.options("postgres:dbname=smolquery host=db user=smol")
      assert options[:pool_size] == 2
      assert options[:hostname] == "db"
      assert options[:parameters] == [search_path: "public"]
    end

    test "has nothing to prepare for SQLite, or a string unlike libpq" do
      assert Migrator.options("sqlite:/tmp/catalog.sqlite") == :none
      assert Migrator.options("postgres:dbname=smolquery") == :none
      assert Migrator.options(nil) == :none
    end
  end

  test "migrations/0 lists the tables, then the indexes on DuckLake's" do
    assert Migrator.migrations() == [
             {20_260_930_110_000, Migrations.SmolqueryTables},
             {20_260_930_120_000, Migrations.DucklakeReadIndexes}
           ]
  end

  test "prepared?/1 is the configured metadata, when the step can prepare it" do
    previous = Application.get_env(:smolquery, DuckLake)
    metadata = "postgres:dbname=smolquery host=db user=smol"
    Application.put_env(:smolquery, DuckLake, metadata: metadata)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:smolquery, DuckLake, previous),
        else: Application.delete_env(:smolquery, DuckLake)
    end)

    assert Migrator.prepared?(metadata)
    refute Migrator.prepared?("postgres:dbname=other host=db user=smol")
    Application.put_env(:smolquery, DuckLake, metadata: "postgres:dbname=smolquery")
    refute Migrator.prepared?("postgres:dbname=smolquery")
  end

  test "prepare_cluster/1 has nothing to do without clustering" do
    assert Migrator.prepare_cluster(enabled: false) == :ok
    assert Migrator.prepare_cluster([]) == :ok
  end

  test "prepare/1 has nothing to do for a SQLite lake" do
    assert Migrator.prepare(metadata: "sqlite:/tmp/never.sqlite", data_path: "/tmp/never") == :ok
    refute File.exists?("/tmp/never.sqlite")
  end

  describe "over a Postgres metadata database" do
    @describetag :integration
    @describetag :tmp_dir

    setup context do
      connection = Postgres.ensure_database!()
      :ok = Postgres.reset_ducklake!(connection)
      on_exit(fn -> Postgres.reset_ducklake!(connection) end)

      config = [metadata: metadata(connection), data_path: Path.join(context.tmp_dir, "data")]

      %{connection: connection, config: config}
    end

    test "prepares a fresh database: DuckLake's tables, then every smolquery table and index",
         context do
      assert Migrator.prepare(context.config) == :ok

      assert "ducklake_snapshot" in tables(context.connection)
      assert Enum.all?(@tables, &(&1 in tables(context.connection)))
      assert indexes(context.connection) == Enum.map(@indexes, &{&1, true})
      assert versions(context.connection) == @versions

      assert Migrator.prepare(context.config) == :ok
      assert versions(context.connection) == @versions
    end

    test "nodes booting together prepare the database once", context do
      results =
        1..3
        |> Enum.map(fn _node -> Task.async(fn -> Migrator.prepare(context.config) end) end)
        |> Enum.map(&Task.await(&1, 120_000))

      assert results == [:ok, :ok, :ok]
      assert versions(context.connection) == @versions
      assert indexes(context.connection) == Enum.map(@indexes, &{&1, true})
    end

    test "rebuilds an index a failed concurrent build left invalid", context do
      :ok = Migrator.prepare(context.config)

      sql!(
        context.connection,
        "UPDATE pg_index SET indisvalid = false " <>
          "WHERE indexrelid = 'public.smolquery_column_table'::regclass"
      )

      sql!(
        context.connection,
        "DELETE FROM smolquery_schema_migrations WHERE version = 20260930120000"
      )

      assert {"smolquery_column_table", false} in indexes(context.connection)
      assert Migrator.prepare(context.config) == :ok
      assert indexes(context.connection) == Enum.map(@indexes, &{&1, true})
    end

    test "records a database the old per-connection bootstrap made, and keeps its rows",
         context do
      old_bootstrap!(context)
      sql!(context.connection, "INSERT INTO smolquery_partitions VALUES ('a', 'b', 4)")

      assert Migrator.prepare(context.config) == :ok
      assert versions(context.connection) == @versions

      assert sql!(context.connection, "SELECT * FROM smolquery_partitions").rows == [
               ["a", "b", 4]
             ]
    end

    test "a node booting into a migrated database attaches nothing", context do
      :ok = Migrator.prepare(context.config)
      unattachable = Keyword.put(context.config, :data_path, "/nonexistent/elsewhere")

      assert Migrator.prepare(unattachable ++ [catalog: "another_lake"]) == :ok
    end

    test "prepares the cluster's database with the tables migration", context do
      assert Migrator.prepare_cluster(enabled: true, postgres: context.connection) == :ok
      assert "smolquery_ring_config" in tables(context.connection)
      assert versions(context.connection) == [hd(@versions)]
    end

    test "a Postgres lake the step does not prepare keeps its bootstrap side tables",
         context do
      engine = __MODULE__.Unprepared

      start_supervised!(
        {DuckLake,
         name: engine, metadata: context.config[:metadata], data_path: context.config[:data_path]}
      )

      assert Enum.all?(
               ~w(smolquery_clustering smolquery_partitions smolquery_connections),
               &(&1 in tables(context.connection))
             )
    end

    test "logs a step that cannot run and answers the error", context do
      config =
        Keyword.update!(
          context.config,
          :metadata,
          &String.replace(&1, "dbname=#{context.connection[:database]}", "dbname=no_such_db")
        )

      log = capture_log(fn -> assert {:error, _reason} = Migrator.prepare(config) end)

      assert log =~ "could not be prepared"
    end
  end

  defp old_bootstrap!(context) do
    engine = __MODULE__.OldLake
    catalog = DuckLake.default_catalog()

    start_supervised!(
      {Engine,
       name: engine,
       extensions: [:postgres, :ducklake],
       statements: [
         DuckLake.attach_statement(
           catalog,
           context.config[:metadata],
           context.config[:data_path]
         ),
         DuckLake.create_clustering_statement(catalog),
         DuckLake.create_partitions_statement(catalog),
         DuckLake.create_connections_statement(catalog),
         DuckLake.create_materialized_statement(catalog),
         DuckLake.create_required_statement(catalog)
       ]},
      id: engine
    )

    stop_supervised!(engine)
  end

  defp tables(connection) do
    %{rows: rows} =
      sql!(connection, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")

    List.flatten(rows)
  end

  defp versions(connection) do
    %{rows: rows} = sql!(connection, "SELECT version FROM smolquery_schema_migrations ORDER BY 1")
    List.flatten(rows)
  end

  defp indexes(connection) do
    %{rows: rows} =
      sql!(
        connection,
        "SELECT c.relname, i.indisvalid FROM pg_index i " <>
          "JOIN pg_class c ON c.oid = i.indexrelid " <>
          "WHERE c.relname LIKE 'smolquery\\_%\\_table' ESCAPE '\\' ORDER BY c.relname"
      )

    Enum.map(rows, &List.to_tuple/1)
  end

  defp sql!(connection, sql) do
    {:ok, conn} = Postgrex.start_link(connection)

    try do
      Postgrex.query!(conn, sql, [])
    after
      GenServer.stop(conn)
    end
  end

  defp metadata(connection) do
    "postgres:dbname=#{connection[:database]} host=#{connection[:hostname]} " <>
      "port=#{connection[:port]} user=#{connection[:username]} " <>
      "password=#{connection[:password]}"
  end
end
