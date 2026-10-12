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

  @versions [20_260_930_110_000, 20_260_930_120_000, 20_261_001_100_000]
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

  test "migrations/0 lists the tables, the indexes on DuckLake's, then connection kinds" do
    assert Migrator.migrations() == [
             {20_260_930_110_000, Migrations.SmolqueryTables},
             {20_260_930_120_000, Migrations.DucklakeReadIndexes},
             {20_261_001_100_000, Migrations.ConnectionKinds}
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

  test "lake_config/0 takes the flag from the Migrator, and drops one left under DuckLake (T-651)" do
    previous =
      {Application.get_env(:smolquery, DuckLake), Application.get_env(:smolquery, Migrator)}

    lake = [metadata: "postgres:dbname=smolquery host=db user=smol", data_path: "/lake"]

    on_exit(fn ->
      for {key, value} <- Enum.zip([DuckLake, Migrator], Tuple.to_list(previous)) do
        if value,
          do: Application.put_env(:smolquery, key, value),
          else: Application.delete_env(:smolquery, key)
      end
    end)

    Application.put_env(:smolquery, DuckLake, lake)
    Application.put_env(:smolquery, Migrator, automatic_migration: true)
    assert Migrator.lake_config() == lake ++ [automatic_migration: true]

    Application.put_env(:smolquery, DuckLake, lake ++ [automatic_migration: true])
    Application.delete_env(:smolquery, Migrator)

    log = capture_log(fn -> assert Migrator.lake_config() == lake end)

    assert log =~
             "ignoring :automatic_migration under config :smolquery, Smolquery.Catalog.DuckLake"
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

    test "migrates an older DuckLake format only when allowed (T-639)", context do
      :ok = Migrator.prepare(context.config)
      log_info()
      set_format!(context.connection, "0.4")

      log =
        capture_log(fn ->
          assert Migrator.prepare(context.config) == {:error, {:catalog_format, "0.4", "1.0"}}
        end)

      assert log =~ ~s(its DuckLake catalog format is "0.4")
      assert format(context.connection) == "0.4"

      log =
        capture_log(fn ->
          assert Migrator.prepare(context.config ++ [automatic_migration: true]) == :ok
        end)

      assert log =~ "catalog DuckLake format migrated from 0.4 to 1.0"
      assert format(context.connection) == "1.0"
    end

    test "nodes booting together migrate the format once, under the lock (T-639)", context do
      :ok = Migrator.prepare(context.config)
      log_info()
      set_format!(context.connection, "0.4")
      config = context.config ++ [automatic_migration: true]

      log =
        capture_log(fn ->
          results =
            1..3
            |> Enum.map(fn _node -> Task.async(fn -> Migrator.prepare(config) end) end)
            |> Task.await_many(60_000)

          assert results == [:ok, :ok, :ok]
        end)

      assert [_before, _after] = String.split(log, "format migrated from 0.4 to 1.0")
      assert format(context.connection) == "1.0"
    end

    test "a catalog newer than this release fails the boot and is left as it is", context do
      :ok = Migrator.prepare(context.config)
      set_format!(context.connection, "1.1-dev1")

      log =
        capture_log(fn ->
          assert Migrator.prepare(context.config) ==
                   {:error, {:catalog_format, "1.1-dev1", "1.0"}}
        end)

      assert log =~ ~s(its DuckLake catalog format is "1.1-dev1")

      log =
        capture_log(fn ->
          assert {:error, {:attach_failed, _refused}} =
                   Migrator.prepare(context.config ++ [automatic_migration: true])
        end)

      assert log =~ "could not be prepared"

      assert format(context.connection) == "1.1-dev1"
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

  describe "a lake the step does not prepare" do
    @describetag :integration
    @describetag :tmp_dir

    test "is attached once to migrate its format, only when allowed", context do
      metadata = Path.join(context.tmp_dir, "catalog.sqlite")
      config = [metadata: "sqlite:" <> metadata, data_path: Path.join(context.tmp_dir, "data")]

      assert Migrator.prepare(config) == :ok
      refute File.exists?(metadata)

      assert Migrator.prepare(config ++ [automatic_migration: true]) == :ok
      assert File.exists?(metadata)
    end
  end

  defp log_info do
    Logger.put_module_level(Migrator, :info)
    on_exit(fn -> Logger.delete_module_level(Migrator) end)
  end

  defp set_format!(connection, version),
    do:
      sql!(connection, "UPDATE ducklake_metadata SET value = '#{version}' WHERE key = 'version'")

  defp format(connection) do
    %{rows: [[version]]} =
      sql!(connection, "SELECT value FROM ducklake_metadata WHERE key = 'version'")

    version
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
