defmodule Smolquery.Catalog.DuckLakePostgresTest do
  @moduledoc """
  Milestone 8 L2: the same DuckLake catalog, tiered onto a real Postgres
  metadata database instead of SQLite (PL-11 D1).

  A DuckLake catalog's identity lives in the metadata database, not the
  DuckDB-side alias it is attached under — two different catalog names
  pointed at the same Postgres database collide (`DATA_PATH` mismatch),
  they do not coexist. So unlike the SQLite suite (a fresh tmp file per
  test), every test here drops the shared `ducklake_*` tables first to get
  a clean catalog, which also means these tests cannot run `async: true`.

  The suite owns a database (`smolquery_test`, created on demand) rather than
  sharing `postgres`. DuckLake creates `ducklake_table_stats` and
  `ducklake_table_column_stats` without primary keys, and Postgres refuses an
  `UPDATE` on a table that a publication covers but no replica identity
  identifies. A developer database carrying a `FOR ALL TABLES` publication —
  logical replication, CDC tooling — therefore fails every commit after a
  table's first one, since the first inserts those stats rows and each later
  one updates them. Publications are per-database, so a database of our own is
  the whole fix.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Lake
  @catalog_name "smolquery_postgres_test"
  @database "smolquery_test"
  @table {"analytics", "events"}

  setup context do
    connection = postgres_connection()
    ensure_database!(connection)
    reset_ducklake_tables!(connection)
    metadata = postgres_metadata(connection)

    start_supervised!(
      {DuckLake,
       name: @engine,
       metadata: metadata,
       data_path: Path.join(context.tmp_dir, "data"),
       catalog: @catalog_name}
    )

    catalog = DuckLake.new(engine: @engine, catalog: @catalog_name)
    segments_dir = Path.join(context.tmp_dir, "segments")
    File.mkdir_p!(segments_dir)

    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    %{catalog: catalog, segments_dir: segments_dir}
  end

  test "loads the postgres extension alongside ducklake" do
    assert {:ok, extensions} =
             Engine.query(@engine, "SELECT extension_name FROM duckdb_extensions() WHERE loaded")

    names = extensions.rows |> List.flatten()

    assert "postgres_scanner" in names
    assert "ducklake" in names
  end

  test "creates a dataset and table through the postgres-backed catalog", %{catalog: catalog} do
    assert {:ok, datasets} = Catalog.list_datasets(catalog)
    assert "analytics" in datasets
    assert Catalog.list_tables(catalog, "analytics") == {:ok, ["events"]}
  end

  test "registers a segment and reads its rows back", %{
    catalog: catalog,
    segments_dir: segments_dir
  } do
    segment = write_segment(segments_dir, 3)

    assert {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])
    assert row_count() == 3
  end

  test "replaces segments atomically (M7's swap primitive, over postgres metadata)", %{
    catalog: catalog,
    segments_dir: segments_dir
  } do
    segment_a = write_segment(segments_dir, 2)
    segment_b = write_segment(segments_dir, 2)
    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment_a, segment_b])
    assert row_count() == 4

    merged = write_segment(segments_dir, 4)

    assert {:ok, _swapped} =
             Catalog.replace_segments(catalog, @table, [merged], [
               segment_a.path,
               segment_b.path
             ])

    assert row_count() == 4
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}])
  end

  defp write_segment(dir, count) do
    rows = for i <- 1..count, do: %{"id" => i}
    {:ok, segment} = Writer.write(rows, schema(), store: Local.new(dir: dir))
    segment
  end

  defp row_count do
    result =
      Engine.query!(@engine, ~s|SELECT count(*) FROM #{@catalog_name}."analytics"."events"|)

    result.rows |> List.flatten() |> List.first()
  end

  defp postgres_connection do
    [
      hostname: System.get_env("TEST_POSTGRES_HOST", "localhost"),
      port: System.get_env("TEST_POSTGRES_PORT", "5432") |> String.to_integer(),
      username: System.get_env("TEST_POSTGRES_USER", "postgres"),
      password: System.get_env("TEST_POSTGRES_PASSWORD", "postgres"),
      database: System.get_env("TEST_POSTGRES_DATABASE", @database)
    ]
  end

  defp postgres_metadata(connection) do
    "postgres:dbname=#{connection[:database]} host=#{connection[:hostname]} " <>
      "port=#{connection[:port]} user=#{connection[:username]} " <>
      "password=#{connection[:password]}"
  end

  # Already existing is the ordinary case and the one thing worth ignoring;
  # anything else — no permission to create, wrong host — is raised here rather
  # than left to resurface as a stranger error from the next connection.
  defp ensure_database!(connection) do
    {:ok, conn} = Postgrex.start_link(Keyword.put(connection, :database, "postgres"))

    result = Postgrex.query(conn, ~s|CREATE DATABASE "#{connection[:database]}"|, [])
    GenServer.stop(conn)

    case result do
      {:ok, _result} -> :ok
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_database}}} -> :ok
      {:error, error} -> raise error
    end
  end

  defp reset_ducklake_tables!(connection) do
    {:ok, conn} = Postgrex.start_link(connection)

    Postgrex.query!(conn, drop_ducklake_tables_sql(), [])
    GenServer.stop(conn)
  end

  defp drop_ducklake_tables_sql do
    """
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT tablename FROM pg_tables
                WHERE schemaname = current_schema() AND tablename LIKE 'ducklake\\_%' ESCAPE '\\'
      LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
      END LOOP;
    END $$;
    """
  end
end
