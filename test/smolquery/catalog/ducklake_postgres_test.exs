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
  @table {"analytics", "events"}

  setup context do
    metadata = postgres_metadata()
    reset_ducklake_tables!(metadata)

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

  defp postgres_metadata do
    hostname = System.get_env("TEST_POSTGRES_HOST", "localhost")
    port = System.get_env("TEST_POSTGRES_PORT", "5432")
    username = System.get_env("TEST_POSTGRES_USER", "postgres")
    password = System.get_env("TEST_POSTGRES_PASSWORD", "postgres")
    database = System.get_env("TEST_POSTGRES_DATABASE", "postgres")

    "postgres:dbname=#{database} host=#{hostname} port=#{port} " <>
      "user=#{username} password=#{password}"
  end

  defp reset_ducklake_tables!("postgres:" <> params) do
    opts =
      params
      |> String.split(" ", trim: true)
      |> Map.new(fn pair ->
        [key, value] = String.split(pair, "=", parts: 2)
        {String.to_existing_atom(translate_key(key)), value}
      end)

    {:ok, conn} =
      Postgrex.start_link(
        hostname: opts.hostname,
        port: String.to_integer(opts.port),
        username: opts.username,
        password: opts.password,
        database: opts.database
      )

    Postgrex.query!(conn, drop_ducklake_tables_sql(), [])
    GenServer.stop(conn)
  end

  defp translate_key("dbname"), do: "database"
  defp translate_key("host"), do: "hostname"
  defp translate_key("user"), do: "username"
  defp translate_key(key), do: key

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
