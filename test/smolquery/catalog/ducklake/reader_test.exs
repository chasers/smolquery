defmodule Smolquery.Catalog.DuckLake.ReaderTest do
  @moduledoc """
  The Postgrex read path (T-608): its configuration and type rebuilding
  alone, then every read it answers against the DuckDB path's answer over
  one real Postgres-backed lake, at the current and a pinned snapshot.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.DuckLake.Reader
  alias Smolquery.Catalog.Migrator
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.Postgres
  alias Smolquery.Test.SegmentFixture

  describe "libpq_options/1" do
    test "reads bare and quoted values, the form DatabaseUrl writes" do
      assert {:ok, options} =
               Reader.libpq_options(
                 "dbname=smolquery host='catalog.internal' port=5433 " <>
                   "user='smol' password='it\\'s a \\\\ secret'"
               )

      assert Enum.sort(options) ==
               Enum.sort(
                 database: "smolquery",
                 hostname: "catalog.internal",
                 port: 5433,
                 username: "smol",
                 password: "it's a \\ secret",
                 ssl: false
               )
    end

    test "maps sslmode disable to plaintext and require to TLS without verification" do
      base = "dbname=smolquery host=db user=smol"

      assert {:ok, disabled} = Reader.libpq_options(base <> " sslmode=disable")
      assert disabled[:ssl] == false
      assert {:ok, required} = Reader.libpq_options(base <> " sslmode=require")
      assert required[:ssl] == [verify: :verify_none]
    end

    test "refuses what Postgrex would not connect to as libpq does" do
      assert Reader.libpq_options("dbname=smolquery host=db user=smol sslmode=verify-full") ==
               :error

      assert Reader.libpq_options("dbname=smolquery host=/run/postgresql user=smol") == :error
      assert Reader.libpq_options("dbname=smolquery user=smol") == :error
      assert Reader.libpq_options("dbname=smolquery host=db") == :error
      assert Reader.libpq_options("host=db user=smol") == :error
      assert Reader.libpq_options("dbname=smolquery host=db user=smol port=fifty") == :error

      assert Reader.libpq_options("dbname=smolquery host=db user=smol application_name=x") ==
               :error

      assert Reader.libpq_options("password='unterminated") == :error
    end
  end

  describe "options/2" do
    test "reads Postgres metadata with the configured pool size, queueing and search path" do
      assert {:ok, options} =
               Reader.options("postgres:dbname=smolquery host=db user=smol", pool_size: 7)

      assert options[:pool_size] == 7
      assert options[:hostname] == "db"
      assert options[:queue_target] == 10_000
      assert options[:parameters] == [search_path: "public"]
    end

    test "stays in DuckDB for SQLite metadata, a reader switched off, or unlike libpq" do
      assert Reader.options("sqlite:/tmp/catalog.sqlite", []) == :none

      assert Reader.options("postgres:dbname=smolquery host=db user=smol", enabled: false) ==
               :none

      assert Reader.options("postgres:dbname=smolquery host=db user=smol sslmode=verify-ca", []) ==
               :none

      assert Reader.options("postgres:dbname=smolquery", []) == :none
      assert Reader.options(nil, []) == :none
    end
  end

  describe "column_rows/1" do
    test "rebuilds DuckDB's type names from DuckLake's, nested columns included" do
      rows = [
        [1, "id", "int64", false, 2, nil],
        [2, "price", "decimal(18,3)", true, 2, nil],
        [3, "labels", "map", true, 3, nil],
        [4, "key", "varchar", true, 3, 3],
        [5, "value", "varchar", true, 3, 3],
        [6, "tags", "list", true, 4, nil],
        [7, "element", "int32", true, 4, 6],
        [8, "body", "variant", true, 5, nil]
      ]

      assert Reader.column_rows(rows) == [
               ["id", "BIGINT", "NO", 1, 2],
               ["price", "DECIMAL(18,3)", "YES", 2, 2],
               ["labels", "MAP(VARCHAR, VARCHAR)", "YES", 3, 3],
               ["tags", "INTEGER[]", "YES", 6, 4],
               ["body", "VARIANT", "YES", 8, 5]
             ]
    end
  end

  test "pool/1 names a pool per engine" do
    assert Reader.pool(Some.Engine) == Some.Engine.Reader
  end

  describe "against the DuckDB path, over a Postgres-backed lake" do
    @describetag :integration
    @describetag :tmp_dir

    @engine __MODULE__.Lake
    @lake "smolquery_reader_test"
    @events {"analytics", "events"}
    @typed {"analytics", "typed"}
    @variant {"analytics", "bodies"}
    @owned {"analytics", "owned"}

    setup context do
      connection = Postgres.ensure_database!()
      :ok = Postgres.reset_ducklake!(connection)
      on_exit(fn -> Postgres.reset_ducklake!(connection) end)

      lake = [
        metadata: metadata(connection),
        data_path: Path.join(context.tmp_dir, "data"),
        catalog: @lake
      ]

      :ok = Migrator.prepare(lake)
      {catalog, opts} = DuckLake.resolve(lake ++ [connections: 2], @engine)

      for child <- DuckLake.children(opts, @engine), do: start_supervised!(child)

      duckdb = %{catalog | config: %{catalog.config | reader: nil}}
      store = Local.new(dir: Path.join(context.tmp_dir, "segments"))

      %{catalog: catalog, duckdb: duckdb, store: store}
    end

    test "resolve/2 hands Postgres metadata a reader pool", %{catalog: catalog} do
      assert catalog.config.reader == Reader.pool(@engine)
      assert is_pid(Process.whereis(Reader.pool(@engine)))
    end

    test "every read answers what DuckDB answers, current and pinned", context do
      %{catalog: catalog, duckdb: duckdb, store: store} = context
      fixture!(catalog, store)

      assert {:ok, current} = Catalog.current_snapshot(catalog)
      assert Catalog.current_snapshot(duckdb) == {:ok, current}
      assert Catalog.schema_version(catalog) == Catalog.schema_version(duckdb)

      for table <- [@events, @typed, @variant, {"analytics", "missing"}] do
        assert Catalog.table_schema(catalog, table) == Catalog.table_schema(duckdb, table)
      end

      for table <- [@events, @variant], snapshot <- [current, pinned(context)] do
        assert sorted(Catalog.registered_through(catalog, table, snapshot)) ==
                 sorted(Catalog.registered_through(duckdb, table, snapshot))

        assert Catalog.segment_stats(catalog, table, snapshot) ==
                 Catalog.segment_stats(duckdb, table, snapshot)

        assert Catalog.segment_files(catalog, table, snapshot) ==
                 Catalog.segment_files(duckdb, table, snapshot)
      end

      assert {:ok, [_ | _]} = Catalog.segment_files(catalog, @variant, current)
    end

    test "a reader that cannot answer returns an error, never an exit or a DuckDB answer",
         context do
      %{duckdb: duckdb, store: store} = context
      fixture!(duckdb, store)
      unreachable = %{duckdb | config: %{duckdb.config | reader: __MODULE__.NoSuchPool}}
      {:ok, snapshot} = Catalog.current_snapshot(duckdb)

      assert {:error, {:reader_exited, _noproc}} = Catalog.current_snapshot(unreachable)
      assert {:error, {:reader_exited, _noproc}} = Catalog.table_schema(unreachable, @typed)

      assert {:error, {:reader_exited, _noproc}} =
               Catalog.segment_files(unreachable, @events, snapshot)
    end

    test "check/1 reads through the pool, and logs an error when it cannot" do
      assert Reader.check(Reader.pool(@engine)) == :ok

      log =
        capture_log(fn ->
          assert {:error, {:reader_exited, _noproc}} = Reader.check(__MODULE__.NoSuchPool)
        end)

      assert log =~ "cannot read the catalog"
      assert log =~ "SMOLQUERY_CATALOG_READER=false"
    end

    test "a relative path DuckLake wrote itself is refused on both paths", context do
      %{catalog: catalog, duckdb: duckdb} = context
      :ok = Catalog.create_dataset(catalog, "analytics")
      :ok = Catalog.create_table(catalog, @owned, Schema.new!([{"id", :int64}]))
      Engine.query!(@engine, ~s|INSERT INTO #{@lake}."analytics"."owned" VALUES (1)|)
      {:ok, snapshot} = Catalog.current_snapshot(catalog)

      assert {:error, {:relative_segment_path, _path}} =
               Catalog.registered_through(catalog, @owned, snapshot)

      assert Catalog.registered_through(catalog, @owned, snapshot) ==
               Catalog.registered_through(duckdb, @owned, snapshot)

      assert Catalog.segment_files(catalog, @owned, snapshot) ==
               Catalog.segment_files(duckdb, @owned, snapshot)
    end

    defp fixture!(catalog, store) do
      :ok = Catalog.create_dataset(catalog, "analytics")

      :ok =
        Catalog.create_table(catalog, @events, Schema.new!([{"id", :int64}, {"name", :string}]))

      :ok = Catalog.put_clustering(catalog, @events, ["id"])
      :ok = Catalog.put_partitions(catalog, @events, 2)

      :ok =
        Catalog.create_table(
          catalog,
          @typed,
          Schema.new!([
            Field.new!("id", :int64, nullable: false),
            {"price", {:numeric, 18, 3}},
            {"seen", :bool},
            {"ts", :timestamp},
            {"ts_ns", :timestamp_ns},
            {"day", :date},
            {"score", :float64},
            {"labels", {:map, :string, :string}},
            {"body", :variant},
            Field.new!("seen_day", :date, materialized: "CAST(ts AS DATE)", nullable: false)
          ])
        )

      :ok =
        Catalog.create_table(catalog, @variant, Schema.new!([{"id", :int64}, {"body", :variant}]))

      [first, second, third] =
        for n <- 1..3 do
          segment = seal!(store, n)
          {:ok, _snapshot} = Catalog.register_segments(catalog, @events, [segment])
          segment
        end

      {:ok, pinned} = Catalog.current_snapshot(catalog)
      Process.put(:pinned, pinned)
      merged = seal!(store, 4, 1..20)

      {:ok, _snapshot} =
        Catalog.replace_segments(catalog, @events, [merged], [first.path, second.path])

      {:ok, _snapshot} = Catalog.drop_segments(catalog, @events, [third.path])
      {:ok, _snapshot} = Catalog.register_segments(catalog, @events, [seal!(store, 5)])
      {:ok, _snapshot} = Catalog.register_segments(catalog, @variant, [variant_file!(store)])
    end

    defp pinned(_context), do: Process.get(:pinned)

    defp seal!(store, n, range \\ nil) do
      range = range || (n * 10 - 9)..(n * 10)
      rows = for i <- range, do: %{"id" => i, "name" => "row #{i}"}

      {:ok, segment} =
        SegmentFixture.write(rows, Schema.new!([{"id", :int64}, {"name", :string}]),
          store: store,
          prefix: "analytics/events"
        )

      segment
    end

    defp variant_file!(store) do
      path =
        Path.join([
          store.config.dir,
          "analytics",
          "bodies",
          "01KYWPEEGAM8FQVQS5S2QF26V1.parquet"
        ])

      File.mkdir_p!(Path.dirname(path))

      Engine.query!(
        @engine,
        "COPY (SELECT range::BIGINT AS id, json_object('n', range)::JSON AS body " <>
          "FROM range(5)) TO $1 (FORMAT PARQUET)",
        [path]
      )

      %Smolquery.Segments.Segment{
        id: "01KYWPEEGAM8FQVQS5S2QF26V1",
        key: "",
        path: path,
        row_count: 5,
        byte_size: File.stat!(path).size
      }
    end

    defp sorted({:ok, paths}), do: {:ok, Enum.sort(paths)}
    defp sorted(other), do: other

    defp metadata(connection) do
      "postgres:dbname=#{connection[:database]} host=#{connection[:hostname]} " <>
        "port=#{connection[:port]} user=#{connection[:username]} " <>
        "password=#{connection[:password]}"
    end
  end
end
