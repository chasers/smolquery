defmodule Smolquery.Catalog.DuckLakeTest do
  @moduledoc """
  The Milestone 2 storage-of-record proof: segments smolquery wrote itself,
  registered in a DuckLake catalog and read back through DuckDB.

  Tagged `:integration` because it downloads the `ducklake` extension on first
  use and writes a real catalog database to disk. The behaviours pinned here
  are the ones the spike found surprising — idempotent registration, snapshot
  pinning, and file-level drops — so a DuckLake upgrade that changes them fails
  loudly rather than silently corrupting row counts.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Lake
  @table {"analytics", "events"}

  setup context do
    start_supervised!(
      {DuckLake,
       name: @engine,
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "data")}
    )

    catalog = DuckLake.new(engine: @engine)
    segments_dir = Path.join(context.tmp_dir, "segments")
    File.mkdir_p!(segments_dir)

    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    %{catalog: catalog, segments_dir: segments_dir}
  end

  defp schema do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}}
    ])
  end

  defp write_segment(dir, day, count) do
    rows =
      for i <- 1..count do
        %{
          "id" => day * 1_000 + i,
          "ts" => NaiveDateTime.new!(Date.new!(2026, 7, day), Time.new!(0, 0, 0)),
          "name" => "row-#{i}",
          "amount" => Decimal.new("#{i}.50")
        }
      end

    {:ok, segment} = Writer.write(rows, schema(), store: Local.new(dir: dir))

    segment
  end

  defp row_count do
    result = Engine.query!(@engine, ~s|SELECT count(*) FROM lake."analytics"."events"|)

    result.rows |> List.flatten() |> List.first()
  end

  describe "datasets and tables" do
    test "creates and lists a dataset", %{catalog: catalog} do
      assert {:ok, datasets} = Catalog.list_datasets(catalog)
      assert "analytics" in datasets
    end

    test "creating a dataset twice is not an error", %{catalog: catalog} do
      assert Catalog.create_dataset(catalog, "analytics") == :ok
    end

    test "creates and lists a table", %{catalog: catalog} do
      assert Catalog.list_tables(catalog, "analytics") == {:ok, ["events"]}
    end

    test "reads back the schema it was given", %{catalog: catalog} do
      assert Catalog.table_schema(catalog, @table) == {:ok, schema()}
    end

    test "reports a table it does not have", %{catalog: catalog} do
      assert Catalog.table_schema(catalog, {"analytics", "missing"}) ==
               {:error, {:unknown_table, {"analytics", "missing"}}}
    end

    test "refuses a name that is not a usable identifier", %{catalog: catalog} do
      assert Catalog.create_dataset(catalog, "bad; DROP TABLE t") ==
               {:error, {:invalid_identifier, "bad; DROP TABLE t"}}

      assert Catalog.create_table(catalog, {"analytics", "bad name"}, schema()) ==
               {:error, {:invalid_identifier, "bad name"}}

      assert Catalog.segments(catalog, {"analytics", ~s(x" --)}, :current) ==
               {:error, {:invalid_identifier, ~s(x" --)}}
    end
  end

  describe "register_segments/3" do
    test "registers externally written Parquet in place", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 1, 100)

      assert {:ok, snapshot} = Catalog.register_segments(catalog, @table, [segment])
      assert is_integer(snapshot)
      assert Catalog.segments(catalog, @table, :current) == {:ok, [segment.path]}
      assert File.exists?(segment.path)
      assert row_count() == 100
    end

    test "registers several segments in one commit", %{catalog: catalog, segments_dir: dir} do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)

      {:ok, before} = Catalog.current_snapshot(catalog)
      assert {:ok, snapshot} = Catalog.register_segments(catalog, @table, [a, b])

      assert snapshot == before + 1
      assert row_count() == 20
    end

    test "re-registering the same segments is a no-op", %{catalog: catalog, segments_dir: dir} do
      segment = write_segment(dir, 1, 50)

      assert {:ok, snapshot} = Catalog.register_segments(catalog, @table, [segment])
      assert {:ok, ^snapshot} = Catalog.register_segments(catalog, @table, [segment])
      assert row_count() == 50
      assert {:ok, [_one]} = Catalog.segments(catalog, @table, :current)
    end

    test "registers only the segments the catalog is missing", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)

      {:ok, _first} = Catalog.register_segments(catalog, @table, [a])
      assert {:ok, _second} = Catalog.register_segments(catalog, @table, [a, b])

      assert row_count() == 20
      assert {:ok, paths} = Catalog.segments(catalog, @table, :current)
      assert Enum.sort(paths) == Enum.sort([a.path, b.path])
    end

    test "deduplicates a repeated segment within one call", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 1, 10)

      assert {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment, segment])
      assert row_count() == 10
    end

    test "rejects a segment whose schema does not match the table", %{
      catalog: catalog,
      segments_dir: dir
    } do
      narrow = Schema.new!([{"id", :int64}])
      {:ok, segment} = Writer.write([%{"id" => 1}], narrow, store: Local.new(dir: dir))

      assert {:error, error} = Catalog.register_segments(catalog, @table, [segment])
      assert Exception.message(error) =~ "not found in file"
      assert row_count() == 0
    end
  end

  describe "segments/3" do
    test "pins a read to a snapshot", %{catalog: catalog, segments_dir: dir} do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)

      {:ok, first} = Catalog.register_segments(catalog, @table, [a])
      {:ok, second} = Catalog.register_segments(catalog, @table, [b])

      assert Catalog.segments(catalog, @table, first) == {:ok, [a.path]}
      assert {:ok, both} = Catalog.segments(catalog, @table, second)
      assert [_first_path, _second_path] = both
    end

    test "is empty for a table with no segments", %{catalog: catalog} do
      assert Catalog.segments(catalog, @table, :current) == {:ok, []}
    end
  end

  describe "reading as of a snapshot (the Milestone 5 planner's sealed side)" do
    test "AT (VERSION => snapshot) pins a table read to a registration's snapshot", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 100)
      b = write_segment(dir, 2, 50)

      {:ok, first} = Catalog.register_segments(catalog, @table, [a])
      {:ok, _second} = Catalog.register_segments(catalog, @table, [b])

      result =
        Engine.query!(
          @engine,
          ~s|SELECT count(*) FROM lake."analytics"."events" AT (VERSION => #{first})|
        )

      assert result.rows == [[100]]
      assert row_count() == 150
    end

    test "a second engine attaches the same lake read-only", %{
      catalog: catalog,
      segments_dir: dir,
      tmp_dir: tmp
    } do
      segment = write_segment(dir, 1, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

      reader = __MODULE__.ReadOnly

      metadata = Identifier.sql_string("ducklake:sqlite:" <> Path.join(tmp, "catalog.sqlite"))
      data_path = Identifier.sql_string(Path.join(tmp, "data"))

      attach =
        "ATTACH #{metadata} AS lake " <>
          "(DATA_PATH #{data_path}, DATA_INLINING_ROW_LIMIT 0, READ_ONLY)"

      start_supervised!({Engine, name: reader, extensions: [:ducklake], statements: [attach]})

      result = Engine.query!(reader, ~s|SELECT count(*) FROM lake."analytics"."events"|)
      assert result.rows == [[10]]

      assert {:error, error} = Engine.query(reader, ~s|DELETE FROM lake."analytics"."events"|)
      assert Exception.message(error) =~ ~r/read.only/i
    end
  end

  describe "known_segments/1" do
    test "is empty for a catalog that has registered nothing", %{catalog: catalog} do
      assert Catalog.known_segments(catalog) == {:ok, []}
    end

    test "reports a registered segment", %{catalog: catalog, segments_dir: dir} do
      segment = write_segment(dir, 1, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

      assert Catalog.known_segments(catalog) == {:ok, [segment.path]}
    end

    test "still reports a segment dropped from the current snapshot", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [a, b])
      {:ok, _dropped} = Catalog.drop_segments(catalog, @table, [a.path])

      assert Catalog.segments(catalog, @table, :current) == {:ok, [b.path]}
      assert {:ok, known} = Catalog.known_segments(catalog)
      assert Enum.sort(known) == Enum.sort([a.path, b.path])
    end

    test "fails loudly on a relative path rather than returning one that matches nothing", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 1, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

      Engine.query!(
        @engine,
        ~s|UPDATE "__ducklake_metadata_lake".ducklake_data_file SET path_is_relative = true|
      )

      assert {:error, {:relative_segment_path, _path}} = Catalog.known_segments(catalog)
    end
  end

  describe "registered_through/3 (the seal-membership read)" do
    test "still lists a path the current snapshot dropped", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, _registered} = Catalog.register_segments(catalog, @table, [a, b])
      {:ok, dropped} = Catalog.drop_segments(catalog, @table, [a.path])

      assert {:ok, through} = Catalog.registered_through(catalog, @table, dropped)
      assert Enum.sort(through) == Enum.sort([a.path, b.path])
    end

    test "excludes a path registered after the asked snapshot", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, first} = Catalog.register_segments(catalog, @table, [a])
      {:ok, _second} = Catalog.register_segments(catalog, @table, [b])

      assert Catalog.registered_through(catalog, @table, first) == {:ok, [a.path]}
    end

    test "is empty for a table that never registered anything", %{catalog: catalog} do
      {:ok, snapshot} = Catalog.current_snapshot(catalog)

      assert Catalog.registered_through(catalog, @table, snapshot) == {:ok, []}
    end
  end

  describe "drop_segments/3" do
    test "removes a segment from the current snapshot but leaves the file", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, registered} = Catalog.register_segments(catalog, @table, [a, b])

      assert {:ok, dropped} = Catalog.drop_segments(catalog, @table, [a.path])

      assert dropped > registered
      assert Catalog.segments(catalog, @table, :current) == {:ok, [b.path]}
      assert File.exists?(a.path)
      assert row_count() == 10
    end

    test "leaves earlier snapshots readable", %{catalog: catalog, segments_dir: dir} do
      segment = write_segment(dir, 1, 10)
      {:ok, registered} = Catalog.register_segments(catalog, @table, [segment])
      {:ok, _dropped} = Catalog.drop_segments(catalog, @table, [segment.path])

      assert Catalog.segments(catalog, @table, registered) == {:ok, [segment.path]}
      assert row_count() == 0
    end

    test "dropping nothing reports the current snapshot", %{catalog: catalog} do
      assert {:ok, snapshot} = Catalog.current_snapshot(catalog)
      assert Catalog.drop_segments(catalog, @table, []) == {:ok, snapshot}
    end
  end

  describe "replace_segments/4" do
    test "one snapshot both adds the replacement and retires the inputs", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      merged = write_segment(dir, 3, 20)
      {:ok, registered} = Catalog.register_segments(catalog, @table, [a, b])

      assert {:ok, swapped} =
               Catalog.replace_segments(catalog, @table, [merged], [a.path, b.path])

      assert swapped == registered + 1
      assert Catalog.segments(catalog, @table, :current) == {:ok, [merged.path]}
      assert row_count() == 20
    end

    test "readers pinned before the swap still see the inputs", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      merged = write_segment(dir, 3, 10)
      {:ok, registered} = Catalog.register_segments(catalog, @table, [a])

      {:ok, _swapped} = Catalog.replace_segments(catalog, @table, [merged], [a.path])

      assert Catalog.segments(catalog, @table, registered) == {:ok, [a.path]}
      assert File.exists?(a.path)

      result =
        Engine.query!(
          @engine,
          ~s|SELECT count(*) FROM lake."analytics"."events" AT (VERSION => #{registered})|
        )

      assert result.rows == [[10]]
    end

    test "a failing addition rolls back the drops that preceded it", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      {:ok, _registered} = Catalog.register_segments(catalog, @table, [a])

      narrow = Schema.new!([{"id", :int64}])
      {:ok, bad} = Writer.write([%{"id" => 1}], narrow, store: Local.new(dir: dir))

      assert {:error, error} = Catalog.replace_segments(catalog, @table, [bad], [a.path])
      assert Exception.message(error) =~ "not found in file"
      assert Catalog.segments(catalog, @table, :current) == {:ok, [a.path]}
      assert row_count() == 10
    end

    test "retrying a committed swap is a no-op that reports the current snapshot", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      merged = write_segment(dir, 3, 10)
      {:ok, _registered} = Catalog.register_segments(catalog, @table, [a])
      {:ok, swapped} = Catalog.replace_segments(catalog, @table, [merged], [a.path])

      assert Catalog.replace_segments(catalog, @table, [merged], [a.path]) == {:ok, swapped}
      assert Catalog.segments(catalog, @table, :current) == {:ok, [merged.path]}
      assert row_count() == 10
    end

    test "refuses to become a drop when there is nothing to add", %{catalog: catalog} do
      assert Catalog.replace_segments(catalog, @table, [], ["/anywhere.parquet"]) ==
               {:error, :no_segments}
    end

    test "with nothing to drop it registers like register_segments/3", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 1, 10)

      assert {:ok, _snapshot} = Catalog.replace_segments(catalog, @table, [segment], [])
      assert Catalog.segments(catalog, @table, :current) == {:ok, [segment.path]}
      assert row_count() == 10
    end
  end

  describe "retention policy" do
    test "round-trips a policy through the metadata database", %{catalog: catalog} do
      assert Catalog.retention(catalog, @table) == {:ok, nil}

      policy = %{column: "ts", ttl_ms: 86_400_000}
      assert Catalog.put_retention(catalog, @table, policy) == :ok
      assert Catalog.retention(catalog, @table) == {:ok, policy}

      replaced = %{column: "ts", ttl_ms: 3_600_000}
      assert Catalog.put_retention(catalog, @table, replaced) == :ok
      assert Catalog.retention(catalog, @table) == {:ok, replaced}

      assert Catalog.put_retention(catalog, @table, nil) == :ok
      assert Catalog.retention(catalog, @table) == {:ok, nil}
    end

    test "policies are per table", %{catalog: catalog} do
      :ok = Catalog.create_table(catalog, {"analytics", "clicks"}, schema())

      policy = %{column: "ts", ttl_ms: 1_000}
      assert Catalog.put_retention(catalog, @table, policy) == :ok
      assert Catalog.retention(catalog, {"analytics", "clicks"}) == {:ok, nil}
    end

    test "refuses a malformed policy", %{catalog: catalog} do
      assert {:error, {:invalid_retention, _policy}} =
               Catalog.put_retention(catalog, @table, %{column: "ts", ttl_ms: 0})

      assert {:error, {:invalid_retention, _policy}} =
               Catalog.put_retention(catalog, @table, %{ttl_ms: 5})
    end

    test "policy rows survive a connection restart", %{catalog: catalog} do
      policy = %{column: "ts", ttl_ms: 86_400_000}
      :ok = Catalog.put_retention(catalog, @table, policy)

      connection = Process.whereis(Engine.connection_name(@engine))
      ref = Process.monitor(connection)
      Process.exit(connection, :kill)
      assert_receive {:DOWN, ^ref, :process, ^connection, :killed}, 1_000

      assert eventually(fn ->
               case Process.whereis(Engine.connection_name(@engine)) do
                 nil -> false
                 ^connection -> false
                 _restarted -> true
               end
             end)

      assert Catalog.retention(catalog, @table) == {:ok, policy}
    end
  end

  describe "clustering key" do
    test "round-trips a key through the metadata database", %{catalog: catalog} do
      assert Catalog.clustering(catalog, @table) == {:ok, []}

      key = ["id", "ts"]
      assert Catalog.put_clustering(catalog, @table, key) == :ok
      assert Catalog.clustering(catalog, @table) == {:ok, key}

      replaced = ["ts"]
      assert Catalog.put_clustering(catalog, @table, replaced) == :ok
      assert Catalog.clustering(catalog, @table) == {:ok, replaced}

      assert Catalog.put_clustering(catalog, @table, []) == :ok
      assert Catalog.clustering(catalog, @table) == {:ok, []}
    end

    test "keys are per table", %{catalog: catalog} do
      :ok = Catalog.create_table(catalog, {"analytics", "clicks"}, schema())

      assert Catalog.put_clustering(catalog, @table, ["id"]) == :ok
      assert Catalog.clustering(catalog, {"analytics", "clicks"}) == {:ok, []}
    end

    test "refuses a malformed key", %{catalog: catalog} do
      assert {:error, {:invalid_clustering, _columns}} =
               Catalog.put_clustering(catalog, @table, ["id", "id"])

      assert {:error, {:invalid_clustering, _columns}} =
               catalog.impl.put_clustering(catalog.config, @table, :not_a_list)
    end

    test "table_schema attaches clustering to the Schema struct", %{catalog: catalog} do
      assert Catalog.put_clustering(catalog, @table, ["id", "ts"]) == :ok

      assert {:ok, schema} = Catalog.table_schema(catalog, @table)
      assert schema.clustering == ["id", "ts"]
    end

    test "key rows survive a connection restart", %{catalog: catalog} do
      key = ["id", "ts"]
      :ok = Catalog.put_clustering(catalog, @table, key)

      connection = Process.whereis(Engine.connection_name(@engine))
      ref = Process.monitor(connection)
      Process.exit(connection, :kill)
      assert_receive {:DOWN, ^ref, :process, ^connection, :killed}, 1_000

      assert eventually(fn ->
               case Process.whereis(Engine.connection_name(@engine)) do
                 nil -> false
                 ^connection -> false
                 _restarted -> true
               end
             end)

      assert Catalog.clustering(catalog, @table) == {:ok, key}
    end
  end

  describe "put_table_options/3" do
    test "applies retention and clustering as one change", %{catalog: catalog} do
      policy = %{column: "ts", ttl_ms: 86_400_000}

      assert Catalog.put_table_options(catalog, @table, %{
               retention: policy,
               clustering: ["id", "ts"]
             }) == :ok

      assert Catalog.retention(catalog, @table) == {:ok, policy}
      assert Catalog.clustering(catalog, @table) == {:ok, ["id", "ts"]}

      assert Catalog.put_table_options(catalog, @table, %{retention: nil, clustering: []}) == :ok

      assert Catalog.retention(catalog, @table) == {:ok, nil}
      assert Catalog.clustering(catalog, @table) == {:ok, []}
    end

    test "an empty options map writes nothing", %{catalog: catalog} do
      assert Catalog.put_table_options(catalog, @table, %{}) == :ok
    end

    test "refuses an option it does not know", %{catalog: catalog} do
      assert {:error, {:unknown_table_option, :compression, :zstd}} =
               Catalog.put_table_options(catalog, @table, %{compression: :zstd})
    end

    test "a failing statement applies neither option", %{catalog: catalog} do
      kept = %{column: "ts", ttl_ms: 1_000}
      :ok = Catalog.put_retention(catalog, @table, kept)

      {:ok, _dropped} =
        Engine.query(
          @engine,
          "DROP TABLE #{Identifier.quote_name!("__ducklake_metadata_lake")}.smolquery_clustering"
        )

      assert {:error, _reason} =
               Catalog.put_table_options(catalog, @table, %{
                 retention: %{column: "ts", ttl_ms: 2_000},
                 clustering: ["id"]
               })

      assert Catalog.retention(catalog, @table) == {:ok, kept}
    end
  end

  describe "expire_snapshots/2 (the T-14 spike for this maintenance function)" do
    test "expires old snapshots over externally-registered files without crashing", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, first} = Catalog.register_segments(catalog, @table, [a])
      {:ok, _second} = Catalog.register_segments(catalog, @table, [b])
      {:ok, _dropped} = Catalog.drop_segments(catalog, @table, [a.path])

      Process.sleep(1_100)

      assert {:ok, expired} = Catalog.expire_snapshots(catalog, 1_000)
      assert expired > 0

      assert {:error, _pinned_read_fails_cleanly} = Catalog.segments(catalog, @table, first)
      assert Catalog.segments(catalog, @table, :current) == {:ok, [b.path]}
      assert row_count() == 10
    end

    test "expiry is what makes a dropped file invisible to known_segments", %{
      catalog: catalog,
      segments_dir: dir
    } do
      a = write_segment(dir, 1, 10)
      b = write_segment(dir, 2, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [a, b])
      {:ok, _dropped} = Catalog.drop_segments(catalog, @table, [a.path])

      assert {:ok, known} = Catalog.known_segments(catalog)
      assert a.path in known

      Process.sleep(1_100)
      assert {:ok, _expired} = Catalog.expire_snapshots(catalog, 1_000)

      assert {:ok, known} = Catalog.known_segments(catalog)
      refute a.path in known
      assert b.path in known
      assert File.exists?(a.path)
    end

    test "never expires the snapshot a current reader would plan against", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 1, 10)
      {:ok, registered} = Catalog.register_segments(catalog, @table, [segment])

      Process.sleep(1_100)
      assert {:ok, _expired} = Catalog.expire_snapshots(catalog, 1_000)

      assert Catalog.segments(catalog, @table, registered) == {:ok, [segment.path]}
      assert row_count() == 10
    end
  end

  describe "query path" do
    test "prunes segments by the stats DuckLake derived from the footers", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segments = Enum.map([1, 10, 20], &write_segment(dir, &1, 100))
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, segments)

      plan =
        Engine.query!(@engine, """
        EXPLAIN ANALYZE
        SELECT count(*) FROM lake."analytics"."events"
         WHERE ts >= TIMESTAMP '2026-07-20 00:00:00'
        """)

      analyzed = plan.rows |> List.flatten() |> Enum.join()

      assert analyzed =~ "Total Files Read: 1"
    end

    test "prunes the same way when the bound is a parameter, not a literal", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segments = Enum.map([1, 10, 20], &write_segment(dir, &1, 100))
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, segments)

      sql = ~s|SELECT count(*) FROM lake."analytics"."events" WHERE ts >= $1|
      bound = ~N[2026-07-20 00:00:00]

      plan = Engine.query!(@engine, "EXPLAIN ANALYZE " <> sql, [bound])
      analyzed = plan.rows |> List.flatten() |> Enum.join()

      assert analyzed =~ "Total Files Read: 1"
      assert Engine.query!(@engine, sql, [bound]) |> Result.one!() == 100
    end

    test "sees the rows a segment carries, with types intact", %{
      catalog: catalog,
      segments_dir: dir
    } do
      segment = write_segment(dir, 5, 3)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

      result =
        Engine.query!(
          @engine,
          ~s|SELECT id, ts, name, amount FROM lake."analytics"."events" ORDER BY id|
        )

      assert result.rows == [
               [5_001, ~N[2026-07-05 00:00:00.000000], "row-1", Decimal.new("1.50")],
               [5_002, ~N[2026-07-05 00:00:00.000000], "row-2", Decimal.new("2.50")],
               [5_003, ~N[2026-07-05 00:00:00.000000], "row-3", Decimal.new("3.50")]
             ]
    end
  end

  describe "attachment" do
    test "survives a connection restart", %{catalog: catalog, segments_dir: dir} do
      segment = write_segment(dir, 1, 10)
      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

      connection = Process.whereis(Engine.connection_name(@engine))
      ref = Process.monitor(connection)
      Process.exit(connection, :kill)
      assert_receive {:DOWN, ^ref, :process, ^connection, :killed}, 1_000

      assert eventually(fn ->
               case Process.whereis(Engine.connection_name(@engine)) do
                 nil -> false
                 ^connection -> false
                 _restarted -> true
               end
             end)

      assert Catalog.segments(catalog, @table, :current) == {:ok, [segment.path]}
    end
  end

  describe "attach_statement/3" do
    test "quotes the catalog name and escapes the literals" do
      statement = DuckLake.attach_statement("lake", "sqlite:/data/it's.sqlite", "/data/seg")

      assert statement ==
               ~s|ATTACH IF NOT EXISTS 'ducklake:sqlite:/data/it''s.sqlite' AS "lake" | <>
                 ~s|(DATA_PATH '/data/seg', DATA_INLINING_ROW_LIMIT 0)|
    end

    test "switches data inlining off so segments are always files" do
      assert DuckLake.attach_statement("lake", "sqlite:/c.sqlite", "/d") =~
               "DATA_INLINING_ROW_LIMIT 0"
    end

    test "leaves automatic migration off by default" do
      refute DuckLake.attach_statement("lake", "sqlite:/c.sqlite", "/d") =~
               "AUTOMATIC_MIGRATION"

      refute DuckLake.attach_statement("lake", "sqlite:/c.sqlite", "/d",
               automatic_migration: false
             ) =~ "AUTOMATIC_MIGRATION"
    end

    test "opts into automatic migration inside the option list" do
      statement =
        DuckLake.attach_statement("lake", "sqlite:/c.sqlite", "/d", automatic_migration: true)

      assert statement =~
               "(DATA_PATH '/d', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION TRUE)"
    end
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() ->
        true

      attempts > 0 ->
        Process.sleep(20)
        eventually(fun, attempts - 1)

      true ->
        false
    end
  end
end
