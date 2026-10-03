defmodule Smolquery.StorageService.SchedulerTest do
  @moduledoc """
  The scheduler against a real DuckLake catalog and a real merge.

  Tagged `:integration` because the swap is the part that can actually be wrong:
  `replace_segments/4` composing registration and retirement in one DuckLake
  transaction, and the post-swap verification that the inlining-off invariant
  held. Faking the catalog would test this module's plumbing and skip both.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Engine.Result
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler
  alias Smolquery.Test.SegmentFixture

  import ExUnit.CaptureLog

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema, do: Schema.new!([{"id", :int64}])

  setup context do
    storage = :"compactor_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake"),
       connections: Runtime.catalog_connections()},
      id: Runtime.catalog_engine(storage)
    )

    for engine <- [Runtime.engine(storage), Runtime.compact_engine(storage)] do
      start_supervised!({Engine, name: engine}, id: engine)
    end

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    %{storage: storage, catalog: catalog}
  end

  defp start_scheduler(context, opts) do
    runtime =
      Runtime.new(
        [
          name: context.storage,
          dir: Path.join(context.tmp_dir, "sealed"),
          catalog: context.catalog,
          engine_extensions: [],
          compact_min_inputs: 2,
          compact_below_bytes: 1_048_576,
          compact_max_bytes: 16_777_216,
          compact_interval_ms: 3_600_000,
          compact_backoff_base_ms: 0,
          compact_target_bytes: nil
        ]
        |> Keyword.merge(opts)
      )

    start_supervised!({Scheduler, runtime}, id: {:scheduler, context.storage})
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(context.storage) end)

    runtime
  end

  defp seal(runtime, catalog, index, range, table \\ @table) do
    {:ok, prefix} = Store.prefix(table)
    rows = for i <- range, do: %{"id" => i}

    {:ok, segment} =
      SegmentFixture.write(rows, schema(),
        store: runtime.store,
        prefix: prefix,
        id: Id.generate(index * 1_000)
      )

    {:ok, _snapshot} = Catalog.register_segments(catalog, table, [segment])

    segment
  end

  defp lake_rows(storage) do
    Runtime.catalog_engine(storage)
    |> Engine.query!(~s|SELECT count(*) FROM lake."analytics"."events"|)
    |> Result.one!()
  end

  test "replaces an undersized run with one merged segment in one snapshot", context do
    runtime = start_scheduler(context, [])
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    c = seal(runtime, context.catalog, 3, 21..30)
    {:ok, before_swap} = Catalog.current_snapshot(context.catalog)

    assert {:ok, report} = Scheduler.sweep(context.storage)

    assert [%{table: @table, replaced: 3, key: key, snapshot: snapshot}] = report.compacted
    assert report.failed == []
    assert snapshot > before_swap

    merged = Store.location(runtime.store, key)
    assert {:ok, inputs} = Catalog.segments(context.catalog, @table, snapshot - 1)
    assert Enum.sort(inputs) == Enum.sort([a.path, b.path, c.path])
    assert Catalog.segments(context.catalog, @table, snapshot) == {:ok, [merged]}
    assert Catalog.segments(context.catalog, @table, :current) == {:ok, [merged]}
    assert lake_rows(context.storage) == 30
    assert Enum.all?([a, b, c], &File.exists?(&1.path))
  end

  defp register_fixture(runtime, catalog, index, schema, rows) do
    {:ok, prefix} = Store.prefix(@table)

    {:ok, segment} =
      SegmentFixture.write(rows, schema,
        store: runtime.store,
        prefix: prefix,
        id: Id.generate(index * 1_000)
      )

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])
    segment
  end

  defp register_written(runtime, catalog, index, schema, rows, dir) do
    {:ok, prefix} = Store.prefix(@table)
    spool = Path.join(dir, "spool-#{index}.ndjson")
    File.write!(spool, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")

    {:ok, segment} =
      Writer.write({:ndjson, [spool]}, schema,
        store: runtime.store,
        engine: Runtime.engine(runtime.name),
        prefix: prefix,
        id: Id.generate(index * 1_000)
      )

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])
    segment
  end

  defp lake_pairs(storage) do
    Runtime.catalog_engine(storage)
    |> Engine.query!(~s|SELECT id, ts_int FROM lake."analytics"."events" ORDER BY id|)
    |> Map.fetch!(:rows)
  end

  test "a legacy input without ids is projected as of its registration snapshot: a re-added name reads NULL (PL-62)",
       context do
    runtime = start_scheduler(context, [])
    catalog = context.catalog
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :int64)})
    wide = Schema.new!([{"id", :int64}, {"ts_int", :int64}])
    register_fixture(runtime, catalog, 1, wide, [%{"id" => 1, "ts_int" => 100}])
    register_fixture(runtime, catalog, 2, wide, [%{"id" => 2, "ts_int" => 200}])

    :ok = Catalog.alter_table(catalog, @table, {:drop_column, "ts_int"})
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :string)})
    assert lake_pairs(context.storage) == [[1, nil], [2, nil]]

    assert {:ok, %{compacted: [_swap], failed: []}} = Scheduler.sweep(context.storage)
    assert lake_pairs(context.storage) == [[1, nil], [2, nil]]
  end

  test "inputs written with ids are projected by id across a drop and a re-add (PL-62)",
       context do
    runtime = start_scheduler(context, [])
    catalog = context.catalog
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :int64)})
    {:ok, before} = Catalog.table_schema(catalog, @table)

    register_written(
      runtime,
      catalog,
      1,
      before,
      [%{"id" => 1, "ts_int" => 100}],
      context.tmp_dir
    )

    :ok = Catalog.alter_table(catalog, @table, {:drop_column, "ts_int"})
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :string)})
    {:ok, after_readd} = Catalog.table_schema(catalog, @table)

    register_written(
      runtime,
      catalog,
      2,
      after_readd,
      [%{"id" => 2, "ts_int" => "x"}],
      context.tmp_dir
    )

    assert {:ok, %{compacted: [_swap], failed: []}} = Scheduler.sweep(context.storage)
    assert lake_pairs(context.storage) == [[1, nil], [2, "x"]]
  end

  test "a materialized column added after the inputs sealed is computed by the compaction (PL-61 L5)",
       context do
    runtime = start_scheduler(context, [])
    catalog = context.catalog
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :int64)})
    {:ok, plain} = Catalog.table_schema(catalog, @table)

    register_written(
      runtime,
      catalog,
      1,
      plain,
      [%{"id" => 1, "ts_int" => 1_700_000_000_000}],
      context.tmp_dir
    )

    register_written(runtime, catalog, 2, plain, [%{"id" => 2, "ts_int" => nil}], context.tmp_dir)

    :ok =
      Catalog.alter_table(
        catalog,
        @table,
        {:add_column, Field.new!("ts", :timestamp, materialized: "epoch_ms(ts_int)")}
      )

    assert lake_stamps(context.storage) == [[1, nil], [2, nil]]

    assert {:ok, %{compacted: [_swap], failed: []}} = Scheduler.sweep(context.storage)
    assert lake_stamps(context.storage) == [[1, ~N[2023-11-14 22:13:20.000000]], [2, nil]]
  end

  test "the chunked merge recomputes a materialized column too (PL-61 L5 review)", context do
    runtime = start_scheduler(context, merge_inputs_per_call: 2)
    catalog = context.catalog
    :ok = Catalog.alter_table(catalog, @table, {:add_column, Field.new!("ts_int", :int64)})
    {:ok, plain} = Catalog.table_schema(catalog, @table)

    for n <- 1..3 do
      register_written(
        runtime,
        catalog,
        n,
        plain,
        [%{"id" => n, "ts_int" => 1_700_000_000_000 + n * 60_000}],
        context.tmp_dir
      )
    end

    :ok =
      Catalog.alter_table(
        catalog,
        @table,
        {:add_column, Field.new!("ts", :timestamp, materialized: "epoch_ms(ts_int)")}
      )

    assert {:ok, %{compacted: [%{replaced: 3}], failed: []}} = Scheduler.sweep(context.storage)

    assert lake_stamps(context.storage) == [
             [1, ~N[2023-11-14 22:14:20.000000]],
             [2, ~N[2023-11-14 22:15:20.000000]],
             [3, ~N[2023-11-14 22:16:20.000000]]
           ]
  end

  defp lake_stamps(storage) do
    Runtime.catalog_engine(storage)
    |> Engine.query!(~s|SELECT id, ts FROM lake."analytics"."events" ORDER BY id|)
    |> Map.fetch!(:rows)
  end

  describe "the fresh tick (T-627)" do
    defp seal_now(runtime, catalog, offset, range) do
      {:ok, prefix} = Store.prefix(@table)

      {:ok, segment} =
        SegmentFixture.write(Enum.map(range, &%{"id" => &1}), schema(),
          store: runtime.store,
          prefix: prefix,
          id: Id.generate(System.os_time(:millisecond) - 60_000 + offset)
        )

      {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])
      segment
    end

    test "merges a quiet table's new seals between sweeps", context do
      runtime = start_scheduler(context, compact_fresh_interval_ms: 3_600_000)
      seal_now(runtime, context.catalog, 1, 1..10)

      assert {:ok, %{compacted: []}} = Scheduler.sweep(context.storage)
      assert MapSet.member?(:sys.get_state(Runtime.scheduler(context.storage)).fresh, @table)

      seal_now(runtime, context.catalog, 2, 11..20)
      seal_now(runtime, context.catalog, 3, 21..30)
      assert {:ok, [_, _, _]} = Catalog.segments(context.catalog, @table, :current)

      send(Runtime.scheduler(context.storage), :fresh)
      _state = :sys.get_state(Runtime.scheduler(context.storage))

      assert {:ok, [_merged]} = Catalog.segments(context.catalog, @table, :current)
      assert lake_rows(context.storage) == 30
    end

    test "leaves a table out of the fresh set when its recent files are not small", context do
      runtime =
        start_scheduler(context,
          compact_fresh_interval_ms: 3_600_000,
          compact_fresh_below_bytes: 1
        )

      seal_now(runtime, context.catalog, 1, 1..10)

      assert {:ok, _report} = Scheduler.sweep(context.storage)
      assert :sys.get_state(Runtime.scheduler(context.storage)).fresh == MapSet.new()
    end
  end

  test "readers pinned before the swap still see the inputs", context do
    runtime = start_scheduler(context, [])
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    {:ok, pinned} = Catalog.current_snapshot(context.catalog)

    assert {:ok, %{compacted: [_swap]}} = Scheduler.sweep(context.storage)

    assert {:ok, paths} = Catalog.segments(context.catalog, @table, pinned)
    assert Enum.sort(paths) == Enum.sort([a.path, b.path])
  end

  describe "the span level (T-592)" do
    @span [
      compact_bucket_ms: 1_000,
      compact_span_ms: 10_000,
      compact_target_bytes: 4_194_304,
      compact_max_rows: 15
    ]

    test "merges settled files across buckets toward the target, past the hour level's row cap",
         context do
      runtime = start_scheduler(context, @span)
      for index <- 1..3, do: seal(runtime, context.catalog, index, (index * 10 - 9)..(index * 10))

      assert {:ok, %{compacted: [%{replaced: 3, rows: 30}], failed: []}} =
               Scheduler.sweep(context.storage)

      assert lake_rows(context.storage) == 30
    end

    test "caps a span group's rows by its estimated decoded size (T-601)", context do
      runtime = start_scheduler(context, Keyword.merge(@span, compact_span_decoded_bytes: 1))

      sealed =
        for index <- 1..3,
            do: seal(runtime, context.catalog, index, (index * 10 - 9)..(index * 10))

      assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)

      assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
      assert Enum.sort(current) == Enum.sort(Enum.map(sealed, & &1.path))
    end

    test "a sweep runs the hour level only while the spill disk is below its floor (T-601)",
         context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span,
            compact_below_bytes: 100,
            compact_spill_floor_bytes: 4_611_686_018_427_387_904
          )
        )

      seal(runtime, context.catalog, 1, 1..10)
      seal(runtime, context.catalog, 2, 11..20)
      ref = :telemetry_test.attach_event_handlers(self(), [[:smolquery, :compact, :span_paused]])

      log =
        capture_log(fn ->
          assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)
        end)

      assert log =~ "span level paused"
      assert_receive {[:smolquery, :compact, :span_paused], ^ref, _count, %{reason: :spill_floor}}
    end

    test "a sweep runs the hour level only while a recycled engine still spills (T-601)",
         context do
      runtime = start_scheduler(context, Keyword.merge(@span, compact_below_bytes: 100))
      seal(runtime, context.catalog, 1, 1..10)
      seal(runtime, context.catalog, 2, 11..20)

      database = Engine.database_name(Runtime.compact_engine(context.storage))
      label = URI.encode(Atom.to_string(database), &URI.char_unreserved?/1)
      leaf = Path.join(Runtime.spill_root(), "#{label}-os#{System.pid()}-db0.1.2")
      File.mkdir_p!(leaf)
      on_exit(fn -> File.rm_rf!(leaf) end)

      log =
        capture_log(fn ->
          assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)
        end)

      assert log =~ "still spills to #{leaf}"

      File.rm_rf!(leaf)

      assert {:ok, %{compacted: [%{level: :span}], failed: []}} =
               Scheduler.sweep(context.storage)
    end

    test "a span group larger than one window merges window by window (T-607)", context do
      :ok = Catalog.put_clustering(context.catalog, @table, ["id"])
      runtime = start_scheduler(context, Keyword.merge(@span, compact_window_decoded_bytes: 1))

      for {index, range} <- [{1, 21..30}, {2, 1..10}, {3, 11..20}],
          do: seal(runtime, context.catalog, index, range)

      assert {:ok, %{compacted: [%{replaced: 3, rows: 30, key: key, level: :span}], failed: []}} =
               Scheduler.sweep(context.storage)

      ids =
        context.storage
        |> Runtime.engine()
        |> Engine.query!("SELECT id FROM read_parquet($1)", [Store.location(runtime.store, key)])
        |> Map.fetch!(:rows)
        |> List.flatten()

      assert ids == Enum.to_list(1..30)
      assert lake_rows(context.storage) == 30
    end

    test "never merges across spans", context do
      runtime = start_scheduler(context, @span)
      a = seal(runtime, context.catalog, 1, 1..10)
      b = seal(runtime, context.catalog, 2, 11..20)
      c = seal(runtime, context.catalog, 11, 21..30)
      d = seal(runtime, context.catalog, 12, 31..40)

      assert {:ok, %{compacted: [%{replaced: 2}]}} = Scheduler.sweep(context.storage)
      assert {:ok, %{compacted: [%{replaced: 2}]}} = Scheduler.sweep(context.storage)
      assert {:ok, %{compacted: []}} = Scheduler.sweep(context.storage)

      assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
      assert [_, _] = current
      refute Enum.any?([a, b, c, d], &(&1.path in current))
      assert lake_rows(context.storage) == 40
    end

    test "merges settled files the hour level would leave alone, under half the target",
         context do
      runtime = start_scheduler(context, Keyword.merge(@span, compact_below_bytes: 100))
      seal(runtime, context.catalog, 1, 1..10)
      seal(runtime, context.catalog, 2, 11..20)

      assert {:ok, %{compacted: [%{replaced: 2, level: :span}], failed: []}} =
               Scheduler.sweep(context.storage)
    end

    test "leaves a file at or past half the target alone", context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span, compact_below_bytes: 64, compact_target_bytes: 130)
        )

      a = seal(runtime, context.catalog, 1, 1..10)
      b = seal(runtime, context.catalog, 2, 11..20)

      assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)

      assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
      assert Enum.sort(current) == Enum.sort([a.path, b.path])
    end

    test "one sweep runs the hour lane and then the span lane for the same table (T-603)",
         context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span, compact_bucket_ms: 3_600_000, compact_span_ms: 86_400_000)
        )

      now = div(System.os_time(:millisecond), 1_000)
      seal(runtime, context.catalog, 1, 1..5)
      seal(runtime, context.catalog, 2, 6..10)
      seal(runtime, context.catalog, now, 11..15)
      seal(runtime, context.catalog, now, 16..20)

      assert {:ok, %{compacted: [hour, span], failed: [], span_waiting: []}} =
               Scheduler.sweep(context.storage)

      assert %{level: :hour, replaced: 2} = hour
      assert %{level: :span, replaced: 2} = span
      assert lake_rows(context.storage) == 20
    end

    test "a call exit on the last table's hour lane skips the span lane (T-603 review)",
         context do
      catalog = DuckLake.new(engine: Runtime.catalog_engine(context.storage), swap_timeout_ms: 1)

      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span,
            catalog: catalog,
            compact_bucket_ms: 3_600_000,
            compact_span_ms: 86_400_000
          )
        )

      now = div(System.os_time(:millisecond), 1_000)
      seal(runtime, context.catalog, 1, 1..5)
      seal(runtime, context.catalog, 2, 6..10)
      seal(runtime, context.catalog, now, 11..15)
      seal(runtime, context.catalog, now, 16..20)

      capture_log(fn ->
        assert {:ok, %{failed: [failure], compacted: []}} = Scheduler.sweep(context.storage)
        refute Map.get(failure, :level) == :span
      end)

      assert {:ok, _snapshot} = Catalog.current_snapshot(Runtime.compaction_catalog(runtime))
    end

    test "a paused span level keeps the span lane's cooldowns (T-603 review)", context do
      start_scheduler(
        context,
        Keyword.merge(@span, compact_spill_floor_bytes: 4_611_686_018_427_387_904)
      )

      scheduler = Runtime.scheduler(context.storage)
      cooling = %{consecutive: 5, retry_at: System.monotonic_time(:millisecond) - 1}
      :sys.replace_state(scheduler, &%{&1 | span_cooldowns: %{@table => cooling}})

      capture_log(fn -> assert {:ok, _report} = Scheduler.sweep(context.storage) end)

      assert %{span_cooldowns: %{@table => ^cooling}} = :sys.get_state(scheduler)
    end

    test "a sweep reports each table's files, the spill space and the span lane's queue (T-603)",
         context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span,
            compact_bucket_ms: 3_600_000,
            compact_span_ms: 86_400_000,
            compact_span_budget_ms: 0
          )
        )

      now = div(System.os_time(:millisecond), 1_000)
      seal(runtime, context.catalog, 1, 1..5)
      seal(runtime, context.catalog, 2, 6..10)
      seal(runtime, context.catalog, now, 11..15)
      ref = :telemetry_test.attach_event_handlers(self(), [[:smolquery, :compact, :swap]])

      assert {:ok, %{span_waiting: [@table]}} = Scheduler.sweep(context.storage)

      metrics = Smolquery.Telemetry.render()
      assert metrics =~ "smolquery_compaction_listed_files 3"
      assert metrics =~ "smolquery_compaction_listed_files_max 3"
      assert metrics =~ ~s|smolquery_compaction_lane_microseconds_total{lane="span"}|
      assert metrics =~ ~s|smolquery_compaction_lane_tables{lane="span",state="waiting"} 1|
      assert metrics =~ "smolquery_compaction_spill_free_bytes "
      refute_received {[:smolquery, :compact, :swap], ^ref, _measurements, _meta}
    end

    test "a learned width shrinks the table's span groups (T-603)", context do
      runtime = start_scheduler(context, Keyword.merge(@span, compact_below_bytes: 100))

      sealed =
        for index <- 1..3,
            do: seal(runtime, context.catalog, index, (index * 10 - 9)..(index * 10))

      :sys.replace_state(Runtime.scheduler(context.storage), fn state ->
        %{state | span_widths: %{@table => div(runtime.compact_span_decoded_bytes, 15)}}
      end)

      assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)
      assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
      assert Enum.sort(current) == Enum.sort(Enum.map(sealed, & &1.path))

      :sys.replace_state(Runtime.scheduler(context.storage), &%{&1 | span_widths: %{}})

      assert {:ok, %{compacted: [%{level: :span, replaced: 3}]}} =
               Scheduler.sweep(context.storage)
    end

    test "a spent span budget leaves the span lane waiting, never the hour lane (T-603)",
         context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span,
            compact_bucket_ms: 3_600_000,
            compact_span_ms: 86_400_000,
            compact_span_budget_ms: 0
          )
        )

      now = div(System.os_time(:millisecond), 1_000)
      seal(runtime, context.catalog, 1, 1..5)
      seal(runtime, context.catalog, 2, 6..10)
      seal(runtime, context.catalog, now, 11..15)
      seal(runtime, context.catalog, now, 16..20)

      assert {:ok, %{compacted: [%{level: :hour}], span_waiting: [@table]}} =
               Scheduler.sweep(context.storage)
    end

    test "keeps recent files at the hour level, under its row cap", context do
      runtime =
        start_scheduler(
          context,
          Keyword.merge(@span, compact_bucket_ms: 3_600_000, compact_span_ms: 86_400_000)
        )

      now = div(System.os_time(:millisecond), 1_000)
      seal(runtime, context.catalog, now, 1..10)
      seal(runtime, context.catalog, now, 11..20)

      assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)
      assert lake_rows(context.storage) == 20
    end
  end

  test "a second sweep finds nothing left to do", context do
    runtime = start_scheduler(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert {:ok, %{compacted: [_swap]}} = Scheduler.sweep(context.storage)
    assert {:ok, %{compacted: [], failed: []}} = Scheduler.sweep(context.storage)
    assert lake_rows(context.storage) == 20
  end

  test "skips a table with fewer segments than the minimum", context do
    runtime = start_scheduler(context, compact_min_inputs: 3)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}
  end

  test "leaves segments at or above the size floor alone", context do
    runtime = start_scheduler(context, compact_below_bytes: 1)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}

    assert {:ok, [_a, _b]} = Catalog.segments(context.catalog, @table, :current)
  end

  test "a ceiling too small for two inputs compacts nothing", context do
    runtime = start_scheduler(context, compact_max_bytes: 1)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}
  end

  test "groups oldest first and leaves what would pass the ceiling", context do
    engine = Runtime.engine(context.storage)
    runtime = start_scheduler(context, [])
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    c = seal(runtime, context.catalog, 3, 21..30)

    sizes =
      engine
      |> Engine.query!(
        "SELECT file_name, sum(total_compressed_size)::BIGINT " <>
          "FROM parquet_metadata([$1, $2, $3]) GROUP BY file_name",
        [a.path, b.path, c.path]
      )
      |> Map.fetch!(:rows)
      |> Map.new(fn [path, bytes] -> {path, bytes} end)

    stop_supervised!({:scheduler, context.storage})

    capped =
      start_scheduler(context,
        compact_max_bytes: Map.fetch!(sizes, a.path) + Map.fetch!(sizes, b.path)
      )

    assert {:ok, %{compacted: [%{replaced: 2, key: key}]}} = Scheduler.sweep(context.storage)

    merged = Store.location(capped.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([merged, c.path])
    assert lake_rows(context.storage) == 30
  end

  test "a backlog past merge_inputs_per_call merges in one sweep, not across sweeps", context do
    runtime = start_scheduler(context, merge_inputs_per_call: 2)

    for n <- 1..5 do
      seal(runtime, context.catalog, n, (n * 10 - 9)..(n * 10))
    end

    assert {:ok, %{compacted: [%{replaced: 5, key: key}]}} = Scheduler.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert current == [merged]
    assert lake_rows(context.storage) == 50

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}
  end

  test "a sweep survives an engine that cannot answer its calls (T-251)", context do
    runtime = start_scheduler(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    stop_supervised!(Runtime.compact_engine(context.storage))

    assert {:ok, %{compacted: [], failed: [failure]}} = Scheduler.sweep(context.storage)
    assert %{table: @table, reason: {:sizing_failed, %CallExited{} = error}} = failure
    assert Exception.message(error) =~ "exited before replying"
    assert is_pid(Process.whereis(Runtime.scheduler(context.storage)))
  end

  test "a sweep recycles the compaction engine after a call exit (T-259)", context do
    runtime = start_scheduler(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    compact_engine = Runtime.compact_engine(context.storage)
    database = Process.whereis(Engine.database_name(compact_engine))
    Process.unregister(Engine.connection_name(compact_engine))

    assert {:ok, %{compacted: [], failed: [failure]}} = Scheduler.sweep(context.storage)
    assert %{table: @table, reason: {:sizing_failed, %CallExited{reason: :noproc}}} = failure

    refute Process.whereis(Engine.database_name(compact_engine)) == database
    assert is_pid(Process.whereis(Engine.connection_name(compact_engine)))

    assert {:ok, %{compacted: [%{replaced: 2}], failed: []}} = Scheduler.sweep(context.storage)
  end

  test "a swap whose transaction times out fails the table without crashing or recycling (T-460)",
       context do
    catalog = DuckLake.new(engine: Runtime.catalog_engine(context.storage), swap_timeout_ms: 1)
    runtime = start_scheduler(context, catalog: catalog)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    compactor = Process.whereis(Runtime.scheduler(context.storage))
    compact_engine = Runtime.compact_engine(context.storage)
    database = Process.whereis(Engine.database_name(compact_engine))

    assert {:ok, %{compacted: [], failed: [failure]}} = Scheduler.sweep(context.storage)
    assert %{table: @table, reason: %CallExited{reason: :timeout}} = failure
    assert Process.alive?(compactor)
    assert Process.whereis(Engine.database_name(compact_engine)) == database

    assert {:ok, _snapshot} = Catalog.current_snapshot(Runtime.compaction_catalog(runtime))
    assert lake_rows(context.storage) == 20
  end

  test "a file the catalog sizes at or above the threshold is never opened, so a corrupt one costs nothing (T-463)",
       context do
    runtime = start_scheduler(context, compact_below_bytes: 1)
    good = seal(runtime, context.catalog, 1, 1..10)
    bad = seal(runtime, context.catalog, 2, 11..20)
    File.write!(bad.path, "not a parquet file")

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}

    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([good.path, bad.path])
  end

  test "a sweep stops at the first call exit and defers the tables behind it (T-460)", context do
    catalog = DuckLake.new(engine: Runtime.catalog_engine(context.storage), swap_timeout_ms: 1)
    runtime = start_scheduler(context, catalog: catalog)
    other = {"analytics", "clicks"}
    :ok = Catalog.create_table(context.catalog, other, schema())
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)
    seal(runtime, context.catalog, 3, 21..30, other)
    seal(runtime, context.catalog, 4, 31..40, other)

    assert {:ok, report} = Scheduler.sweep(context.storage)

    assert [%{table: first, reason: %CallExited{reason: :timeout}}] =
             report.failed

    assert [second] = report.deferred
    assert Enum.sort([first, second]) == Enum.sort([@table, other])
    assert report.cooling == []

    assert {:ok, _snapshot} = Catalog.current_snapshot(Runtime.compaction_catalog(runtime))
    assert {:ok, [_a, _b]} = Catalog.segments(context.catalog, second, :current)

    assert {:ok, %{failed: [%{table: ^first}], deferred: [^second]}} =
             Scheduler.sweep(context.storage)
  end

  test "a catalog listing that exits fails the sweep instead of crashing it (T-460)", context do
    runtime = start_scheduler(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    compactor = Process.whereis(Runtime.scheduler(context.storage))
    Process.unregister(Engine.connection_name(Runtime.catalog_engine(context.storage), 2))

    assert Scheduler.sweep(context.storage) == {:error, %CallExited{reason: :noproc}}

    assert Process.alive?(compactor)
  end

  test "quarantines a segment that fails compaction identically, then stops replanning it (T-310)",
       context do
    runtime = start_scheduler(context, [])
    good = seal(runtime, context.catalog, 1, 1..10)
    bad = seal(runtime, context.catalog, 2, 11..20)

    File.write!(bad.path, "not a parquet file")

    bad_path = bad.path

    for _sweep <- 1..5 do
      assert {:ok, %{compacted: [], failed: [failure]}} = Scheduler.sweep(context.storage)

      assert %{table: @table, reason: {:sizing_failed, %Adbc.Error{}}, paths: [^bad_path]} =
               failure
    end

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [[bad.path]],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}

    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([good.path, bad.path])
  end

  test "rows cap a group the byte ceiling would admit (T-260)", context do
    runtime = start_scheduler(context, compact_max_rows: 20)
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    c = seal(runtime, context.catalog, 3, 21..30)

    assert {:ok, %{compacted: [%{replaced: 2, key: key}]}} = Scheduler.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([merged, c.path])
    refute a.path in current
    refute b.path in current
    assert lake_rows(context.storage) == 30
  end

  test "a row-heavy head no neighbor fits beside cannot wedge the table (T-260)", context do
    runtime = start_scheduler(context, compact_max_rows: 25)
    a = seal(runtime, context.catalog, 1, 1..20)
    b = seal(runtime, context.catalog, 2, 21..30)
    c = seal(runtime, context.catalog, 3, 31..40)

    assert {:ok, %{compacted: [%{replaced: 2, key: key}], failed: []}} =
             Scheduler.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([a.path, merged])
    refute b.path in current
    refute c.path in current
    assert lake_rows(context.storage) == 40
  end

  test "an empty catalog sweeps nothing", context do
    start_scheduler(context, [])

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}
  end

  test "a table this node's storage ring hands to another node is left alone", context do
    runtime = start_scheduler(context, ring: [:"storage1@elsewhere.invalid"])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Scheduler.sweep(context.storage) ==
             {:ok,
              %{
                compacted: [],
                failed: [],
                quarantined: [],
                cooling: [],
                deferred: [],
                span_cooling: [],
                span_waiting: [],
                span_deferred: []
              }}

    assert {:ok, [_a, _b]} = Catalog.segments(context.catalog, @table, :current)
  end

  test "a self-sufficient bucket groups alone; stragglers carry forward (T-269)", context do
    runtime = start_scheduler(context, compact_bucket_ms: 1_000)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 1, 11..20)
    seal(runtime, context.catalog, 2, 21..30)
    seal(runtime, context.catalog, 2, 31..40)

    assert {:ok, report} = Scheduler.sweep(context.storage)
    assert [%{table: @table, replaced: 2, rows: 20}] = report.compacted
    assert {:ok, [_merged, _c, _d]} = Catalog.segments(context.catalog, @table, :current)

    assert {:ok, second} = Scheduler.sweep(context.storage)
    assert [%{table: @table, replaced: 3, rows: 40}] = second.compacted
    assert lake_rows(context.storage) == 40
  end

  test "a bucket below compact_min_inputs rolls into the next owned bucket", context do
    runtime = start_scheduler(context, compact_bucket_ms: 1_000)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert {:ok, report} = Scheduler.sweep(context.storage)
    assert [%{table: @table, replaced: 2}] = report.compacted
    assert lake_rows(context.storage) == 20
  end

  test "an unowned bucket is left for its owner (T-269)", context do
    runtime =
      start_scheduler(context,
        ring: [node(), :"storage1@elsewhere.invalid"],
        compact_bucket_ms: 1_000
      )

    routing = Routing.resolve(context.storage)
    owned? = fn index -> Routing.own?(routing, {@table, index}) end
    mine = Enum.find(1..100, owned?)
    theirs = Enum.find(1..100, &(not owned?.(&1)))

    seal(runtime, context.catalog, mine, 1..10)
    seal(runtime, context.catalog, mine, 11..20)
    a = seal(runtime, context.catalog, theirs, 21..30)
    b = seal(runtime, context.catalog, theirs, 31..40)

    assert {:ok, report} = Scheduler.sweep(context.storage)
    assert [%{table: @table, replaced: 2}] = report.compacted
    assert report.failed == []

    {:ok, current} = Catalog.segments(context.catalog, @table, :current)

    for segment <- [a, b] do
      assert Store.location(runtime.store, segment.key) in current
    end
  end

  describe "the compaction catalog connection (T-458)" do
    test "the scheduler commits through the catalog engine's last connection, never the seal side's",
         context do
      runtime = start_scheduler(context, [])
      seal(runtime, context.catalog, 1, 1..10)
      seal(runtime, context.catalog, 2, 11..20)
      catalog_engine = Runtime.catalog_engine(context.storage)

      assert Runtime.compaction_catalog(runtime).config.engine ==
               {catalog_engine, Runtime.catalog_connections()}

      seal_side = Process.whereis(Engine.connection_name(catalog_engine))
      :ok = :sys.suspend(seal_side)

      try do
        assert {:ok, %{compacted: [%{replaced: 2}], failed: []}} =
                 Scheduler.sweep(context.storage)
      after
        :ok = :sys.resume(seal_side)
      end

      assert lake_rows(context.storage) == 20
    end
  end

  describe "a failing table backs off (T-458)" do
    # A store whose puts fail `failures` times, then work. The merge's final
    # COPY lands through it, so each failure is a `{:put_failed, ...}` —
    # the shape with no recovery of its own, which is what backs off.
    defmodule FlakyPut do
      @behaviour Store

      def new(inner, failures) do
        {:ok, counter} = Agent.start_link(fn -> failures end)

        %Store{impl: __MODULE__, config: {inner, counter}}
      end

      @impl Store
      def put({inner, counter}, key, encoder) do
        if Agent.get_and_update(counter, &{&1 > 0, max(&1 - 1, 0)}),
          do: {:error, {:put_failed, key, :enospc}},
          else: Store.put(inner, key, encoder)
      end

      @impl Store
      def location({inner, _counter}, key), do: Store.location(inner, key)

      @impl Store
      def list({inner, _counter}, prefix), do: Store.list(inner, prefix)

      @impl Store
      def delete({inner, _counter}, key), do: Store.delete(inner, key)

      @impl Store
      def shared?({inner, _counter}), do: Store.shared?(inner)

      @impl Store
      def sweep_staging({inner, _counter}, age_ms), do: Store.sweep_staging(inner, age_ms)
    end

    defp flaky_compactor(context, failures, opts) do
      inner = Store.Local.new(dir: Path.join(context.tmp_dir, "sealed"))
      runtime = start_scheduler(context, [store: FlakyPut.new(inner, failures)] ++ opts)
      seal(%{runtime | store: inner}, context.catalog, 1, 1..10)
      seal(%{runtime | store: inner}, context.catalog, 2, 11..20)

      runtime
    end

    test "a table whose compaction failed is left out of the sweep until the wait passes",
         context do
      flaky_compactor(context, 3, compact_backoff_base_ms: 300, compact_backoff_max_ms: 300)

      assert {:ok, %{failed: [%{table: @table}], cooling: []}} = Scheduler.sweep(context.storage)

      assert Scheduler.sweep(context.storage) ==
               {:ok,
                %{
                  compacted: [],
                  failed: [],
                  quarantined: [],
                  cooling: [@table],
                  deferred: [],
                  span_cooling: [],
                  span_waiting: [],
                  span_deferred: []
                }}

      Process.sleep(350)

      assert {:ok, %{failed: [%{table: @table}], cooling: []}} = Scheduler.sweep(context.storage)
    end

    # The base is well past what a sweep takes on a slow CI runner.
    test "a success clears the cooldown", context do
      flaky_compactor(context, 1,
        compact_backoff_base_ms: 1_500,
        compact_backoff_max_ms: 1_500
      )

      assert {:ok, %{failed: [%{reason: {:put_failed, _key, :enospc}}]}} =
               Scheduler.sweep(context.storage)

      assert {:ok, %{compacted: [], cooling: [@table]}} = Scheduler.sweep(context.storage)

      Process.sleep(1_600)

      assert {:ok, %{compacted: [%{replaced: 2}], cooling: []}} =
               Scheduler.sweep(context.storage)

      assert {:ok, %{compacted: [], failed: [], cooling: []}} = Scheduler.sweep(context.storage)
    end

    test "a base of zero never leaves a table out", context do
      flaky_compactor(context, 3, compact_backoff_base_ms: 0)

      for _sweep <- 1..3 do
        assert {:ok, %{failed: [_failure], cooling: []}} = Scheduler.sweep(context.storage)
      end
    end

    test "a corruption-shaped failure is the quarantine's to stop, and never backs off",
         context do
      runtime =
        start_scheduler(context, compact_backoff_base_ms: 300, compact_backoff_max_ms: 300)

      _good = seal(runtime, context.catalog, 1, 1..10)
      bad = seal(runtime, context.catalog, 2, 11..20)
      File.write!(bad.path, "not a parquet file")

      for _sweep <- 1..2 do
        assert {:ok, %{failed: [%{reason: {:sizing_failed, %Adbc.Error{}}}], cooling: []}} =
                 Scheduler.sweep(context.storage)
      end
    end
  end

  describe "the compaction connection is optional for a catalog handed in (T-458)" do
    test "a catalog on a one-connection engine is shared with the seal side, and compaction runs",
         context do
      storage = :"compactor_single_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {DuckLake,
         name: Runtime.catalog_engine(storage),
         metadata: "sqlite:#{Path.join(context.tmp_dir, "single.sqlite")}",
         data_path: Path.join(context.tmp_dir, "single_lake")},
        id: Runtime.catalog_engine(storage)
      )

      for engine <- [Runtime.engine(storage), Runtime.compact_engine(storage)] do
        start_supervised!({Engine, name: engine}, id: engine)
      end

      catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
      :ok = Catalog.create_dataset(catalog, "analytics")
      :ok = Catalog.create_table(catalog, @table, schema())
      single = %{context | storage: storage, catalog: catalog}

      {runtime, log} = with_log(fn -> start_scheduler(single, []) end)

      assert log =~ "carries no connection 2 for compaction"
      seal(runtime, catalog, 1, 1..10)
      seal(runtime, catalog, 2, 11..20)

      assert {:ok, %{compacted: [%{replaced: 2}], failed: []}} = Scheduler.sweep(storage)
    end
  end
end
