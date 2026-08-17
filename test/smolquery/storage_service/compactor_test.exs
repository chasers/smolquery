defmodule Smolquery.StorageService.CompactorTest do
  @moduledoc """
  The compactor against a real DuckLake catalog and a real merge.

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
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer
  alias Smolquery.StorageService.Compactor
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

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
       data_path: Path.join(context.tmp_dir, "ducklake")},
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

  defp start_compactor(context, opts) do
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
          compact_interval_ms: 3_600_000
        ] ++ opts
      )

    start_supervised!({Compactor, runtime}, id: {:compactor, context.storage})
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(context.storage) end)

    runtime
  end

  defp seal(runtime, catalog, index, range) do
    {:ok, prefix} = Store.prefix(@table)
    rows = for i <- range, do: %{"id" => i}

    {:ok, segment} =
      Writer.write(rows, schema(),
        store: runtime.store,
        prefix: prefix,
        id: Id.generate(index * 1_000)
      )

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

    segment
  end

  defp lake_rows(storage) do
    Runtime.catalog_engine(storage)
    |> Engine.query!(~s|SELECT count(*) FROM lake."analytics"."events"|)
    |> Result.one!()
  end

  test "replaces an undersized run with one merged segment in one snapshot", context do
    runtime = start_compactor(context, [])
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    c = seal(runtime, context.catalog, 3, 21..30)
    {:ok, before_swap} = Catalog.current_snapshot(context.catalog)

    assert {:ok, report} = Compactor.sweep(context.storage)

    assert [%{table: @table, replaced: 3, key: key, snapshot: snapshot}] = report.compacted
    assert report.failed == []
    assert snapshot == before_swap + 1

    merged = Store.location(runtime.store, key)
    assert Catalog.segments(context.catalog, @table, :current) == {:ok, [merged]}
    assert lake_rows(context.storage) == 30
    assert Enum.all?([a, b, c], &File.exists?(&1.path))
  end

  test "readers pinned before the swap still see the inputs", context do
    runtime = start_compactor(context, [])
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    {:ok, pinned} = Catalog.current_snapshot(context.catalog)

    assert {:ok, %{compacted: [_swap]}} = Compactor.sweep(context.storage)

    assert {:ok, paths} = Catalog.segments(context.catalog, @table, pinned)
    assert Enum.sort(paths) == Enum.sort([a.path, b.path])
  end

  test "a second sweep finds nothing left to do", context do
    runtime = start_compactor(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert {:ok, %{compacted: [_swap]}} = Compactor.sweep(context.storage)
    assert {:ok, %{compacted: [], failed: []}} = Compactor.sweep(context.storage)
    assert lake_rows(context.storage) == 20
  end

  test "skips a table with fewer segments than the minimum", context do
    runtime = start_compactor(context, compact_min_inputs: 3)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
  end

  test "leaves segments at or above the size floor alone", context do
    runtime = start_compactor(context, compact_below_bytes: 1)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
    assert {:ok, [_a, _b]} = Catalog.segments(context.catalog, @table, :current)
  end

  test "a ceiling too small for two inputs compacts nothing", context do
    runtime = start_compactor(context, compact_max_bytes: 1)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
  end

  test "groups oldest first and leaves what would pass the ceiling", context do
    engine = Runtime.engine(context.storage)
    runtime = start_compactor(context, [])
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

    stop_supervised!({:compactor, context.storage})

    capped =
      start_compactor(context,
        compact_max_bytes: Map.fetch!(sizes, a.path) + Map.fetch!(sizes, b.path)
      )

    assert {:ok, %{compacted: [%{replaced: 2, key: key}]}} = Compactor.sweep(context.storage)

    merged = Store.location(capped.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([merged, c.path])
    assert lake_rows(context.storage) == 30
  end

  test "a backlog past merge_inputs_per_call merges in one sweep, not across sweeps", context do
    runtime = start_compactor(context, merge_inputs_per_call: 2)

    for n <- 1..5 do
      seal(runtime, context.catalog, n, (n * 10 - 9)..(n * 10))
    end

    assert {:ok, %{compacted: [%{replaced: 5, key: key}]}} = Compactor.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert current == [merged]
    assert lake_rows(context.storage) == 50

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
  end

  test "a sweep survives an engine that cannot answer its calls (T-251)", context do
    runtime = start_compactor(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    stop_supervised!(Runtime.compact_engine(context.storage))

    assert {:ok, %{compacted: [], failed: [failure]}} = Compactor.sweep(context.storage)
    assert %{table: @table, reason: {:sizing_failed, %CallExited{} = error}} = failure
    assert Exception.message(error) =~ "exited before replying"
    assert is_pid(Process.whereis(Runtime.compactor(context.storage)))
  end

  test "a sweep recycles the compaction engine after a call exit (T-259)", context do
    runtime = start_compactor(context, [])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    compact_engine = Runtime.compact_engine(context.storage)
    database = Process.whereis(Engine.database_name(compact_engine))
    Process.unregister(Engine.connection_name(compact_engine))

    assert {:ok, %{compacted: [], failed: [failure]}} = Compactor.sweep(context.storage)
    assert %{table: @table, reason: {:sizing_failed, %CallExited{reason: :noproc}}} = failure

    refute Process.whereis(Engine.database_name(compact_engine)) == database
    assert is_pid(Process.whereis(Engine.connection_name(compact_engine)))

    assert {:ok, %{compacted: [%{replaced: 2}], failed: []}} = Compactor.sweep(context.storage)
  end

  test "rows cap a group the byte ceiling would admit (T-260)", context do
    runtime = start_compactor(context, compact_max_rows: 20)
    a = seal(runtime, context.catalog, 1, 1..10)
    b = seal(runtime, context.catalog, 2, 11..20)
    c = seal(runtime, context.catalog, 3, 21..30)

    assert {:ok, %{compacted: [%{replaced: 2, key: key}]}} = Compactor.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([merged, c.path])
    refute a.path in current
    refute b.path in current
    assert lake_rows(context.storage) == 30
  end

  test "a row-heavy head no neighbor fits beside cannot wedge the table (T-260)", context do
    runtime = start_compactor(context, compact_max_rows: 25)
    a = seal(runtime, context.catalog, 1, 1..20)
    b = seal(runtime, context.catalog, 2, 21..30)
    c = seal(runtime, context.catalog, 3, 31..40)

    assert {:ok, %{compacted: [%{replaced: 2, key: key}], failed: []}} =
             Compactor.sweep(context.storage)

    merged = Store.location(runtime.store, key)
    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([a.path, merged])
    refute b.path in current
    refute c.path in current
    assert lake_rows(context.storage) == 40
  end

  test "an empty catalog sweeps nothing", context do
    start_compactor(context, [])

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
  end

  test "a table this node's storage ring hands to another node is left alone", context do
    runtime = start_compactor(context, ring: [:"storage1@elsewhere.invalid"])
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 2, 11..20)

    assert Compactor.sweep(context.storage) == {:ok, %{compacted: [], failed: []}}
    assert {:ok, [_a, _b]} = Catalog.segments(context.catalog, @table, :current)
  end

  test "a group never crosses a bucket boundary (T-269)", context do
    runtime = start_compactor(context, compact_bucket_ms: 1_000)
    seal(runtime, context.catalog, 1, 1..10)
    seal(runtime, context.catalog, 1, 11..20)
    seal(runtime, context.catalog, 2, 21..30)
    seal(runtime, context.catalog, 2, 31..40)

    assert {:ok, report} = Compactor.sweep(context.storage)
    assert [%{table: @table, replaced: 2}] = report.compacted
    assert {:ok, [_merged, _c, _d]} = Catalog.segments(context.catalog, @table, :current)

    assert {:ok, second} = Compactor.sweep(context.storage)
    assert [%{table: @table, replaced: 2}] = second.compacted
    assert lake_rows(context.storage) == 40
  end

  test "an unowned bucket is left for its owner (T-269)", context do
    runtime =
      start_compactor(context,
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

    assert {:ok, report} = Compactor.sweep(context.storage)
    assert [%{table: @table, replaced: 2}] = report.compacted
    assert report.failed == []

    {:ok, current} = Catalog.segments(context.catalog, @table, :current)

    for segment <- [a, b] do
      assert Store.location(runtime.store, segment.key) in current
    end
  end

  describe "adjusted_row_caps/3 (T-262)" do
    @resolved 4_194_304

    defp oom_failure(table) do
      {:failed,
       %{
         table: table,
         reason:
           {:put_failed, "analytics/events/x.parquet",
            {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}}}
       }}
    end

    defp staging_oom_failure(table) do
      {:failed,
       %{
         table: table,
         reason: {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}}
       }}
    end

    defp compacted(table, rows) do
      {:ok, %{table: table, key: "k", replaced: 2, rows: rows, snapshot: 1}}
    end

    test "a merge OOM halves the table's cap" do
      caps = Compactor.adjusted_row_caps(%{}, [oom_failure(@table)], @resolved)

      assert caps == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2}}
    end

    test "a staging-phase OOM tightens the cap the same way" do
      caps = Compactor.adjusted_row_caps(%{}, [staging_oom_failure(@table)], @resolved)

      assert caps == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2}}
    end

    test "repeated OOMs keep halving, never below the floor, and grow the patience" do
      caps =
        Enum.reduce(1..30, %{}, fn _sweep, caps ->
          Compactor.adjusted_row_caps(caps, [oom_failure(@table)], @resolved)
        end)

      assert caps == %{@table => %{cap: 65_536, streak: 0, patience: 64}}
    end

    test "cap-filling successes raise the cap after the patience and shed at the resolved cap" do
      caps = %{@table => %{cap: div(@resolved, 4), streak: 0, patience: 2}}
      quarter = compacted(@table, div(@resolved, 4))
      half = compacted(@table, div(@resolved, 2))

      counted = Compactor.adjusted_row_caps(caps, [quarter], @resolved)
      assert counted == %{@table => %{cap: div(@resolved, 4), streak: 1, patience: 2}}

      doubled = Compactor.adjusted_row_caps(counted, [quarter], @resolved)
      assert doubled == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2}}

      counted = Compactor.adjusted_row_caps(doubled, [half], @resolved)
      assert Compactor.adjusted_row_caps(counted, [half], @resolved) == %{}
    end

    test "a small group's success is not evidence for a raise" do
      caps = %{@table => %{cap: div(@resolved, 4), streak: 1, patience: 2}}

      assert Compactor.adjusted_row_caps(caps, [compacted(@table, 100)], @resolved) == caps
    end

    test "a cap that makes every plan skip still earns its raise, so the floor unwedges" do
      caps = %{@table => %{cap: 65_536, streak: 0, patience: 2}}

      counted = Compactor.adjusted_row_caps(caps, [{:skip, @table}], @resolved)
      doubled = Compactor.adjusted_row_caps(counted, [{:skip, @table}], @resolved)

      assert doubled == %{@table => %{cap: 131_072, streak: 0, patience: 2}}
    end

    test "a success or skip without an override changes nothing" do
      assert Compactor.adjusted_row_caps(%{}, [compacted(@table, 100)], @resolved) == %{}
      assert Compactor.adjusted_row_caps(%{}, [{:skip, @table}], @resolved) == %{}
    end

    test "a failure that is not a merge OOM changes nothing" do
      timeout =
        {:failed,
         %{
           table: @table,
           reason:
             {:put_failed, "analytics/events/x.parquet",
              {:merge_failed, %Smolquery.Engine.CallExited{reason: :timeout}}}
         }}

      sizing = {:failed, %{table: @table, reason: {:sizing_failed, %Adbc.Error{message: "x"}}}}

      assert Compactor.adjusted_row_caps(%{}, [timeout, sizing, :skip], @resolved) == %{}
    end
  end

  describe "engine_call_exited?/1" do
    test "matches bare sizing and merge exits" do
      exit = %CallExited{reason: :timeout}

      assert Compactor.engine_call_exited?({:sizing_failed, exit})
      assert Compactor.engine_call_exited?({:merge_failed, exit})
    end

    test "matches a final COPY's exit, which the store wraps" do
      wrapped =
        {:put_failed, "analytics/events/x.parquet",
         {:merge_failed, %CallExited{reason: :timeout}}}

      assert Compactor.engine_call_exited?(wrapped)
    end

    test "does not match plain errors" do
      refute Compactor.engine_call_exited?({:merge_failed, %Adbc.Error{message: "x"}})

      refute Compactor.engine_call_exited?(
               {:put_failed, "x.parquet", {:merge_failed, %Adbc.Error{message: "x"}}}
             )
    end
  end
end
