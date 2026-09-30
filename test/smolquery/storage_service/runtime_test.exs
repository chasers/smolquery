defmodule Smolquery.StorageService.RuntimeTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Smolquery.Catalog
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.StubCatalog

  describe "new/1" do
    test "derives a local store from :dir" do
      runtime = Runtime.new(name: __MODULE__.Derived, dir: "/tmp/sealed")

      assert %Store{impl: Store.Local, config: %{dir: "/tmp/sealed"}} = runtime.store
    end

    test "takes a store outright when given one" do
      runtime =
        Runtime.new(
          name: __MODULE__.Given,
          store: {Store.Local, [dir: "/mnt/bulk", fsync: false]}
        )

      assert %Store{impl: Store.Local, config: %{dir: "/mnt/bulk", fsync: false}} = runtime.store
    end

    test "opts override application config" do
      runtime = Runtime.new(name: __MODULE__.Overridden, max_concurrent_seals: 9)

      assert runtime.max_concurrent_seals == 9
    end

    test "inherits application config for what opts leave out" do
      configured = Application.get_env(:smolquery, Smolquery.StorageService, [])
      runtime = Runtime.new(name: __MODULE__.Inherited)

      assert runtime.buffer_base_url == Keyword.fetch!(configured, :buffer_base_url)
      assert runtime.gc_grace_ms == Keyword.fetch!(configured, :gc_grace_ms)
    end

    test "defaults the instance name" do
      assert Runtime.new().name == Smolquery.StorageService
    end

    test "wires a DuckLake catalog to this instance's own engine" do
      runtime = Runtime.new(name: __MODULE__.Lake)

      assert %Catalog{impl: Catalog.DuckLake, config: %{engine: engine}} = runtime.catalog
      assert engine == Runtime.catalog_engine(__MODULE__.Lake)
      assert runtime.catalog_opts == [connections: Runtime.catalog_connections()]
    end

    test "passes catalog options through for the supervisor to start" do
      opts = [metadata: "sqlite:/tmp/c.sqlite", data_path: "/tmp/lake"]
      runtime = Runtime.new(name: __MODULE__.LakeOpts, catalog: opts)

      assert runtime.catalog_opts == [connections: Runtime.catalog_connections()] ++ opts
    end

    test "takes a catalog handle outright, and then starts none" do
      catalog = StubCatalog.new(self())
      runtime = Runtime.new(name: __MODULE__.Given, catalog: catalog)

      assert runtime.catalog == catalog
      assert runtime.catalog_opts == nil
    end

    test "defaults the buffer instance it retires against" do
      assert Runtime.new(name: __MODULE__.Buffered).buffer_name == Smolquery.BufferService
    end

    test "refuses an unsupported compression codec at boot, not per seal attempt" do
      assert_raise ArgumentError, ~r/unsupported sealed-segment compression/, fn ->
        Runtime.new(name: __MODULE__.BadCodec, compression: :lz4)
      end
    end

    test "refuses a non-positive compact_bucket_ms at boot, not per sweep (T-269)" do
      assert_raise ArgumentError, ~r/unsupported compact_bucket_ms/, fn ->
        Runtime.new(name: __MODULE__.BadBucket, compact_bucket_ms: 0)
      end
    end

    test "refuses a non-positive seal_row_group_size at boot, not per seal attempt" do
      assert_raise ArgumentError, ~r/unsupported seal_row_group_size/, fn ->
        Runtime.new(name: __MODULE__.BadRowGroup, seal_row_group_size: 0)
      end
    end

    test "refuses a malformed compact_min_inputs at boot, naming the right key" do
      for min <- [nil, 0, "2"] do
        assert_raise ArgumentError, ~r/unsupported compact_min_inputs/, fn ->
          Runtime.new(name: __MODULE__.BadCompactMin, compact_min_inputs: min)
        end
      end
    end

    test "refuses an unusable merge_inputs_per_call at boot, not at first merge" do
      for per_call <- [0, -1, "12", nil] do
        assert_raise ArgumentError, ~r/unsupported merge_inputs_per_call/, fn ->
          Runtime.new(name: __MODULE__.BadMergeCap, merge_inputs_per_call: per_call)
        end
      end
    end

    test "the merge's three call budgets default, and take an override each" do
      runtime = Runtime.new(name: __MODULE__.MergeBudgets)

      assert runtime.merge_copy_timeout_ms == 300_000
      assert runtime.merge_staging_timeout_ms == 120_000
      assert runtime.merge_describe_timeout_ms == 120_000

      raised =
        Runtime.new(
          name: __MODULE__.RaisedMergeBudgets,
          merge_copy_timeout_ms: 900_000,
          merge_staging_timeout_ms: 600_000,
          merge_describe_timeout_ms: 300_000
        )

      assert raised.merge_copy_timeout_ms == 900_000
      assert raised.merge_staging_timeout_ms == 600_000
      assert raised.merge_describe_timeout_ms == 300_000
    end

    test "refuses an unusable merge call budget at boot, naming the key (T-335)" do
      for key <- [:merge_copy_timeout_ms, :merge_staging_timeout_ms, :merge_describe_timeout_ms],
          ms <- [0, -1, "300000", nil] do
        assert_raise ArgumentError, ~r/unsupported #{key}/, fn ->
          Runtime.new([{:name, __MODULE__.BadMergeBudget}, {key, ms}])
        end
      end
    end

    test "refuses a non-string engine_memory_limit at boot, not at engine start" do
      for limit <- [512, :"2GiB"] do
        assert_raise ArgumentError, ~r/unsupported engine_memory_limit/, fn ->
          Runtime.new(name: __MODULE__.BadMemoryLimit, engine_memory_limit: limit)
        end
      end
    end

    test "refuses a non-string compact_engine_memory_limit at boot" do
      for limit <- [512, :"1GiB"] do
        assert_raise ArgumentError, ~r/unsupported compact_engine_memory_limit/, fn ->
          Runtime.new(name: __MODULE__.BadCompactMemoryLimit, compact_engine_memory_limit: limit)
        end
      end
    end

    test "refuses an unusable compact_max_rows at boot, not at first sweep" do
      for rows <- [0, -1, "4194304"] do
        assert_raise ArgumentError, ~r/unsupported compact_max_rows/, fn ->
          Runtime.new(name: __MODULE__.BadRowCap, compact_max_rows: rows)
        end
      end
    end
  end

  describe "engine_memory_limit/2" do
    test "an explicit limit wins over the cgroup" do
      runtime = Runtime.new(name: __MODULE__.ExplicitLimit, engine_memory_limit: "3GiB")

      assert Runtime.engine_memory_limit(runtime, {:ok, 4_294_967_296}) == "3GiB"
    end

    test "derives half the cgroup limit" do
      runtime = Runtime.new(name: __MODULE__.DerivedLimit)

      assert Runtime.engine_memory_limit(runtime, {:ok, 4_294_967_296}) == "2048MiB"
    end

    test "a cgroup limit under two mebibytes still yields a size DuckDB accepts" do
      runtime = Runtime.new(name: __MODULE__.TinyLimit)

      assert Runtime.engine_memory_limit(runtime, {:ok, 1}) == "1MiB"
    end

    test "without a cgroup limit the engine inherits its application config" do
      runtime = Runtime.new(name: __MODULE__.InheritedLimit)

      assert Runtime.engine_memory_limit(runtime, :none) == nil
    end
  end

  describe "compaction_catalog/1 (T-458)" do
    test "a lake this service runs starts with the compaction connection beside the first" do
      runtime = Runtime.new(name: __MODULE__.CompactionCatalog)
      engine = Runtime.catalog_engine(runtime.name)

      assert runtime.catalog_opts[:connections] == Runtime.catalog_connections()
      assert runtime.catalog.config.engine == engine
      assert Runtime.compaction_catalog(runtime).config.engine == {engine, 2}
    end

    test "the compaction backoff defaults to two sweeps, doubling up to four hours" do
      runtime = Runtime.new(name: __MODULE__.CompactBackoff)

      assert runtime.compact_backoff_base_ms == 2 * runtime.compact_interval_ms
      assert runtime.compact_backoff_max_ms == 14_400_000
    end

    test "refuses a compaction backoff without a non-negative base and a positive ceiling" do
      for opts <- [
            [compact_backoff_base_ms: -1],
            [compact_backoff_base_ms: "600000"],
            [compact_backoff_max_ms: 0]
          ] do
        assert_raise ArgumentError, ~r/unsupported compact backoff/, fn ->
          Runtime.new([name: __MODULE__.BadCompactBackoff] ++ opts)
        end
      end
    end
  end

  describe "compact_engine_memory_limit/2" do
    test "an explicit limit wins over the cgroup" do
      runtime =
        Runtime.new(name: __MODULE__.ExplicitCompactLimit, compact_engine_memory_limit: "1GiB")

      assert Runtime.compact_engine_memory_limit(runtime, {:ok, 4_294_967_296}) == "1GiB"
    end

    test "derives a quarter of the cgroup limit" do
      runtime = Runtime.new(name: __MODULE__.DerivedCompactLimit)

      assert Runtime.compact_engine_memory_limit(runtime, {:ok, 4_294_967_296}) == "1024MiB"
    end

    test "a cgroup limit under four mebibytes still yields a size DuckDB accepts" do
      runtime = Runtime.new(name: __MODULE__.TinyCompactLimit)

      assert Runtime.compact_engine_memory_limit(runtime, {:ok, 1}) == "1MiB"
    end

    test "without a cgroup limit the engine inherits its application config" do
      runtime = Runtime.new(name: __MODULE__.InheritedCompactLimit)

      assert Runtime.compact_engine_memory_limit(runtime, :none) == nil
    end
  end

  describe "compaction target (T-592)" do
    test "defaults to files of 512 MiB to 1 GiB, spanning a day" do
      runtime = Runtime.new(name: __MODULE__.DefaultTarget)

      assert runtime.compact_target_bytes == 1_073_741_824
      assert runtime.compact_span_ms == 86_400_000
    end

    test "a nil target turns the span level off" do
      runtime = Runtime.new(name: __MODULE__.NoTarget, compact_target_bytes: nil)

      assert runtime.compact_target_bytes == nil
    end

    test "refuses a target at or under compact_below_bytes, or a span at or under the bucket" do
      for opts <- [
            [compact_target_bytes: 1_024, compact_below_bytes: 1_024],
            [compact_target_bytes: "1GiB"],
            [compact_span_ms: 3_600_000],
            [compact_target_bytes: nil, compact_span_ms: 1_000]
          ] do
        assert_raise ArgumentError, ~r/unsupported compaction target/, fn ->
          Runtime.new([name: __MODULE__.BadTarget] ++ opts)
        end
      end
    end
  end

  test "compact_span_budget_ms defaults to two minutes and refuses a negative budget (T-603)" do
    assert Runtime.new(name: __MODULE__.SpanBudget).compact_span_budget_ms == 120_000

    assert Runtime.new(name: __MODULE__.NoSpan, compact_span_budget_ms: 0).compact_span_budget_ms ==
             0

    assert_raise ArgumentError, ~r/compact_span_budget_ms/, fn ->
      Runtime.new(name: __MODULE__.BadBudget, compact_span_budget_ms: -1)
    end
  end

  describe "compact_spill_cap/2 (T-601)" do
    setup do
      configured = Application.get_env(:smolquery, :max_temp_directory_size)

      on_exit(fn ->
        if configured,
          do: Application.put_env(:smolquery, :max_temp_directory_size, configured),
          else: Application.delete_env(:smolquery, :max_temp_directory_size)
      end)
    end

    test "takes a share of the free space, below the configured cap" do
      runtime = Runtime.new(name: __MODULE__.SpillShare)
      Application.put_env(:smolquery, :max_temp_directory_size, "32GiB")

      assert Runtime.compact_spill_cap(runtime, {:ok, 100 * 1_073_741_824}) == "25600MiB"
      assert Runtime.compact_spill_cap(runtime, {:ok, 400 * 1_073_741_824}) == "32GiB"
    end

    test "with no configured cap takes the share, and with free space unknown keeps the config" do
      runtime = Runtime.new(name: __MODULE__.SpillUnknown, compact_spill_share: 2)
      Application.delete_env(:smolquery, :max_temp_directory_size)

      assert Runtime.compact_spill_cap(runtime, {:ok, 10 * 1_048_576}) == "5MiB"
      assert Runtime.compact_spill_cap(runtime, :error) == nil
    end

    test "refuses a share, decoded budget or window that is not positive" do
      assert_raise ArgumentError, ~r/compaction spill settings/, fn ->
        Runtime.new(name: __MODULE__.SpillBad, compact_spill_share: 0)
      end

      assert_raise ArgumentError, ~r/compaction spill settings/, fn ->
        Runtime.new(name: __MODULE__.DecodedBad, compact_span_decoded_bytes: -1)
      end

      assert_raise ArgumentError, ~r/compact_window_decoded_bytes 0/, fn ->
        Runtime.new(name: __MODULE__.WindowBad, compact_window_decoded_bytes: 0)
      end
    end
  end

  describe "compact_engine_threads/3" do
    test "takes one thread per 256 MiB of the limit by default" do
      runtime = Runtime.new(name: __MODULE__.ThreadsPerLimit, compact_engine_memory_limit: "1GiB")

      assert Runtime.compact_engine_threads(runtime, :none, 16) == 4
    end

    test "never takes more threads than cores" do
      runtime = Runtime.new(name: __MODULE__.ThreadsCapped, compact_engine_memory_limit: "8GiB")

      assert Runtime.compact_engine_threads(runtime, :none, 8) == 8
    end

    test "takes one thread under a limit smaller than one thread's share" do
      runtime = Runtime.new(name: __MODULE__.ThreadsFloor, compact_engine_memory_limit: "100MB")

      assert Runtime.compact_engine_threads(runtime, :none, 8) == 1
    end

    test "follows the limit derived from the cgroup" do
      runtime = Runtime.new(name: __MODULE__.ThreadsDerived)

      assert Runtime.compact_engine_threads(runtime, {:ok, 6 * 1_073_741_824}, 16) == 6
    end

    test "honours a configured share per thread" do
      runtime =
        Runtime.new(
          name: __MODULE__.ThreadsShare,
          compact_engine_memory_limit: "1536MiB",
          compact_engine_mib_per_thread: 512
        )

      assert Runtime.compact_engine_threads(runtime, :none, 16) == 3
    end

    test "reads DuckDB's decimal and SI sizes" do
      runtime = Runtime.new(name: __MODULE__.ThreadsSi, compact_engine_memory_limit: "1.5 GB")

      assert Runtime.compact_engine_threads(runtime, :none, 16) == 5
    end

    test "without a limit of its own follows the inherited engine memory_limit" do
      runtime = Runtime.new(name: __MODULE__.ThreadsInherited)
      inherited = Application.get_env(:smolquery, Smolquery.Engine)[:memory_limit]

      assert inherited == "512MB"
      assert Runtime.compact_engine_threads(runtime, :none, 16) == 1
    end

    test "reads DuckDB's long unit names" do
      runtime =
        Runtime.new(name: __MODULE__.ThreadsLongUnit, compact_engine_memory_limit: "2 gigabytes")

      assert Runtime.compact_engine_threads(runtime, :none, 16) == 7
    end

    test "leaves DuckDB's default threads, with a warning, for a limit it cannot divide" do
      for limit <- ["80%", "none", "-1"] do
        runtime =
          Runtime.new(name: __MODULE__.ThreadsUnreadable, compact_engine_memory_limit: limit)

        log =
          capture_log(fn -> assert Runtime.compact_engine_threads(runtime, :none, 16) == nil end)

        assert log =~ "keeps DuckDB's default thread count"
      end
    end

    test "still refuses a limit that is not a string at boot" do
      assert_raise ArgumentError, ~r/unsupported compact_engine_memory_limit/, fn ->
        Runtime.new(name: __MODULE__.ThreadsNotString, compact_engine_memory_limit: 1_024)
      end
    end

    test "refuses a share per thread that is not a positive integer" do
      for mib <- [0, -1, "256", nil] do
        assert_raise ArgumentError, ~r/unsupported compact_engine_mib_per_thread/, fn ->
          Runtime.new(name: __MODULE__.ThreadsBadShare, compact_engine_mib_per_thread: mib)
        end
      end
    end
  end

  describe "with_compact_max_rows/1" do
    test "an explicit cap survives untouched" do
      runtime = Runtime.new(name: __MODULE__.ExplicitRowCap, compact_max_rows: 20)

      assert Runtime.with_compact_max_rows(runtime).compact_max_rows == 20
    end

    test "an unset cap starts at the default, for the compactor to adapt per table" do
      runtime = Runtime.new(name: __MODULE__.DefaultRowCap)

      assert Runtime.with_compact_max_rows(runtime).compact_max_rows == 4_194_304
    end

    test "an explicit engine budget does not change the start" do
      runtime =
        Runtime.new(name: __MODULE__.BudgetRowCap, compact_engine_memory_limit: "1GiB")

      assert Runtime.with_compact_max_rows(runtime).compact_max_rows == 4_194_304
    end
  end

  describe "merge_engine/1" do
    test "resolves to the seal merge engine unless overridden" do
      runtime = Runtime.new(name: Storage)

      assert Runtime.merge_engine(runtime) == Storage.Engine

      assert Runtime.merge_engine(%{runtime | merge_engine: Storage.CompactEngine}) ==
               Storage.CompactEngine
    end
  end

  describe "put/1, fetch/1 and delete/1" do
    test "round-trips a runtime through persistent_term" do
      runtime = Runtime.new(name: __MODULE__.Published, dir: "/tmp/published")

      assert Runtime.put(runtime) == :ok
      assert Runtime.fetch(__MODULE__.Published) == {:ok, runtime}
      assert Runtime.delete(__MODULE__.Published)
      assert Runtime.fetch(__MODULE__.Published) == :error
    end

    test "fetching an unpublished instance is an error, not a raise" do
      assert Runtime.fetch(__MODULE__.NeverPublished) == :error
    end
  end

  describe "naming" do
    test "derives every process name from the instance name" do
      assert Runtime.supervisor(Storage) == Storage.Supervisor
      assert Runtime.sealer(Storage) == Storage.Sealer
      assert Runtime.engine(Storage) == Storage.Engine
      assert Runtime.compact_engine(Storage) == Storage.CompactEngine
      assert Runtime.seals(Storage) == Storage.Seals
      assert Runtime.catalog_engine(Storage) == Storage.Catalog
    end
  end
end
