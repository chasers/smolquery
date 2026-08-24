defmodule Smolquery.StorageService.RuntimeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.MapCatalog
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
      assert runtime.catalog_opts == []
    end

    test "passes catalog options through for the supervisor to start" do
      opts = [metadata: "sqlite:/tmp/c.sqlite", data_path: "/tmp/lake"]
      runtime = Runtime.new(name: __MODULE__.LakeOpts, catalog: opts)

      assert runtime.catalog_opts == opts
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

  describe "store_for/2 (PL-51 L4)" do
    setup do
      previous = Application.get_env(:smolquery, :credential_key)

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      on_exit(fn ->
        if previous do
          Application.put_env(:smolquery, :credential_key, previous)
        else
          Application.delete_env(:smolquery, :credential_key)
        end
      end)

      catalog = MapCatalog.new()

      {:ok, owned} =
        Dataset.new(%{
          "name" => "tenant",
          "storage" => %{
            "bucket" => "tenant-bucket",
            "prefix" => "lakes/tenant",
            "endpoint" => "http://minio:9000",
            "region" => "eu-west-1",
            "access_key_id" => "id",
            "secret_access_key" => "secret"
          }
        })

      :ok = Catalog.create_dataset(catalog, owned)
      :ok = Catalog.create_dataset(catalog, "plain")

      %{runtime: Runtime.new(name: __MODULE__.Stores, dir: "/tmp/sealed", catalog: catalog)}
    end

    test "a dataset with its own storage gets its own S3 store, staged like the default",
         %{runtime: runtime} do
      assert {:ok, %Store{impl: Store.S3, config: config}} = Runtime.store_for(runtime, "tenant")

      assert config.bucket == "tenant-bucket"
      assert config.prefix == "lakes/tenant"
      assert config.endpoint == "http://minio:9000"
      assert config.region == "eu-west-1"
      assert config.access_key_id == "id"
      assert config.secret_access_key == "secret"
      assert config.secret_name == "smolquery_ds_tenant"
      assert config.staging_dir == "/tmp/sealed/staging"
    end

    test "a dataset on the default store, or an unknown one, gets the runtime's store",
         %{runtime: runtime} do
      assert Runtime.store_for(runtime, "plain") == {:ok, runtime.store}
      assert Runtime.store_for(runtime, "nope") == {:ok, runtime.store}
    end

    test "a catalog without dataset records answers the runtime's store" do
      runtime =
        Runtime.new(name: __MODULE__.Plain, dir: "/tmp/sealed", catalog: StubCatalog.new(self()))

      assert Runtime.store_for(runtime, "anything") == {:ok, runtime.store}
    end

    test "prepare_store/2 is a no-op for the default store and any local store", %{
      runtime: runtime
    } do
      assert Runtime.prepare_store(runtime, runtime.store) == :ok
      assert Runtime.prepare_store(runtime, Store.Local.new(dir: "/tmp/other")) == :ok
    end
  end
end
