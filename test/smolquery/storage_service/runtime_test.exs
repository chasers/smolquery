defmodule Smolquery.StorageService.RuntimeTest do
  use ExUnit.Case, async: true

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

    test "refuses a non-positive seal_row_group_size at boot, not per seal attempt" do
      assert_raise ArgumentError, ~r/unsupported seal_row_group_size/, fn ->
        Runtime.new(name: __MODULE__.BadRowGroup, seal_row_group_size: 0)
      end
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
      assert Runtime.seals(Storage) == Storage.Seals
      assert Runtime.catalog_engine(Storage) == Storage.Catalog
    end
  end
end
