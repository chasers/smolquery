defmodule Smolquery.StorageService.GCTest do
  use ExUnit.Case, async: false

  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.PathCatalog

  @moduletag :tmp_dir

  defp start_gc(context, opts) do
    name = :"gc_#{:erlang.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        [
          name: name,
          dir: Path.join(context.tmp_dir, "sealed"),
          catalog: PathCatalog.new(context.catalog)
        ] ++ opts
      )

    start_supervised!({GC, runtime}, id: name)

    %{name: name, runtime: runtime}
  end

  defp put(runtime, id) do
    {:ok, prefix} = Store.prefix({"analytics", "events"})
    {:ok, key} = Store.key(prefix, id)
    {:ok, _put} = Store.put(runtime.store, key, &File.write(&1, "segment"))

    key
  end

  setup do
    {:ok, catalog} = Agent.start_link(fn -> [] end)

    %{catalog: catalog}
  end

  describe "sweep/2" do
    test "deletes an uncommitted segment once its grace period has passed", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      assert {:ok, report} = GC.sweep(name)
      assert report.swept == [key]
      assert {:ok, []} = Store.list(runtime.store, "")
    end

    test "keeps a registered segment", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      PathCatalog.register(context.catalog, Store.location(runtime.store, key))

      assert {:ok, report} = GC.sweep(name)
      assert report.swept == []
      assert {:ok, [^key]} = Store.list(runtime.store, "")
    end

    test "keeps a segment no current snapshot holds but an older one does", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      PathCatalog.register(context.catalog, Store.location(runtime.store, key))
      PathCatalog.drop_from_current(context.catalog, Store.location(runtime.store, key))

      assert {:ok, report} = GC.sweep(name)
      assert report.swept == []
      assert {:ok, [^key]} = Store.list(runtime.store, "")
    end

    test "waits out the grace period before deleting", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 60_000)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      assert {:ok, first} = GC.sweep(name)
      assert first.swept == []
      assert first.watching == 1

      assert {:ok, second} = GC.sweep(name)
      assert second.swept == []
      assert second.watching == 1

      assert {:ok, [^key]} = Store.list(runtime.store, "")
    end

    test "rechecks the catalog before deleting, so a mid-sweep commit keeps its file",
         context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      PathCatalog.register_on_next_read(context.catalog, Store.location(runtime.store, key))

      assert {:ok, report} = GC.sweep(name)
      assert report.swept == []
      assert {:ok, [^key]} = Store.list(runtime.store, "")
    end

    test "stops watching a candidate that got registered after all", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 60_000)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      assert {:ok, %{watching: 1}} = GC.sweep(name)

      PathCatalog.register(context.catalog, Store.location(runtime.store, key))

      assert {:ok, report} = GC.sweep(name)
      assert report.watching == 0
      assert report.swept == []
    end

    test "an empty store is not an error", context do
      %{name: name} = start_gc(context, gc_grace_ms: 0)

      assert {:ok, %{swept: [], watching: 0}} = GC.sweep(name)
    end

    test "reports a catalog that cannot answer, rather than deleting", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)
      key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      PathCatalog.fail(context.catalog, :catalog_down)

      assert GC.sweep(name) == {:error, :catalog_down}
      assert {:ok, [^key]} = Store.list(runtime.store, "")
    end

    test "sweeps across tables in one pass", context do
      %{name: name, runtime: runtime} = start_gc(context, gc_grace_ms: 0)

      {:ok, events} = Store.prefix({"analytics", "events"})
      {:ok, clicks} = Store.prefix({"analytics", "clicks"})
      {:ok, first} = Store.key(events, "01KYWPEEGAM8FQVQS5S2QF26SV")
      {:ok, second} = Store.key(clicks, "01KYWPEEGAM8FQVQS5S2QF26SW")

      {:ok, _} = Store.put(runtime.store, first, &File.write(&1, "a"))
      {:ok, _} = Store.put(runtime.store, second, &File.write(&1, "b"))

      assert {:ok, report} = GC.sweep(name)
      assert Enum.sort(report.swept) == Enum.sort([first, second])
    end
  end

  describe "the periodic sweep" do
    test "runs on its own interval", context do
      %{runtime: runtime} = start_gc(context, gc_grace_ms: 0, gc_interval_ms: 20)
      _key = put(runtime, "01KYWPEEGAM8FQVQS5S2QF26SV")

      assert Eventually.until(fn -> Store.list(runtime.store, "") == {:ok, []} end)
    end
  end
end
