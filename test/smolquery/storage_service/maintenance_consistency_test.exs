defmodule Smolquery.StorageService.MaintenanceConsistencyTest do
  @moduledoc """
  The maintenance crash matrix — M7's counterpart to M4's seal matrix and
  M5's reader-side walk.

  Those proved every partial state of the *seal* handoff counts every row
  exactly once. Compaction adds a second catalog-rewriting machine, so this
  walks its partial states the same way — staged exactly, by running the
  steps directly, with every read going through `QueryService.Client` and
  the planner — plus the one failure only a kill can stage (a buffer dying
  with an accumulator full of unacked rows), and the full physical-reclaim
  chain: compact → snapshot expiry → GC deletes the inputs, with a pinned
  reader honest at every step.

  One ordering is load-bearing enough to state: snapshot expiry runs only
  after the hot tier's grace reaper has dropped the entries a seal retired
  (`snapshot_keep_ms` >> `retire_grace_ms` in any sane configuration).
  Expiry erases the registration history the membership rule reads, so an
  expired seal with live hot entries would double-count — the reclaim test
  below reaps the hot tier first, the order reality guarantees by clock.

  Tagged `:integration`: a live `HotServer`, a real DuckLake catalog, and
  `httpfs` reads in every job engine.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Compactor
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime, as: StorageRuntime
  alias Smolquery.Test.Eventually

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  setup context do
    unique = :erlang.unique_integer([:positive])
    buffer = :"maintenance_buffer_#{unique}"
    storage = :"maintenance_storage_#{unique}"
    query = :"maintenance_query_#{unique}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_interval_ms: 25,
       seal_max_files: 1_000_000,
       seal_max_bytes: 1_000_000_000,
       seal_max_age_ms: 600_000,
       retire_grace_ms: 600_000},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "ducklake")

    start_supervised!(
      {DuckLake,
       name: StorageRuntime.catalog_engine(storage), metadata: metadata, data_path: data_path},
      id: StorageRuntime.catalog_engine(storage)
    )

    catalog = DuckLake.new(engine: StorageRuntime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    storage_runtime =
      StorageRuntime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_name: buffer,
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog,
        gc_grace_ms: 0,
        compact_below_bytes: 1_048_576,
        compact_min_inputs: 2,
        compact_max_bytes: 16_777_216
      )

    start_supervised!(
      {Engine,
       name: StorageRuntime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
    )

    start_supervised!({Compactor, storage_runtime}, id: {:compactor, storage})
    start_supervised!({GC, storage_runtime}, id: {:gc, storage})

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_name: buffer,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{
      buffer: buffer,
      buffer_runtime: buffer_runtime,
      storage: storage,
      storage_runtime: storage_runtime,
      query: query,
      catalog: catalog
    }
  end

  defp schema, do: Schema.new!([{"id", :int64}])

  defp write(buffer, range) do
    batch = %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}
    {:ok, ack} = BufferService.Client.write_batch(buffer, @table, batch)

    ack.segment_id
  end

  defp seal(context, range, sealed_key_seed) do
    id = write(context.buffer, range)
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, Id.generate(sealed_key_seed))
    {:ok, claim} = HotManifest.claim(context.buffer_runtime.manifest, @table, [id], [key])

    {:ok, segment} = Merge.run(context.storage_runtime, @table, claim)
    {:ok, snapshot} = Catalog.register_segments(context.catalog, @table, [segment])
    :ok = BufferService.Client.retire(context.buffer, @table, claim.ids, snapshot)

    segment
  end

  defp visible_ids(context) do
    {:ok, job, frame} =
      Client.query(context.query, "SELECT id FROM analytics.events ORDER BY id")

    assert job.state == :done, "query failed: #{inspect(job.error)}"

    DataFrame.to_columns(frame)["id"]
  end

  defp reap_hot_tier(context) do
    manifest = context.buffer_runtime.manifest
    ids = manifest |> HotManifest.entries(@table) |> Enum.map(& &1.id)

    :ok = HotManifest.drop(manifest, @table, ids)
  end

  defp compaction_key({dataset, table} = table_ref, paths) do
    ids = paths |> Enum.map(&Path.basename(&1, ".parquet")) |> Enum.sort()
    {:ok, timestamp} = ids |> List.last() |> Id.timestamp()

    {:ok, prefix} = Store.prefix(table_ref)

    {:ok, key} =
      Store.key(prefix, Id.derive(timestamp, [dataset, 0, table, 0, Enum.intersperse(ids, 0)]))

    key
  end

  test "every partial state of a compaction counts every row exactly once", context do
    a = seal(context, 1..10, 1_000)
    b = seal(context, 11..20, 2_000)
    all = Enum.to_list(1..20)

    assert visible_ids(context) == all

    key = compaction_key(@table, [a.path, b.path])
    {:ok, _merged} = Merge.compact(context.storage_runtime, @table, key, [a.path, b.path])

    assert visible_ids(context) == all

    assert {:ok, %{compacted: [%{key: ^key}], failed: []}} = Compactor.sweep(context.storage)

    assert visible_ids(context) == all

    merged_path = Store.location(context.storage_runtime.store, key)
    assert Catalog.segments(context.catalog, @table, :current) == {:ok, [merged_path]}
  end

  test "a compaction abandoned before its swap is swept, then rebuilt identically", context do
    a = seal(context, 1..10, 1_000)
    b = seal(context, 11..20, 2_000)

    key = compaction_key(@table, [a.path, b.path])
    {:ok, _merged} = Merge.compact(context.storage_runtime, @table, key, [a.path, b.path])

    assert {:ok, %{swept: [^key]}} = GC.sweep(context.storage)
    refute File.exists?(Store.location(context.storage_runtime.store, key))
    assert visible_ids(context) == Enum.to_list(1..20)

    assert {:ok, %{compacted: [%{key: ^key}], failed: []}} = Compactor.sweep(context.storage)
    assert visible_ids(context) == Enum.to_list(1..20)
  end

  test "compacted inputs survive GC until their snapshots expire, then reclaim", context do
    a = seal(context, 1..10, 1_000)
    b = seal(context, 11..20, 2_000)
    {:ok, pinned} = Catalog.current_snapshot(context.catalog)

    assert {:ok, %{compacted: [_swap]}} = Compactor.sweep(context.storage)

    assert {:ok, %{swept: []}} = GC.sweep(context.storage)
    assert File.exists?(a.path)
    assert File.exists?(b.path)
    assert {:ok, pinned_paths} = Catalog.segments(context.catalog, @table, pinned)
    assert Enum.sort(pinned_paths) == Enum.sort([a.path, b.path])

    reap_hot_tier(context)

    Process.sleep(1_100)
    assert {:ok, expired} = Catalog.expire_snapshots(context.catalog, 1_000)
    assert expired > 0

    assert {:ok, %{swept: swept}} = GC.sweep(context.storage)
    assert Enum.sort(swept) == Enum.sort([a.key, b.key])
    refute File.exists?(a.path)
    refute File.exists?(b.path)

    assert visible_ids(context) == Enum.to_list(1..20)

    assert {:error, _expired_pin_fails_cleanly} =
             Catalog.segments(context.catalog, @table, pinned)
  end

  test "a buffer killed with unacked rows in its accumulator never shows them", context do
    write(context.buffer, 1..5)
    assert visible_ids(context) == Enum.to_list(1..5)

    slow = :"slow_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: slow, dir: Path.join(context.tmp_dir, "slow"), flush_interval_ms: 60_000},
      id: slow
    )

    on_exit(fn -> BufferService.Runtime.delete(slow) end)

    batch = %{schema: schema(), rows: [%{"id" => 99}]}

    caller =
      Task.async(fn ->
        try do
          BufferService.Client.write_batch(slow, @table, batch)
        catch
          :exit, reason -> {:exit, reason}
        end
      end)

    {:ok, slow_runtime} = BufferService.Runtime.fetch(slow)

    Eventually.until(fn ->
      Registry.lookup(BufferService.Runtime.registry(slow), @table) != []
    end)

    Process.sleep(50)
    [{pid, _value}] = Registry.lookup(BufferService.Runtime.registry(slow), @table)
    Process.exit(pid, :kill)

    refute match?({:ok, _ack}, Task.await(caller))

    {:ok, _report} = HotManifest.recover(slow_runtime.manifest, @table)
    assert HotManifest.entries(slow_runtime.manifest, @table) == []
  end
end
