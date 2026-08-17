defmodule Smolquery.StorageService.Handoff.SealTest do
  @moduledoc """
  The crash matrix — this milestone's centerpiece.

  A seal handoff can die between any two of its steps. The property that must hold
  through all of them is that a query counts every row exactly once, at every
  snapshot, and that storage leaks nothing. So each test drives the handoff to a
  partial state, runs it again, and asserts the union of the sealed tier and the
  hot tier against the rows that were actually written.

  Partial states are staged by running the steps directly rather than by killing a
  process mid-flight: a kill lands wherever the scheduler happens to be, which
  tests one arbitrary interleaving and cannot name which one. Calling `Merge.run/3`
  and `register_segments/3` by hand puts the system in exactly the state a crash at
  a named point would leave, deterministically.

  Tagged `:integration` because both halves are real: a live `HotServer` serving
  Parquet over `httpfs`, and a real DuckLake catalog with real snapshots.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.Eventually

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema, do: Schema.new!([{"id", :int64}])
  defp batch(range), do: %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}

  setup context do
    unique = :erlang.unique_integer([:positive])
    buffer = :"seal_buffer_#{unique}"
    storage = :"seal_storage_#{unique}"

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

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake")},
      id: Runtime.catalog_engine(storage)
    )

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    runtime =
      Runtime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_name: buffer,
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog
      )

    start_supervised!(
      {Engine,
       name: Runtime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
    )

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{buffer: buffer, buffer_runtime: buffer_runtime, runtime: runtime, catalog: catalog}
  end

  defp write(buffer, range) do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(range))

    ack.segment_id
  end

  defp freeze_claim(context, ids) do
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
    {:ok, claim} = HotManifest.claim(context.buffer_runtime.manifest, @table, ids, [key])

    claim
  end

  defp seal(context, claim),
    do: Handoff.seal(context.runtime.handoff, context.runtime, @table, claim)

  defp add_column(context, definition) do
    context.runtime.name
    |> Runtime.catalog_engine()
    |> Engine.query!(~s|ALTER TABLE lake."analytics"."events" ADD COLUMN #{definition}|)

    :ok
  end

  defp visible_ids(context, snapshot \\ :current) do
    {:ok, sealed} = Catalog.segments(context.catalog, @table, snapshot)
    {:ok, entries} = Client.hot_manifest(context.buffer, @table)

    registered = MapSet.new(sealed)

    hot =
      entries
      |> Enum.reject(&claim_committed?(&1, context.runtime, registered))
      |> Enum.map(&Store.location(context.buffer_runtime.store, &1.key))

    read_ids(context, sealed ++ hot)
  end

  defp claim_committed?(entry, runtime, registered) do
    entry.claim_keys != [] and
      Enum.all?(entry.claim_keys, &MapSet.member?(registered, Store.location(runtime.store, &1)))
  end

  defp read_ids(_context, []), do: []

  defp read_ids(context, paths) do
    placeholders = Enum.map_join(1..length(paths), ", ", &"$#{&1}")

    context.runtime.name
    |> Runtime.engine()
    |> Engine.query!(
      "SELECT id FROM read_parquet([#{placeholders}], union_by_name := true) ORDER BY id",
      paths
    )
    |> Map.fetch!(:rows)
    |> List.flatten()
  end

  defp sealed_count(context) do
    {:ok, sealed} = Catalog.segments(context.catalog, @table, :current)

    length(sealed)
  end

  describe "the happy path" do
    test "merges, commits, retires — and the rows are counted once throughout", context do
      first = write(context.buffer, 1..2)
      second = write(context.buffer, 3..4)

      assert visible_ids(context) == [1, 2, 3, 4]

      claim = freeze_claim(context, [first, second])

      assert visible_ids(context) == [1, 2, 3, 4]
      assert seal(context, claim) == :ok
      assert visible_ids(context) == [1, 2, 3, 4]
      assert sealed_count(context) == 1

      assert {:ok, entries} = Client.hot_manifest(context.buffer, @table)
      assert Enum.all?(entries, &(&1.sealed_at != nil))
    end

    test "a partition ref seals into its parent's catalog table (T-170)", context do
      partition = {"analytics", "events__p1"}

      {:ok, ack} = Client.write_batch(context.buffer, partition, batch(10..12))

      {:ok, prefix} = Store.prefix(partition)
      {:ok, key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SW")

      {:ok, claim} =
        HotManifest.claim(context.buffer_runtime.manifest, partition, [ack.segment_id], [key])

      assert Handoff.seal(context.runtime.handoff, context.runtime, partition, claim) == :ok

      {:ok, sealed} = Catalog.segments(context.catalog, @table, :current)
      assert [_only_segment] = sealed
      assert read_ids(context, sealed) == [10, 11, 12]

      assert {:ok, entries} = Client.hot_manifest(context.buffer, partition)
      assert Enum.all?(entries, &(&1.sealed_at != nil))
    end

    test "leaves rows written after the claim in the hot tier", context do
      claimed = write(context.buffer, 1..2)
      claim = freeze_claim(context, [claimed])
      later = write(context.buffer, 7..8)

      assert seal(context, claim) == :ok
      assert visible_ids(context) == [1, 2, 7, 8]

      assert {:ok, entry} = HotManifest.entry(context.buffer_runtime.manifest, @table, later)
      assert entry.sealed_at == nil
    end
  end

  describe "a column added after the claim's inputs were written" do
    test "seals to the catalog's schema instead of retrying a file it will reject", context do
      ids = [write(context.buffer, 1..2), write(context.buffer, 3..3)]
      claim = freeze_claim(context, ids)

      add_column(context, ~s|"name" VARCHAR|)

      assert seal(context, claim) == :ok
      assert sealed_count(context) == 1
      assert visible_ids(context) == [1, 2, 3]

      assert {:ok, entries} = Client.hot_manifest(context.buffer, @table)
      assert Enum.all?(entries, &(&1.sealed_at != nil))
    end

    test "the added column reads back as NULL for the rows that predate it", context do
      claim = freeze_claim(context, [write(context.buffer, 1..2)])

      add_column(context, ~s|"name" VARCHAR|)

      assert seal(context, claim) == :ok
      assert {:ok, [sealed]} = Catalog.segments(context.catalog, @table, :current)

      result =
        context.runtime.name
        |> Runtime.engine()
        |> Engine.query!("SELECT id, name FROM read_parquet($1) ORDER BY id", [sealed])

      assert result.rows == [[1, nil], [2, nil]]
    end
  end

  describe "crashed before the commit" do
    test "a fresh attempt merges again and counts once", context do
      ids = [write(context.buffer, 1..2), write(context.buffer, 3..3)]
      claim = freeze_claim(context, ids)

      assert {:ok, _segment} = Merge.run(context.runtime, @table, claim)
      assert sealed_count(context) == 0
      assert visible_ids(context) == [1, 2, 3]

      assert seal(context, claim) == :ok
      assert sealed_count(context) == 1
      assert visible_ids(context) == [1, 2, 3]
    end
  end

  describe "crashed after the commit, before retirement" do
    test "the dedup rule already excludes the micro-segments", context do
      ids = [write(context.buffer, 1..2), write(context.buffer, 3..3)]
      claim = freeze_claim(context, ids)

      {:ok, segment} = Merge.run(context.runtime, @table, claim)
      {:ok, _snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

      assert {:ok, entries} = Client.hot_manifest(context.buffer, @table)
      assert Enum.all?(entries, &(&1.sealed_at == nil))
      assert visible_ids(context) == [1, 2, 3]
    end

    test "the next attempt skips the merge and only retires", context do
      ids = [write(context.buffer, 1..2)]
      claim = freeze_claim(context, ids)

      {:ok, segment} = Merge.run(context.runtime, @table, claim)
      {:ok, _snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

      assert Handoff.Seal.committed?(context.runtime, @table, claim) == {:ok, true}
      assert seal(context, claim) == :ok

      assert sealed_count(context) == 1
      assert visible_ids(context) == [1, 2]

      assert {:ok, entries} = Client.hot_manifest(context.buffer, @table)
      assert Enum.all?(entries, &(&1.sealed_at != nil))
    end
  end

  describe "a released claim (T-294)" do
    defp release_and_reclaim(context, claim, keep) do
      :ok = HotManifest.release(context.buffer_runtime.manifest, @table, claim.ids)

      {:ok, prefix} = Store.prefix(@table)
      {:ok, new_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SW")
      {:ok, next} = HotManifest.claim(context.buffer_runtime.manifest, @table, keep, [new_key])

      next
    end

    test "an attempt for a released claim is refused before it merges", context do
      ids = [write(context.buffer, 1..2), write(context.buffer, 3..4)]
      claim = freeze_claim(context, ids)
      _next = release_and_reclaim(context, claim, [hd(ids)])

      assert {:error, {:stale_claim, stale}} = seal(context, claim)
      assert Enum.sort(stale) == Enum.sort(claim.ids)
      assert sealed_count(context) == 0
      assert visible_ids(context) == [1, 2, 3, 4]
    end

    test "the retire fence refuses an attempt that raced past both guards", context do
      ids = [write(context.buffer, 1..2)]
      claim = freeze_claim(context, ids)

      {:ok, segment} = Merge.run(context.runtime, @table, claim)
      {:ok, _snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

      _next = release_and_reclaim(context, claim, ids)

      assert {:error, {:stale_claim, _diff}} =
               Client.retire(context.buffer, @table, claim.ids, 1, claim.keys)

      assert {:ok, entries} = Client.hot_manifest(context.buffer, @table)
      assert Enum.all?(entries, &(&1.sealed_at == nil))
    end

    test "a stale refusal drops the orphan a raced attempt registered", context do
      ids = [write(context.buffer, 1..2)]
      claim = freeze_claim(context, ids)

      {:ok, segment} = Merge.run(context.runtime, @table, claim)
      {:ok, _snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

      next = release_and_reclaim(context, claim, ids)

      assert {:error, {:stale_claim, _stale}} = seal(context, claim)
      assert sealed_count(context) == 0
      assert visible_ids(context) == [1, 2]

      assert seal(context, next) == :ok
      assert sealed_count(context) == 1
      assert visible_ids(context) == [1, 2]
    end
  end

  describe "the orphan a crash leaves" do
    test "an uncommitted merge output is swept, and a committed one is not", context do
      ids = [write(context.buffer, 1..2)]
      claim = freeze_claim(context, ids)

      assert {:ok, orphan} = Merge.run(context.runtime, @table, claim)
      assert sealed_count(context) == 0

      start_supervised!({GC, %{context.runtime | gc_grace_ms: 0}}, id: :gc_orphan)

      assert {:ok, report} = GC.sweep(context.runtime.name)
      assert report.swept == [orphan.key]
      assert visible_ids(context) == [1, 2]

      assert seal(context, claim) == :ok
      assert {:ok, second} = GC.sweep(context.runtime.name)
      assert second.swept == []
      assert visible_ids(context) == [1, 2]
    end
  end

  describe "crashed after retirement" do
    test "a repeated attempt changes nothing", context do
      ids = [write(context.buffer, 1..3)]
      claim = freeze_claim(context, ids)

      assert seal(context, claim) == :ok
      assert seal(context, claim) == :ok
      assert seal(context, claim) == :ok

      assert sealed_count(context) == 1
      assert visible_ids(context) == [1, 2, 3]
    end
  end

  describe "older snapshots" do
    test "a query planned before the commit still sees the micro-segments", context do
      ids = [write(context.buffer, 1..2)]
      {:ok, before} = Catalog.current_snapshot(context.catalog)

      claim = freeze_claim(context, ids)
      assert seal(context, claim) == :ok

      assert visible_ids(context, before) == [1, 2]
      assert visible_ids(context) == [1, 2]
    end
  end

  describe "failures" do
    test "an unreachable buffer node fails the attempt without committing", context do
      ids = [write(context.buffer, 1..2)]
      claim = freeze_claim(context, ids)

      runtime = %{context.runtime | buffer_base_url: "http://127.0.0.1:1"}

      assert {:error, {:manifest_unreachable, _reason}} =
               Handoff.seal(runtime.handoff, runtime, @table, claim)

      assert sealed_count(context) == 0
      assert visible_ids(context) == [1, 2]
    end

    test "a table the catalog does not hold is an error, not an invented table", context do
      ids = [write(context.buffer, 1..1)]
      claim = freeze_claim(context, ids)

      assert {:error, _reason} =
               Handoff.seal(
                 context.runtime.handoff,
                 context.runtime,
                 {"analytics", "absent"},
                 claim
               )
    end

    test "a claim with no keys is refused", context do
      assert Handoff.seal(context.runtime.handoff, context.runtime, @table, %{ids: [], keys: []}) ==
               {:error, {:invalid_claim, %{ids: [], keys: []}}}
    end
  end

  describe "end to end, through the buffer's own signal" do
    test "a table crossing its threshold is sealed and swept", context do
      unique = :erlang.unique_integer([:positive])
      buffer = :"seal_e2e_buffer_#{unique}"
      storage = :"seal_e2e_storage_#{unique}"

      start_supervised!(
        {DuckLake,
         name: Runtime.catalog_engine(storage),
         metadata: "sqlite:#{Path.join(context.tmp_dir, "e2e.sqlite")}",
         data_path: Path.join(context.tmp_dir, "e2e_ducklake")},
        id: Runtime.catalog_engine(storage)
      )

      catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
      :ok = Catalog.create_dataset(catalog, "analytics")
      :ok = Catalog.create_table(catalog, @table, schema())

      start_supervised!(
        {BufferService.Supervisor,
         name: buffer,
         dir: Path.join(context.tmp_dir, "e2e_buffer"),
         hot_server_port: 0,
         flush_interval_ms: 25,
         maintenance_interval_ms: 20,
         seal_max_files: 1,
         seal_retry_ms: 50,
         retire_grace_ms: 0,
         seal_consumer: {StorageService.Client, [name: storage]}},
        id: buffer
      )

      on_exit(fn -> BufferService.Runtime.delete(buffer) end)

      start_supervised!(
        {StorageService.Supervisor,
         name: storage,
         dir: Path.join(context.tmp_dir, "e2e_sealed"),
         buffer_name: buffer,
         buffer_base_url: HotServer.base_url(buffer),
         catalog: catalog,
         engine_extensions: [:httpfs]},
        id: storage
      )

      on_exit(fn -> Runtime.delete(storage) end)

      {:ok, _ack} = Client.write_batch(buffer, @table, batch(1..3))

      assert Eventually.until(
               fn ->
                 {:ok, sealed} = Catalog.segments(catalog, @table, :current)
                 {:ok, entries} = Client.hot_manifest(buffer, @table)

                 match?([_one], sealed) and entries == []
               end,
               200,
               25
             )

      rows =
        Runtime.catalog_engine(storage)
        |> Engine.query!(~s|SELECT id FROM lake."analytics"."events" ORDER BY id|)
        |> Map.fetch!(:rows)
        |> List.flatten()

      assert rows == [1, 2, 3]
    end
  end
end
