defmodule Smolquery.StorageService.Handoff.ReleasedClaimReconcilerTest do
  @moduledoc """
  The durable reconciler for the F-1 residuals (T-386, `tla/FINDINGS.md`,
  `tla/ReleasedClaim.tla`).

  The T-385 gate fix closes F-1 only while the released claim's in-flight
  attempt survives to run its retire with the manifest evidence still present.
  Two residual paths strand the double-counting orphan anyway:

    * **crash-after-register** — the attempt dies between register and retire.
      A released claim is never re-signalled, so no retry ever runs and
      `compensate_stale` never fires.
    * **reap-before-retire** — the attempt's retire lands after the grace
      reaper deleted the entries. With no entry left to inspect, the retire
      returns `:ok` and the fence has no evidence.

  The reconciler closes both from durable state: the release records a
  tombstone naming the released claim's output keys, and once every released
  id is sealed under the re-derived claims, `Handoff.reconcile_released/4`
  drops any segment registered under the tombstoned keys and clears the
  tombstone. `tla/ReleasedClaim_reconciler.cfg` proves the design; these tests
  drive the real code through both residual schedules.
  """

  use ExUnit.Case, async: false
  use Snabbkaffex

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  @orig_ulid "01KYWPEEGAM8FQVQS5S2QF26S0"
  @re1_ulid "01KYWPEEGAM8FQVQS5S2QF26S1"
  @re2_ulid "01KYWPEEGAM8FQVQS5S2QF26S2"

  defp schema, do: Schema.new!([{"id", :int64}])
  defp batch(range), do: %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}

  setup context do
    unique = :erlang.unique_integer([:positive])
    buffer = :"rec_buffer_#{unique}"
    storage = :"rec_storage_#{unique}"

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

  defp freeze_claim(context, ids, ulid) do
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, ulid)
    {:ok, claim} = HotManifest.claim(context.buffer_runtime.manifest, @table, ids, [key])

    claim
  end

  defp seal(context, claim),
    do: Handoff.seal(context.runtime.handoff, context.runtime, @table, claim)

  defp reconcile(context, claim) do
    Handoff.reconcile_released(context.runtime.handoff, context.runtime, @table, %{
      keys: claim.keys,
      ids: claim.ids,
      origin: nil
    })
  end

  defp tombstones(context),
    do: HotManifest.tombstones(context.buffer_runtime.manifest, @table)

  defp visible_ids(context) do
    {:ok, sealed} = Catalog.segments(context.catalog, @table, :current)
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

  defp reseal_released(context, m1, m2) do
    :ok = HotManifest.release(context.buffer_runtime.manifest, @table, [m1, m2])

    r1 = freeze_claim(context, [m1], @re1_ulid)
    r2 = freeze_claim(context, [m2], @re2_ulid)

    assert seal(context, r1) == :ok
    assert seal(context, r2) == :ok
  end

  test "crash-after-register: the reconciler drops the orphan the dead attempt stranded",
       context do
    m1 = write(context.buffer, 1..2)
    m2 = write(context.buffer, 3..4)

    orig = freeze_claim(context, [m1, m2], @orig_ulid)
    orig_keys = orig.keys

    check_trace(
      fn ->
        inject_crash(%{:"$kind" => :"storage.seal.before_retire", keys: ^orig_keys})

        {pid, ref} = spawn_monitor(fn -> seal(context, orig) end)

        {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", keys: ^orig_keys})

        assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 15_000
        refute reason == :normal

        reseal_released(context, m1, m2)
      end,
      fn _result, _trace -> true end
    )

    assert tombstones(context) == [%{keys: orig_keys, ids: [m1, m2]}]
    assert sealed_count(context) == 3
    assert visible_ids(context) == [1, 1, 2, 2, 3, 3, 4, 4]

    assert reconcile(context, orig) == :ok

    assert visible_ids(context) == [1, 2, 3, 4]
    assert sealed_count(context) == 2
    assert tombstones(context) == []
  end

  test "reap-before-retire: the fence has no evidence, and the reconciler still converges",
       context do
    m1 = write(context.buffer, 1..2)
    m2 = write(context.buffer, 3..4)

    orig = freeze_claim(context, [m1, m2], @orig_ulid)
    orig_keys = orig.keys

    check_trace(
      fn ->
        force_ordering(
          delay: %{:"$kind" => :"storage.seal.before_retire", keys: ^orig_keys},
          until: %{:"$kind" => :test_reaped}
        )

        task = Task.async(fn -> seal(context, orig) end)

        {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", keys: ^orig_keys})

        reseal_released(context, m1, m2)

        :ok = HotManifest.drop(context.buffer_runtime.manifest, @table, [m1, m2])
        tp(:test_reaped, %{})

        Task.await(task, 15_000)
      end,
      fn result, _trace ->
        assert result == :ok
      end
    )

    assert tombstones(context) == [%{keys: orig_keys, ids: [m1, m2]}]
    assert sealed_count(context) == 3
    assert visible_ids(context) == [1, 1, 2, 2, 3, 3, 4, 4]

    assert reconcile(context, orig) == :ok

    assert visible_ids(context) == [1, 2, 3, 4]
    assert sealed_count(context) == 2
    assert tombstones(context) == []
  end
end
