defmodule Smolquery.BufferService.HotManifest.TombstoneTest do
  @moduledoc """
  Release tombstones (T-386): the durable record that outlives a released
  claim's entries, so its orphan segment can be reconciled after the in-flight
  attempt crashed or the reaper deleted the evidence (F-1 residuals,
  `tla/FINDINGS.md`).
  """

  use ExUnit.Case, async: true

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.SegmentFixture

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @keys ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SV.parquet"]
  @rekeys ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SW.parquet"]

  defp start_manifest(context, store) do
    name = :"manifest_#{:erlang.unique_integer([:positive])}"
    start_supervised!({HotManifest, name: name})

    HotManifest.new(name: name, log_dir: Path.join(context.tmp_dir, "logs"), store: store)
  end

  defp add(manifest, table_ref) do
    {:ok, prefix} = Store.prefix(table_ref)
    schema = Schema.new!([{"id", :int64}])

    {:ok, segment} =
      SegmentFixture.write([%{"id" => 1}], schema, store: manifest.store, prefix: prefix)

    {:ok, entry} = HotManifest.add(manifest, table_ref, segment)

    entry
  end

  defp released_claim(manifest) do
    entry = add(manifest, @table)
    {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)
    :ok = HotManifest.release(manifest, @table, claim.ids)

    claim
  end

  setup context do
    store = Store.Local.new(dir: Path.join(context.tmp_dir, "segments"))

    %{manifest: start_manifest(context, store)}
  end

  test "release records a tombstone naming the claim's keys and ids", %{manifest: manifest} do
    claim = released_claim(manifest)

    assert HotManifest.tombstones(manifest, @table) == [%{keys: @keys, ids: claim.ids}]
  end

  test "reconcile_released clears it, and is idempotent", %{manifest: manifest} do
    released_claim(manifest)

    assert HotManifest.reconcile_released(manifest, @table, @keys) == :ok
    assert HotManifest.tombstones(manifest, @table) == []

    assert HotManifest.reconcile_released(manifest, @table, @keys) == :ok
    assert HotManifest.reconcile_released(manifest, @table, ["never/released.parquet"]) == :ok
  end

  test "a tombstone survives recovery; a cleared one stays cleared", %{manifest: manifest} do
    claim = released_claim(manifest)

    assert {:ok, _report} = HotManifest.recover(manifest, @table)
    assert HotManifest.tombstones(manifest, @table) == [%{keys: @keys, ids: claim.ids}]

    :ok = HotManifest.reconcile_released(manifest, @table, @keys)

    assert {:ok, _report} = HotManifest.recover(manifest, @table)
    assert HotManifest.tombstones(manifest, @table) == []
  end

  test "a tombstone survives log compaction without clobbering re-derived claims",
       %{manifest: manifest} do
    claim = released_claim(manifest)
    {:ok, _reclaim} = HotManifest.claim(manifest, @table, claim.ids, @rekeys)

    assert :ok = HotManifest.compact(manifest, @table)
    assert {:ok, _report} = HotManifest.recover(manifest, @table)

    assert HotManifest.tombstones(manifest, @table) == [%{keys: @keys, ids: claim.ids}]

    assert manifest
           |> HotManifest.entries(@table)
           |> Enum.all?(&(&1.claim_keys == @rekeys))
  end

  test "a tombstone outlives its entries", %{manifest: manifest} do
    claim = released_claim(manifest)
    {:ok, _reclaim} = HotManifest.claim(manifest, @table, claim.ids, @rekeys)
    :ok = HotManifest.retire(manifest, @table, claim.ids, 7, @rekeys)
    :ok = HotManifest.drop(manifest, @table, claim.ids)

    assert HotManifest.entries(manifest, @table) == []
    assert HotManifest.tombstones(manifest, @table) == [%{keys: @keys, ids: claim.ids}]

    assert {:ok, _report} = HotManifest.recover(manifest, @table)
    assert HotManifest.tombstones(manifest, @table) == [%{keys: @keys, ids: claim.ids}]
  end

  test "a release logged without claim keys leaves no tombstone", %{manifest: manifest} do
    entry = add(manifest, @table)

    assert HotManifest.release(manifest, @table, [entry.id]) == :ok
    assert HotManifest.tombstones(manifest, @table) == []
  end
end
