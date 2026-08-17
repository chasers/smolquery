defmodule Smolquery.BufferService.HotManifest.ClaimTest do
  @moduledoc """
  The claim half of `Smolquery.BufferService.HotManifest`.

  Separate from `HotManifestTest` because these tests are about one property —
  that a sealer always sees the same input set — rather than about the manifest's
  storage mechanics.
  """

  use ExUnit.Case, async: true

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @keys ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SV.parquet"]

  defp start_manifest(context, store) do
    name = :"manifest_#{:erlang.unique_integer([:positive])}"
    start_supervised!({HotManifest, name: name})

    HotManifest.new(name: name, log_dir: Path.join(context.tmp_dir, "logs"), store: store)
  end

  defp add(manifest, table_ref) do
    {:ok, prefix} = Store.prefix(table_ref)
    schema = Schema.new!([{"id", :int64}])

    {:ok, segment} =
      Writer.write([%{"id" => 1}], schema, store: manifest.store, prefix: prefix)

    {:ok, entry} = HotManifest.add(manifest, table_ref, segment)

    entry
  end

  setup context do
    store = Store.Local.new(dir: Path.join(context.tmp_dir, "segments"))

    %{manifest: start_manifest(context, store), store: store}
  end

  describe "claim/5" do
    test "freezes the ids and stamps them with the sealed keys", %{manifest: manifest} do
      first = add(manifest, @table)
      second = add(manifest, @table)
      ids = [first.id, second.id]

      assert {:ok, claim} = HotManifest.claim(manifest, @table, ids, @keys)
      assert Enum.sort(claim.ids) == Enum.sort(ids)
      assert claim.keys == @keys

      assert manifest
             |> HotManifest.entries(@table)
             |> Enum.all?(&(&1.claim_keys == @keys))
    end

    test "refuses a second claim while one is live, so a claim cannot be grown", %{
      manifest: manifest
    } do
      first = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [first.id], @keys)

      second = add(manifest, @table)
      grown = ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SW.parquet"]

      assert HotManifest.claim(manifest, @table, [first.id, second.id], grown) ==
               {:error, :claim_outstanding}

      assert {:ok, first_entry} = HotManifest.entry(manifest, @table, first.id)
      assert first_entry.claim_keys == @keys

      assert {:ok, second_entry} = HotManifest.entry(manifest, @table, second.id)
      assert second_entry.claim_keys == []
    end

    test "absorbs an identical re-claim of the live claim", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      assert HotManifest.claim(manifest, @table, [entry.id], @keys) == {:ok, claim}
    end

    test "refuses to re-freeze a live claim under different keys", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      other = ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SX.parquet"]

      assert HotManifest.claim(manifest, @table, [entry.id], other) ==
               {:error, :claim_outstanding}
    end

    test "refuses a set it can only partially freeze, naming the divergence", %{
      manifest: manifest
    } do
      sealed = add(manifest, @table)
      :ok = HotManifest.retire(manifest, @table, [sealed.id], 7)
      live = add(manifest, @table)
      absent = "01KYWPEEGAM8FQVQS5S2QF26SV"

      assert HotManifest.claim(manifest, @table, [sealed.id, live.id, absent], @keys) ==
               {:error, {:partial_claim, %{missing: [absent], sealed: [sealed.id]}}}

      assert {:ok, entry} = HotManifest.entry(manifest, @table, live.id)
      assert entry.claim_keys == []
    end

    test "refuses a sealed id", %{manifest: manifest} do
      entry = add(manifest, @table)
      :ok = HotManifest.retire(manifest, @table, [entry.id], 7)

      assert HotManifest.claim(manifest, @table, [entry.id], @keys) ==
               {:error, :nothing_to_claim}
    end

    test "refuses an id the node never held", %{manifest: manifest} do
      assert HotManifest.claim(manifest, @table, ["01KYWPEEGAM8FQVQS5S2QF26SV"], @keys) ==
               {:error, :nothing_to_claim}
    end
  end

  describe "release/4" do
    test "returns the live claim's ids to pending, so the next claim re-derives", %{
      manifest: manifest
    } do
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)

      assert :ok = HotManifest.release(manifest, @table, claim.ids)
      assert HotManifest.live_claim(manifest, @table) == :error

      assert manifest
             |> HotManifest.entries(@table)
             |> Enum.all?(&(&1.claim_keys == []))

      other = ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SX.parquet"]
      assert {:ok, next} = HotManifest.claim(manifest, @table, [first.id], other)
      assert next.ids == [first.id]
      assert next.keys == other
    end

    test "refuses ids that are not the live claim's", %{manifest: manifest} do
      entry = add(manifest, @table)
      other = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      assert HotManifest.release(manifest, @table, [other.id]) == {:error, :claim_mismatch}
      assert {:ok, _still_live} = HotManifest.live_claim(manifest, @table)
    end

    test "absorbs a release with no live claim", %{manifest: manifest} do
      entry = add(manifest, @table)

      assert :ok = HotManifest.release(manifest, @table, [entry.id])

      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)
      :ok = HotManifest.release(manifest, @table, claim.ids)

      assert :ok = HotManifest.release(manifest, @table, claim.ids)
    end

    test "survives recovery, so a restarted buffer does not resurrect the claim", context do
      manifest = context.manifest
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)
      :ok = HotManifest.release(manifest, @table, claim.ids)

      assert {:ok, %{entries: 2}} = HotManifest.recover(manifest, @table)
      assert HotManifest.live_claim(manifest, @table) == :error
    end
  end

  describe "retire/6 key fencing" do
    test "retires under the claim's own keys, refuses stale ones", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      stale = ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SX.parquet"]

      assert HotManifest.retire(manifest, @table, claim.ids, 7, stale) ==
               {:error, {:stale_claim, %{keys: stale, ids: claim.ids}}}

      assert {:ok, unsealed} = HotManifest.entry(manifest, @table, entry.id)
      assert is_nil(unsealed.sealed_at)

      assert HotManifest.retire(manifest, @table, claim.ids, 7, claim.keys) == :ok
    end

    test "refuses a released claim's old keys", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)
      :ok = HotManifest.release(manifest, @table, claim.ids)

      assert {:error, {:stale_claim, _diff}} =
               HotManifest.retire(manifest, @table, claim.ids, 7, claim.keys)
    end

    test "stays idempotent for sealed and reaped ids, keys or not", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)
      :ok = HotManifest.retire(manifest, @table, claim.ids, 7, claim.keys)

      stale = ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SX.parquet"]
      assert HotManifest.retire(manifest, @table, claim.ids, 9, stale) == :ok
      assert HotManifest.retire(manifest, @table, ["01KYWPEEGAM8FQVQS5S2QF26SZ"], 9, stale) == :ok
    end
  end

  describe "live_claim/2" do
    test "is an error with nothing claimed", %{manifest: manifest} do
      _entry = add(manifest, @table)

      assert HotManifest.live_claim(manifest, @table) == :error
    end

    test "reports the outstanding claim", %{manifest: manifest} do
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)

      assert HotManifest.live_claim(manifest, @table) == {:ok, claim}
    end

    test "excludes entries written after the claim", %{manifest: manifest} do
      first = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [first.id], @keys)
      later = add(manifest, @table)

      assert {:ok, claim} = HotManifest.live_claim(manifest, @table)
      assert claim.ids == [first.id]
      refute later.id in claim.ids
    end

    test "goes away once the claim is retired", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      :ok = HotManifest.retire(manifest, @table, claim.ids, 9)

      assert HotManifest.live_claim(manifest, @table) == :error
    end
  end

  describe "retire/5 against a claim" do
    test "retires every member when told about one", %{manifest: manifest} do
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)

      assert HotManifest.retire(manifest, @table, [first.id], 11) == :ok

      assert manifest
             |> HotManifest.entries(@table)
             |> Enum.all?(&(&1.sealed_at == 11))
    end

    test "leaves an unclaimed entry alone", %{manifest: manifest} do
      claimed = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [claimed.id], @keys)
      loose = add(manifest, @table)

      :ok = HotManifest.retire(manifest, @table, [claimed.id], 11)

      assert {:ok, entry} = HotManifest.entry(manifest, @table, loose.id)
      refute Entry.sealed?(entry)
    end
  end

  describe "recovery" do
    test "replays a claim, so a restarted buffer re-signals the same set", context do
      manifest = context.manifest
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)

      restarted = start_manifest(context, context.store)
      restarted = %{restarted | log_dir: manifest.log_dir}

      assert {:ok, _report} = HotManifest.recover(restarted, @table)
      assert HotManifest.live_claim(restarted, @table) == {:ok, claim}
    end

    test "survives log compaction", %{manifest: manifest} do
      entry = add(manifest, @table)
      {:ok, claim} = HotManifest.claim(manifest, @table, [entry.id], @keys)

      assert HotManifest.compact(manifest, @table) == :ok
      assert {:ok, _report} = HotManifest.recover(manifest, @table)
      assert HotManifest.live_claim(manifest, @table) == {:ok, claim}
    end

    test "a claimed entry whose segment vanished keeps the claim's keys stable", context do
      manifest = context.manifest
      first = add(manifest, @table)
      second = add(manifest, @table)
      {:ok, _claim} = HotManifest.claim(manifest, @table, [first.id, second.id], @keys)

      :ok = Store.delete(context.store, first.key)

      assert {:ok, report} = HotManifest.recover(manifest, @table)
      assert report.missing == [first.id]

      assert {:ok, claim} = HotManifest.live_claim(manifest, @table)
      assert claim.ids == [second.id]
      assert claim.keys == @keys
    end
  end
end
