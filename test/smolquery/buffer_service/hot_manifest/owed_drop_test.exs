defmodule Smolquery.BufferService.HotManifest.OwedDropTest do
  @moduledoc """
  Owed replica drops (T-390): the durable record that a compensated flush's
  drop still has to reach the replicas, so a zombie copy cannot outlive its
  compensation (F-2, `tla/FINDINGS.md`).
  """

  use ExUnit.Case, async: true

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.Segments.Store

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @id "01KYWPEEGAM8FQVQS5S2QF26S0"
  @other "01KYWPEEGAM8FQVQS5S2QF26S1"

  setup context do
    store = Store.Local.new(dir: Path.join(context.tmp_dir, "segments"))
    name = :"manifest_#{:erlang.unique_integer([:positive])}"
    start_supervised!({HotManifest, name: name})

    manifest =
      HotManifest.new(name: name, log_dir: Path.join(context.tmp_dir, "logs"), store: store)

    %{manifest: manifest}
  end

  test "owe records the ids; settle clears them, idempotently", %{manifest: manifest} do
    assert HotManifest.owe_drop(manifest, @table, [@id, @id]) == :ok
    assert HotManifest.owed_drops(manifest, @table) == [@id]

    assert HotManifest.settle_drop(manifest, @table, [@id, @other]) == :ok
    assert HotManifest.owed_drops(manifest, @table) == []

    assert HotManifest.settle_drop(manifest, @table, [@id]) == :ok
    assert HotManifest.owe_drop(manifest, @table, []) == :ok
    assert HotManifest.owed_drops(manifest, @table) == []
  end

  test "an owed drop survives recovery; a settled one stays settled", %{manifest: manifest} do
    :ok = HotManifest.owe_drop(manifest, @table, [@id])

    assert {:ok, _report} = HotManifest.recover(manifest, @table)
    assert HotManifest.owed_drops(manifest, @table) == [@id]

    :ok = HotManifest.settle_drop(manifest, @table, [@id])

    assert {:ok, _report} = HotManifest.recover(manifest, @table)
    assert HotManifest.owed_drops(manifest, @table) == []
  end

  test "an owed drop survives log compaction", %{manifest: manifest} do
    :ok = HotManifest.owe_drop(manifest, @table, [@id, @other])

    assert :ok = HotManifest.compact(manifest, @table)
    assert {:ok, _report} = HotManifest.recover(manifest, @table)

    assert Enum.sort(HotManifest.owed_drops(manifest, @table)) == [@id, @other]
  end
end
