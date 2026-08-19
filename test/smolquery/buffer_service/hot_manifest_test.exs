defmodule Smolquery.BufferService.HotManifestTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer
  alias Smolquery.Test.MemoryStore

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @other {"analytics", "clicks"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}, {"name", :string}])
  end

  defp rows(count) do
    for i <- 1..count do
      %{"id" => i, "ts" => NaiveDateTime.add(~N[2026-07-31 12:00:00], i), "name" => "row-#{i}"}
    end
  end

  defp start_manifest(context, store) do
    name = :"manifest_#{:erlang.unique_integer([:positive])}"
    start_supervised!({HotManifest, name: name})

    HotManifest.new(name: name, log_dir: Path.join(context.tmp_dir, "logs"), store: store)
  end

  defp write(manifest, table_ref, rows) do
    {:ok, prefix} = Store.prefix(table_ref)
    {:ok, segment} = Writer.write(rows, schema(), store: manifest.store, prefix: prefix)

    segment
  end

  defp add(manifest, table_ref, rows) do
    {:ok, entry} = HotManifest.add(manifest, table_ref, write(manifest, table_ref, rows))

    entry
  end

  setup context do
    %{local: Store.Local.new(dir: Path.join(context.tmp_dir, "segments"))}
  end

  describe "add/3" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "makes a written segment part of the hot tier", %{manifest: manifest} do
      segment = write(manifest, @table, rows(3))

      assert {:ok, %Entry{} = entry} = HotManifest.add(manifest, @table, segment)

      assert entry.id == segment.id
      assert entry.key == segment.key
      assert entry.row_count == 3
      assert entry.byte_size == segment.byte_size
      assert entry.sealed_at == nil
      assert entry.retired_at == nil
      assert entry.added_at > 0

      assert HotManifest.entries(manifest, @table) == [entry]
    end

    test "carries the segment's stats, so the planner can prune", %{manifest: manifest} do
      entry = add(manifest, @table, rows(4))

      assert entry.stats["id"] == %{min: 1, max: 4, null_count: 0}
      assert entry.stats["ts"].min == ~N[2026-07-31 12:00:01.000000]
    end

    test "is durable before it is visible", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      {:ok, path} = HotManifest.log_path(manifest, @table)

      assert File.read!(path) =~ entry.id
    end

    test "keeps tables apart", %{manifest: manifest} do
      one = add(manifest, @table, rows(1))
      two = add(manifest, @other, rows(1))

      assert HotManifest.entries(manifest, @table) == [one]
      assert HotManifest.entries(manifest, @other) == [two]
    end

    test "orders entries oldest first", %{manifest: manifest} do
      entries = for _ <- 1..3, do: add(manifest, @table, rows(1))

      assert HotManifest.entries(manifest, @table) == Enum.sort_by(entries, & &1.id)
    end

    test "refuses a table name that is not an identifier", %{manifest: manifest} do
      segment = write(manifest, @table, rows(1))

      assert {:error, {:invalid_identifier, _}} =
               HotManifest.add(manifest, {"../etc", "events"}, segment)
    end
  end

  describe "entries/3" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "reads the whole table, oldest first", %{manifest: manifest} do
      first = add(manifest, @table, rows(1))
      second = add(manifest, @table, rows(2))

      assert HotManifest.entries(manifest, @table) == [first, second]
    end

    test "narrows to the ids a claim names, in the same order (T-316)", %{manifest: manifest} do
      first = add(manifest, @table, rows(1))
      _second = add(manifest, @table, rows(2))
      third = add(manifest, @table, rows(3))

      assert HotManifest.entries(manifest, @table, [third.id, first.id]) == [first, third]
    end

    test "an id the table no longer holds is absent, not an error", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert HotManifest.entries(manifest, @table, [entry.id, Id.generate()]) == [entry]
    end

    test "an empty id list reads nothing, whatever the table holds", %{manifest: manifest} do
      add(manifest, @table, rows(1))

      assert HotManifest.entries(manifest, @table, []) == []
    end

    test "does not leak an id across tables", %{manifest: manifest} do
      entry = add(manifest, @other, rows(1))

      assert HotManifest.entries(manifest, @table, [entry.id]) == []
    end
  end

  describe "pending/3 and empty?/2 (T-317)" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "reads the unsealed entries, oldest first", %{manifest: manifest} do
      first = add(manifest, @table, rows(1))
      second = add(manifest, @table, rows(2))

      assert HotManifest.pending(manifest, @table) == [first, second]
    end

    test "leaves a sealed entry out, without copying it", %{manifest: manifest} do
      sealed = add(manifest, @table, rows(1))
      unsealed = add(manifest, @table, rows(2))
      :ok = HotManifest.retire(manifest, @table, [sealed.id], 9)

      assert HotManifest.pending(manifest, @table) == [unsealed]
    end

    test "stops at the limit, taking the oldest", %{manifest: manifest} do
      entries = for _ <- 1..5, do: add(manifest, @table, rows(1))

      assert HotManifest.pending(manifest, @table, 2) == Enum.take(entries, 2)
      assert HotManifest.pending(manifest, @table, 1) == Enum.take(entries, 1)
      assert HotManifest.pending(manifest, @table, 99) == entries
    end

    test "answers a limit against an empty table without a continuation", %{manifest: manifest} do
      assert HotManifest.pending(manifest, @table, 4) == []
    end

    test "does not leak entries across tables", %{manifest: manifest} do
      mine = add(manifest, @table, rows(1))
      add(manifest, @other, rows(1))

      assert HotManifest.pending(manifest, @table) == [mine]
    end

    test "empty? is true only while the table holds nothing", %{manifest: manifest} do
      assert HotManifest.empty?(manifest, @table)

      entry = add(manifest, @table, rows(1))

      refute HotManifest.empty?(manifest, @table)

      :ok = HotManifest.retire(manifest, @table, [entry.id], 3)

      refute HotManifest.empty?(manifest, @table)

      :ok = HotManifest.drop(manifest, @table, [entry.id])

      assert HotManifest.empty?(manifest, @table)
    end

    test "empty? does not answer for a sibling table", %{manifest: manifest} do
      add(manifest, @other, rows(1))

      assert HotManifest.empty?(manifest, @table)
      refute HotManifest.empty?(manifest, @other)
    end
  end

  describe "live_claim/2 (T-318)" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "names the claim's ids oldest first, from the index's own order", %{manifest: manifest} do
      entries = for _ <- 1..4, do: add(manifest, @table, rows(1))
      ids = Enum.map(entries, & &1.id)
      {:ok, claim} = HotManifest.claim(manifest, @table, ids, ["analytics/events/sealed.parquet"])

      assert HotManifest.live_claim(manifest, @table) == {:ok, claim}
      assert claim.ids == Enum.sort(ids)
    end

    test "ignores entries the claim does not hold", %{manifest: manifest} do
      claimed = add(manifest, @table, rows(1))
      _unclaimed = add(manifest, @table, rows(1))
      keys = ["analytics/events/sealed.parquet"]

      {:ok, _claim} = HotManifest.claim(manifest, @table, [claimed.id], keys)

      assert HotManifest.live_claim(manifest, @table) == {:ok, %{ids: [claimed.id], keys: keys}}
    end

    test "a fully sealed claim is no longer live", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      keys = ["analytics/events/sealed.parquet"]
      {:ok, _claim} = HotManifest.claim(manifest, @table, [entry.id], keys)

      :ok = HotManifest.retire(manifest, @table, [entry.id], 4, keys)

      assert HotManifest.live_claim(manifest, @table) == :error
    end

    test "does not answer with a sibling table's claim", %{manifest: manifest} do
      entry = add(manifest, @other, rows(1))
      keys = ["analytics/clicks/sealed.parquet"]
      {:ok, _claim} = HotManifest.claim(manifest, @other, [entry.id], keys)

      assert HotManifest.live_claim(manifest, @table) == :error
      assert HotManifest.live_claim(manifest, @other) == {:ok, %{ids: [entry.id], keys: keys}}
    end
  end

  describe "live_claim/2 is cached but never stale (T-318)" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    defp derived_claim(manifest, table_ref) do
      manifest
      |> HotManifest.entries(table_ref)
      |> Enum.reject(&(Entry.sealed?(&1) or not Entry.claimed?(&1)))
      |> case do
        [] -> :error
        claimed -> {:ok, %{ids: Enum.map(claimed, & &1.id), keys: hd(claimed).claim_keys}}
      end
    end

    defp assert_matches_derivation(manifest, table_ref) do
      assert HotManifest.live_claim(manifest, table_ref) == derived_claim(manifest, table_ref)
    end

    test "matches a fresh derivation through claim, drop and retire", %{manifest: manifest} do
      keys = ["analytics/events/sealed.parquet"]
      entries = for _ <- 1..4, do: add(manifest, @table, rows(1))
      [first, second, third, fourth] = Enum.map(entries, & &1.id)

      assert_matches_derivation(manifest, @table)

      {:ok, _claim} = HotManifest.claim(manifest, @table, [first, second, third], keys)
      assert_matches_derivation(manifest, @table)
      assert {:ok, %{ids: [^first, ^second, ^third]}} = HotManifest.live_claim(manifest, @table)

      :ok = HotManifest.drop(manifest, @table, [second])
      assert_matches_derivation(manifest, @table)
      assert {:ok, %{ids: [^first, ^third]}} = HotManifest.live_claim(manifest, @table)

      # Retiring any member seals the whole claim — the input set never moves.
      :ok = HotManifest.retire(manifest, @table, [first], 7, keys)
      assert HotManifest.live_claim(manifest, @table) == :error
      assert_matches_derivation(manifest, @table)

      refute Entry.claimed?(elem(HotManifest.entry(manifest, @table, fourth), 1))
    end

    test "matches a fresh derivation after a release", %{manifest: manifest} do
      keys = ["analytics/events/sealed.parquet"]
      entries = for _ <- 1..3, do: add(manifest, @table, rows(1))
      ids = Enum.map(entries, & &1.id)

      {:ok, _claim} = HotManifest.claim(manifest, @table, ids, keys)
      assert {:ok, _live} = HotManifest.live_claim(manifest, @table)

      :ok = HotManifest.release(manifest, @table, ids)

      assert HotManifest.live_claim(manifest, @table) == :error
      assert_matches_derivation(manifest, @table)
    end

    test "is rebuilt by recovery, not carried over", %{manifest: manifest} do
      keys = ["analytics/events/sealed.parquet"]
      entries = for _ <- 1..3, do: add(manifest, @table, rows(1))
      ids = Enum.map(entries, & &1.id)
      {:ok, _claim} = HotManifest.claim(manifest, @table, ids, keys)

      :ets.delete_all_objects(manifest.table)
      assert HotManifest.live_claim(manifest, @table) == {:ok, %{ids: ids, keys: keys}}

      assert {:ok, _report} = HotManifest.recover(manifest, @table)

      assert HotManifest.live_claim(manifest, @table) == {:ok, %{ids: ids, keys: keys}}
      assert_matches_derivation(manifest, @table)
    end

    test "does not answer with a sibling table's claim", %{manifest: manifest} do
      entry = add(manifest, @other, rows(1))
      keys = ["analytics/clicks/sealed.parquet"]
      {:ok, _claim} = HotManifest.claim(manifest, @other, [entry.id], keys)

      assert HotManifest.live_claim(manifest, @table) == :error
      assert HotManifest.live_claim(manifest, @other) == {:ok, %{ids: [entry.id], keys: keys}}
    end
  end

  describe "index size events (T-320)" do
    setup context do
      handler = "index-size-#{:erlang.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:smolquery, :hot_manifest, :change],
        fn _event, measurements, meta, _config ->
          send(test, {:change, meta.change, measurements.entries})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      %{manifest: start_manifest(context, context.local)}
    end

    test "reports an entry entering, retiring, and being reaped", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      assert_received {:change, :added, 1}

      :ok = HotManifest.retire(manifest, @table, [entry.id], 5)
      assert_received {:change, :retired, 1}

      :ok = HotManifest.drop(manifest, @table, [entry.id])
      assert_received {:change, :reaped, 1}
    end

    test "a drop of an id it never held reports nothing", %{manifest: manifest} do
      :ok = HotManifest.drop(manifest, @table, [Id.generate()])

      refute_received {:change, :reaped, _entries}
    end

    test "recovery reports what it restored, so the arithmetic survives a restart", %{
      manifest: manifest
    } do
      for _ <- 1..3, do: add(manifest, @table, rows(1))
      :ets.delete_all_objects(manifest.table)

      assert {:ok, %{entries: 3}} = HotManifest.recover(manifest, @table)
      assert_received {:change, :recovered, 3}
    end
  end

  describe "entry/3" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "resolves a segment id through the manifest", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert HotManifest.entry(manifest, @table, entry.id) == {:ok, entry}
    end

    test "reports an id the table does not hold", %{manifest: manifest} do
      assert HotManifest.entry(manifest, @table, Id.generate()) == :error
    end

    test "does not leak an id across tables", %{manifest: manifest} do
      entry = add(manifest, @other, rows(1))

      assert HotManifest.entry(manifest, @table, entry.id) == :error
    end
  end

  describe "retire/4" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "stamps the snapshot and leaves the segment readable", %{manifest: manifest} do
      entry = add(manifest, @table, rows(2))

      assert HotManifest.retire(manifest, @table, [entry.id], 42) == :ok

      assert {:ok, retired} = HotManifest.entry(manifest, @table, entry.id)
      assert retired.sealed_at == 42
      assert retired.retired_at > 0
      assert Entry.sealed?(retired)
      assert File.exists?(Store.location(manifest.store, retired.key))
    end

    test "is idempotent for an already retired id", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert HotManifest.retire(manifest, @table, [entry.id], 7) == :ok
      assert HotManifest.retire(manifest, @table, [entry.id], 9) == :ok

      assert {:ok, retired} = HotManifest.entry(manifest, @table, entry.id)
      assert retired.sealed_at == 7
    end

    test "is idempotent for an id the reaper already dropped", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      :ok = HotManifest.drop(manifest, @table, [entry.id])

      assert HotManifest.retire(manifest, @table, [entry.id], 7) == :ok
    end

    test "is idempotent for an id it never held", %{manifest: manifest} do
      assert HotManifest.retire(manifest, @table, [Id.generate()], 7) == :ok
      assert HotManifest.retire(manifest, @table, [], 7) == :ok
    end

    test "retires a set in one record", %{manifest: manifest} do
      entries = for _ <- 1..3, do: add(manifest, @table, rows(1))
      ids = Enum.map(entries, & &1.id)

      assert HotManifest.retire(manifest, @table, ids, 5) == :ok

      assert Enum.all?(HotManifest.entries(manifest, @table), &(&1.sealed_at == 5))
    end
  end

  describe "drop/3" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "removes the segment from the store and the manifest", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      location = Store.location(manifest.store, entry.key)

      assert HotManifest.drop(manifest, @table, [entry.id]) == :ok

      refute File.exists?(location)
      assert HotManifest.entries(manifest, @table) == []
    end

    test "is idempotent", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert HotManifest.drop(manifest, @table, [entry.id]) == :ok
      assert HotManifest.drop(manifest, @table, [entry.id]) == :ok
      assert HotManifest.drop(manifest, @table, []) == :ok
    end

    test "appends no record when there is nothing to drop", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))
      {:ok, path} = HotManifest.log_path(manifest, @table)

      :ok = HotManifest.drop(manifest, @table, [entry.id])
      compacted = File.read!(path)

      assert HotManifest.drop(manifest, @table, [entry.id]) == :ok
      assert HotManifest.drop(manifest, @table, ["01KYWPEEGAM8FQVQS5S2QF26SV"]) == :ok
      assert File.read!(path) == compacted
    end

    test "leaves other entries alone", %{manifest: manifest} do
      dropped = add(manifest, @table, rows(1))
      kept = add(manifest, @table, rows(1))

      assert HotManifest.drop(manifest, @table, [dropped.id]) == :ok
      assert HotManifest.entries(manifest, @table) == [kept]
    end
  end

  describe "a held log" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "carries add, retire, and drop through one fd", %{manifest: manifest} do
      {:ok, log} = HotManifest.open_log(manifest, @table)

      segment = write(manifest, @table, rows(2))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, log)

      assert HotManifest.retire(manifest, @table, [entry.id], 7, nil, log) == :ok
      assert HotManifest.drop(manifest, @table, [entry.id], log) == :ok
      assert HotManifest.close_log(log) == :ok

      assert HotManifest.recover(manifest, @table) ==
               {:ok, %{entries: 0, orphans: [], missing: []}}
    end

    test "records appended through a held log recover identically", context do
      manifest = context.manifest
      {:ok, log} = HotManifest.open_log(manifest, @table)

      entries =
        for _ <- 1..2 do
          {:ok, entry} = HotManifest.add(manifest, @table, write(manifest, @table, rows(1)), log)

          entry
        end

      :ok = HotManifest.close_log(log)

      restarted = %{start_manifest(context, context.local) | log_dir: manifest.log_dir}
      {:ok, report} = HotManifest.recover(restarted, @table)

      assert report.entries == 2

      assert Enum.map(HotManifest.entries(restarted, @table), & &1.id) ==
               entries |> Enum.map(& &1.id) |> Enum.sort()
    end

    test "open_log creates the table's log directories", %{manifest: manifest} do
      fresh = {"analytics", "fresh"}

      assert {:ok, log} = HotManifest.open_log(manifest, fresh)
      {:ok, path} = HotManifest.log_path(manifest, fresh)

      assert File.exists?(path)
      assert HotManifest.close_log(log) == :ok
    end
  end

  describe "retired_before/3" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "reports only entries retired before the cutoff", %{manifest: manifest} do
      retired = add(manifest, @table, rows(1))
      unretired = add(manifest, @table, rows(1))

      :ok = HotManifest.retire(manifest, @table, [retired.id], 1)

      future = System.os_time(:millisecond) + 1_000
      past = System.os_time(:millisecond) - 1_000

      assert Enum.map(HotManifest.retired_before(manifest, @table, future), & &1.id) ==
               [retired.id]

      assert HotManifest.retired_before(manifest, @table, past) == []
      refute Entry.sealed?(unretired)
    end

    test "does not leak a sibling table's retired entries", %{manifest: manifest} do
      mine = add(manifest, @table, rows(1))
      theirs = add(manifest, @other, rows(1))

      :ok = HotManifest.retire(manifest, @table, [mine.id], 1)
      :ok = HotManifest.retire(manifest, @other, [theirs.id], 1)

      future = System.os_time(:millisecond) + 1_000

      assert Enum.map(HotManifest.retired_before(manifest, @table, future), & &1.id) == [mine.id]
    end
  end

  describe "recover/2" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "rebuilds entries from the log after the index is lost", context do
      manifest = context.manifest
      entries = for _ <- 1..3, do: add(manifest, @table, rows(2))
      :ok = HotManifest.retire(manifest, @table, [hd(entries).id], 11)

      restarted = start_manifest(context, context.local)
      restarted = %{restarted | log_dir: manifest.log_dir}

      assert {:ok, report} = HotManifest.recover(restarted, @table)
      assert report.entries == 3
      assert report.orphans == []
      assert report.missing == []

      recovered = HotManifest.entries(restarted, @table)

      assert Enum.map(recovered, & &1.id) == Enum.map(Enum.sort_by(entries, & &1.id), & &1.id)
      assert Enum.find(recovered, &(&1.id == hd(entries).id)).sealed_at == 11
    end

    test "round-trips stats through the log", context do
      manifest = context.manifest
      entry = add(manifest, @table, rows(4))

      restarted = %{start_manifest(context, context.local) | log_dir: manifest.log_dir}
      {:ok, _report} = HotManifest.recover(restarted, @table)

      assert [recovered] = HotManifest.entries(restarted, @table)
      assert recovered.stats == entry.stats
      assert recovered.stats["ts"].min == ~N[2026-07-31 12:00:01.000000]
    end

    test "deletes a segment the log never recorded", context do
      manifest = context.manifest
      logged = add(manifest, @table, rows(1))
      unlogged = write(manifest, @table, rows(1))

      assert {:ok, report} = HotManifest.recover(manifest, @table)

      assert report.orphans == [unlogged.key]
      refute File.exists?(Store.location(manifest.store, unlogged.key))
      assert Enum.map(HotManifest.entries(manifest, @table), & &1.id) == [logged.id]
    end

    test "drops a record whose segment is gone from the store", context do
      manifest = context.manifest
      entry = add(manifest, @table, rows(1))
      File.rm!(Store.location(manifest.store, entry.key))

      assert {:ok, report} = HotManifest.recover(manifest, @table)

      assert report.missing == [entry.id]
      assert report.entries == 0
      assert HotManifest.entries(manifest, @table) == []
    end

    test "keeps a healed record dropped on a second recovery", context do
      manifest = context.manifest
      entry = add(manifest, @table, rows(1))
      File.rm!(Store.location(manifest.store, entry.key))

      {:ok, _first} = HotManifest.recover(manifest, @table)

      assert {:ok, second} = HotManifest.recover(manifest, @table)
      assert second.missing == []
      assert second.entries == 0
    end

    test "tolerates a torn final log line", context do
      manifest = context.manifest
      kept = add(manifest, @table, rows(1))
      {:ok, path} = HotManifest.log_path(manifest, @table)

      File.write!(path, ~s({"op":"add","id":"partial), [:append])

      assert {:ok, report} = HotManifest.recover(manifest, @table)
      assert report.entries == 1
      assert Enum.map(HotManifest.entries(manifest, @table), & &1.id) == [kept.id]
    end

    test "truncates a torn tail, so a later append cannot garble the log", context do
      manifest = context.manifest
      kept = add(manifest, @table, rows(1))
      {:ok, path} = HotManifest.log_path(manifest, @table)
      intact = File.read!(path)

      File.write!(path, ~s({"op":"add","id":"partial), [:append])
      {:ok, _report} = HotManifest.recover(manifest, @table)

      assert File.read!(path) == intact

      added = add(manifest, @table, rows(1))

      assert {:ok, report} = HotManifest.recover(manifest, @table)
      assert report.entries == 2

      assert manifest |> HotManifest.entries(@table) |> Enum.map(& &1.id) |> Enum.sort() ==
               Enum.sort([kept.id, added.id])
    end

    test "truncates before its own reconciling append, not after", context do
      manifest = context.manifest
      entry = add(manifest, @table, rows(1))
      File.rm!(Store.location(manifest.store, entry.key))
      {:ok, path} = HotManifest.log_path(manifest, @table)

      File.write!(path, ~s({"op":"add","id":"partial), [:append])

      assert {:ok, report} = HotManifest.recover(manifest, @table)
      assert report.missing == [entry.id]

      assert {:ok, second} = HotManifest.recover(manifest, @table)
      assert second.entries == 0
    end

    test "refuses a log corrupt anywhere but the last line", context do
      manifest = context.manifest
      _entry = add(manifest, @table, rows(1))
      {:ok, path} = HotManifest.log_path(manifest, @table)

      File.write!(path, "not json\n" <> File.read!(path))

      assert {:error, {:corrupt_log, 1, _reason}} = HotManifest.recover(manifest, @table)
    end

    test "recovers a table it has no log for", %{manifest: manifest} do
      assert HotManifest.recover(manifest, @table) ==
               {:ok, %{entries: 0, orphans: [], missing: []}}
    end
  end

  describe "tables/1" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "reports every table a log exists for", %{manifest: manifest} do
      add(manifest, @table, rows(1))
      add(manifest, @other, rows(1))

      assert HotManifest.tables(manifest) == Enum.sort([@table, @other])
    end

    test "reports nothing on a fresh node", %{manifest: manifest} do
      assert HotManifest.tables(manifest) == []
    end
  end

  describe "compact/2" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "shrinks the log to the live tail without changing what it means", context do
      manifest = context.manifest
      dropped = add(manifest, @table, rows(1))
      kept = add(manifest, @table, rows(1))
      :ok = HotManifest.retire(manifest, @table, [kept.id], 3)
      :ok = HotManifest.drop(manifest, @table, [dropped.id])

      {:ok, path} = HotManifest.log_path(manifest, @table)
      before_size = File.stat!(path).size

      assert HotManifest.compact(manifest, @table) == :ok
      assert File.stat!(path).size < before_size

      restarted = %{start_manifest(context, context.local) | log_dir: manifest.log_dir}
      {:ok, report} = HotManifest.recover(restarted, @table)

      assert report.entries == 1
      assert [recovered] = HotManifest.entries(restarted, @table)
      assert recovered.id == kept.id
      assert recovered.sealed_at == 3
    end

    test "leaves no staged file behind", %{manifest: manifest} do
      add(manifest, @table, rows(1))

      assert HotManifest.compact(manifest, @table) == :ok

      {:ok, path} = HotManifest.log_path(manifest, @table)
      assert File.ls!(Path.dirname(path)) == ["manifest.log"]
    end
  end

  describe "against a store that is not a filesystem" do
    setup context do
      %{manifest: start_manifest(context, MemoryStore.new())}
    end

    test "adds, retires and reads entries the same way", %{manifest: manifest} do
      entry = add(manifest, @table, rows(3))

      assert entry.row_count == 3
      assert HotManifest.entries(manifest, @table) == [entry]

      assert HotManifest.retire(manifest, @table, [entry.id], 4) == :ok
      assert {:ok, retired} = HotManifest.entry(manifest, @table, entry.id)
      assert retired.sealed_at == 4
    end

    test "recovers from the log and reconciles against the store", context do
      manifest = context.manifest
      logged = add(manifest, @table, rows(1))
      unlogged = write(manifest, @table, rows(1))

      assert {:ok, report} = HotManifest.recover(manifest, @table)

      assert report.orphans == [unlogged.key]
      assert MemoryStore.fetch(manifest.store, unlogged.key) == :error
      assert Enum.map(HotManifest.entries(manifest, @table), & &1.id) == [logged.id]
    end

    test "drops a segment through the store, not the filesystem", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert {:ok, _contents} = MemoryStore.fetch(manifest.store, entry.key)
      assert HotManifest.drop(manifest, @table, [entry.id]) == :ok
      assert MemoryStore.fetch(manifest.store, entry.key) == :error
    end

    test "reports a location no filesystem can open", %{manifest: manifest} do
      entry = add(manifest, @table, rows(1))

      assert Store.location(manifest.store, entry.key) == "memory://" <> entry.key
      refute File.exists?(Store.location(manifest.store, entry.key))
    end
  end

  describe "batch dedup (T-41)" do
    setup(context, do: %{manifest: start_manifest(context, context.local)})

    test "add with batch ids makes batch_ack answer with the commit's ack", %{
      manifest: manifest
    } do
      segment = write(manifest, @table, rows(3))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, nil, ["batch-a", "batch-b"])

      ack = %{segment_id: entry.id, row_count: 3}
      assert HotManifest.batch_ack(manifest, @table, "batch-a") == {:ok, ack}
      assert HotManifest.batch_ack(manifest, @table, "batch-b") == {:ok, ack}
      assert HotManifest.batch_ack(manifest, @table, "batch-c") == :error
      assert HotManifest.batch_ack(manifest, @other, "batch-a") == :error
    end

    test "an id lives exactly as long as its entry", %{manifest: manifest} do
      segment = write(manifest, @table, rows(2))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, nil, ["batch-a"])

      :ok = HotManifest.drop(manifest, @table, [entry.id])

      assert HotManifest.batch_ack(manifest, @table, "batch-a") == :error
    end

    test "retirement keeps the id answering until the reaper drops the entry", %{
      manifest: manifest
    } do
      segment = write(manifest, @table, rows(2))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, nil, ["batch-a"])

      {:ok, _claim} =
        HotManifest.claim(manifest, @table, [entry.id], ["analytics/events/#{entry.id}.parquet"])

      :ok = HotManifest.retire(manifest, @table, [entry.id], 7)

      assert {:ok, _ack} = HotManifest.batch_ack(manifest, @table, "batch-a")
    end

    test "recovery rebuilds the index from the log", context do
      manifest = start_manifest(context, context.local)
      segment = write(manifest, @table, rows(2))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, nil, ["batch-a"])

      rebuilt = start_manifest(context, context.local)
      {:ok, _report} = HotManifest.recover(rebuilt, @table)

      assert HotManifest.batch_ack(rebuilt, @table, "batch-a") ==
               {:ok, %{segment_id: entry.id, row_count: 2}}
    end

    test "batch index rows never pollute the entry listing", %{manifest: manifest} do
      segment = write(manifest, @table, rows(1))
      {:ok, entry} = HotManifest.add(manifest, @table, segment, nil, ["batch-a"])

      assert [%Entry{id: id}] = HotManifest.entries(manifest, @table)
      assert id == entry.id
    end
  end
end
