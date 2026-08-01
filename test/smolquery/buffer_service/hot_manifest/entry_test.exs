defmodule Smolquery.BufferService.HotManifest.EntryTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Segments.Segment

  @id "01KYWPEEGAM8FQVQS5S2QF26SV"

  defp segment do
    %Segment{
      id: @id,
      key: "analytics/events/#{@id}.parquet",
      path: "/data/analytics/events/#{@id}.parquet",
      row_count: 4,
      byte_size: 2_048,
      stats: %{
        "id" => %{min: 1, max: 4, null_count: 0},
        "ts" => %{
          min: ~N[2026-07-31 12:00:01.000000],
          max: ~N[2026-07-31 12:00:04.000000],
          null_count: 0
        },
        "day" => %{min: ~D[2026-08-01], max: ~D[2026-08-04], null_count: 1},
        "ratio" => %{min: 0.5, max: 2.0, null_count: 0},
        "name" => %{min: nil, max: nil, null_count: 2}
      }
    }
  end

  defp round_trip(entry) do
    {:ok, decoded} =
      entry |> Entry.to_record() |> JSON.encode!() |> JSON.decode!() |> Entry.from_record()

    decoded
  end

  describe "from_segment/2" do
    test "describes a freshly written, unsealed segment" do
      entry = Entry.from_segment(segment(), 1_700_000_000_000)

      assert entry.id == @id
      assert entry.key == "analytics/events/#{@id}.parquet"
      assert entry.row_count == 4
      assert entry.byte_size == 2_048
      assert entry.added_at == 1_700_000_000_000
      assert entry.sealed_at == nil
      assert entry.retired_at == nil
      refute Entry.sealed?(entry)
    end
  end

  describe "seal/3" do
    test "stamps the snapshot and the moment" do
      entry = segment() |> Entry.from_segment(1) |> Entry.seal(99, 1_700_000_000_000)

      assert entry.sealed_at == 99
      assert entry.retired_at == 1_700_000_000_000
      assert Entry.sealed?(entry)
    end
  end

  describe "records" do
    test "survive a round trip through JSON unchanged" do
      entry = Entry.from_segment(segment(), 1_700_000_000_000)

      assert round_trip(entry) == entry
    end

    test "keep a sealed stamp" do
      entry = segment() |> Entry.from_segment(1) |> Entry.seal(99, 2)

      assert round_trip(entry) == entry
    end

    test "bring timestamp and date bounds back as comparable terms" do
      decoded = segment() |> Entry.from_segment(1) |> round_trip()

      assert decoded.stats["ts"].min == ~N[2026-07-31 12:00:01.000000]
      assert decoded.stats["ts"].max == ~N[2026-07-31 12:00:04.000000]
      assert decoded.stats["day"].min == ~D[2026-08-01]
      assert NaiveDateTime.compare(decoded.stats["ts"].min, ~N[2026-07-31 12:00:02]) == :lt
    end

    test "keep numeric bounds and null counts" do
      decoded = segment() |> Entry.from_segment(1) |> round_trip()

      assert decoded.stats["id"] == %{min: 1, max: 4, null_count: 0}
      assert decoded.stats["ratio"] == %{min: 0.5, max: 2.0, null_count: 0}
      assert decoded.stats["name"] == %{min: nil, max: nil, null_count: 2}
    end

    test "report a record that is not an entry" do
      assert {:error, {:invalid_record, _}} = Entry.from_record(%{"op" => "add"})
      assert {:error, {:invalid_record, _}} = Entry.from_record(%{"id" => 1, "key" => "k"})
    end
  end

  describe "batch ids" do
    test "round-trip through the record" do
      entry = %Entry{
        id: "01KYWPEEGAM8FQVQS5S2QF26SV",
        key: "analytics/events/01KYWPEEGAM8FQVQS5S2QF26SV.parquet",
        row_count: 3,
        byte_size: 100,
        added_at: 1,
        batch_ids: ["batch-a", "batch-b"]
      }

      assert {:ok, restored} = entry |> Entry.to_record() |> Entry.from_record()
      assert restored.batch_ids == ["batch-a", "batch-b"]
    end

    test "a record from before batch ids existed restores empty" do
      record = %{
        "op" => "add",
        "id" => "01KYWPEEGAM8FQVQS5S2QF26SV",
        "key" => "analytics/events/01KYWPEEGAM8FQVQS5S2QF26SV.parquet",
        "row_count" => 3
      }

      assert {:ok, restored} = Entry.from_record(record)
      assert restored.batch_ids == []
    end
  end
end
