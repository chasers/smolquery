defmodule Smolquery.Segments.SegmentTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Segment

  defp segment do
    %Segment{
      id: "01KYWPEEGAM8FQVQS5S2QF26SV",
      path: "/data/segments/01KYWPEEGAM8FQVQS5S2QF26SV.parquet",
      row_count: 2,
      byte_size: 1_024,
      stats: %{"id" => %{min: 1, max: 2, null_count: 0}}
    }
  end

  describe "column_stats/2" do
    test "returns the stats for a column the segment carries" do
      assert Segment.column_stats(segment(), "id") == {:ok, %{min: 1, max: 2, null_count: 0}}
    end

    test "reports a column it has no stats for" do
      assert Segment.column_stats(segment(), "missing") == :error
    end
  end
end
