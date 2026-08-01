defmodule Smolquery.Segments.StoreTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store

  @id "01KYWPEEGAM8FQVQS5S2QF26SV"

  describe "prefix/1" do
    test "joins a validated dataset and table" do
      assert Store.prefix({"analytics", "events"}) == {:ok, "analytics/events"}
    end

    test "refuses a dataset or table that is not an identifier" do
      assert {:error, {:invalid_identifier, _}} = Store.prefix({"../etc", "events"})
      assert {:error, {:invalid_identifier, _}} = Store.prefix({"analytics", "ev/ents"})
    end
  end

  describe "key/2" do
    test "names a segment under a prefix" do
      assert Store.key("analytics/events", @id) == {:ok, "analytics/events/#{@id}.parquet"}
    end

    test "names a segment at the store root" do
      assert Store.key("", @id) == {:ok, "#{@id}.parquet"}
    end

    test "refuses an id that is not a ULID" do
      assert Store.key("", "../escape") == {:error, {:invalid_segment_id, "../escape"}}
    end

    test "refuses a prefix that could climb out of the store" do
      assert Store.key("../../etc", @id) == {:error, {:invalid_prefix, "../../etc"}}
      assert Store.key("analytics/../..", @id) == {:error, {:invalid_prefix, "analytics/../.."}}
      assert Store.key("/absolute", @id) == {:error, {:invalid_prefix, "/absolute"}}
      assert Store.key("trailing/", @id) == {:error, {:invalid_prefix, "trailing/"}}
    end

    test "refuses a prefix that is not a string" do
      assert Store.key(:analytics, @id) == {:error, {:invalid_prefix, :analytics}}
    end
  end

  describe "id/1" do
    test "recovers the segment id a key names" do
      assert Store.id("analytics/events/#{@id}.parquet") == {:ok, @id}
      assert Store.id("#{@id}.parquet") == {:ok, @id}
    end

    test "round-trips whatever key/2 produced" do
      id = Id.generate()
      {:ok, key} = Store.key("analytics/events", id)

      assert Store.id(key) == {:ok, id}
    end

    test "reports a key that does not name a segment" do
      assert Store.id("analytics/events/manifest.log") == :error
      assert Store.id("") == :error
    end
  end
end
