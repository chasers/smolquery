defmodule Smolquery.IdentifierTest do
  use ExUnit.Case, async: true

  alias Smolquery.Identifier

  describe "valid?/1" do
    test "accepts names a table or column may plausibly have" do
      assert Identifier.valid?("events")
      assert Identifier.valid?("_private")
      assert Identifier.valid?("Events2")
      assert Identifier.valid?("user_id")
      assert Identifier.valid?(String.duplicate("a", 63))
    end

    test "rejects anything that could carry SQL" do
      refute Identifier.valid?("")
      refute Identifier.valid?("1events")
      refute Identifier.valid?("has space")
      refute Identifier.valid?(~s(quote"inside))
      refute Identifier.valid?("drop; DROP TABLE t")
      refute Identifier.valid?("café")
      refute Identifier.valid?(String.duplicate("a", 64))
    end

    test "rejects non-binaries" do
      refute Identifier.valid?(:events)
      refute Identifier.valid?(nil)
      refute Identifier.valid?(1)
    end
  end

  describe "validate/1" do
    test "returns the name when usable" do
      assert Identifier.validate("events") == {:ok, "events"}
    end

    test "tags the offending value" do
      assert Identifier.validate("bad name") == {:error, {:invalid_identifier, "bad name"}}
    end
  end

  describe "quote_name!/1" do
    test "double-quotes a valid name" do
      assert Identifier.quote_name!("events") == ~s("events")
    end

    test "raises rather than emit an unsafe identifier" do
      assert_raise ArgumentError, ~r/invalid SQL identifier/, fn ->
        Identifier.quote_name!(~s(t" ; DROP TABLE x --))
      end
    end
  end

  describe "sql_string/1" do
    test "single-quotes a literal" do
      assert Identifier.sql_string("/data/seg.parquet") == "'/data/seg.parquet'"
    end

    test "escapes embedded quotes by doubling them" do
      assert Identifier.sql_string("it's") == "'it''s'"
      assert Identifier.sql_string("' OR 1=1 --") == "''' OR 1=1 --'"
    end
  end
end
