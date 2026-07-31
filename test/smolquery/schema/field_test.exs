defmodule Smolquery.Schema.FieldTest do
  use ExUnit.Case, async: true

  alias Smolquery.Schema.Field

  describe "new/3" do
    test "defaults to nullable, as BigQuery does" do
      assert {:ok, field} = Field.new("id", :int64)
      assert field == %Field{name: "id", type: :int64, nullable: true}
    end

    test "takes an explicit mode" do
      assert {:ok, field} = Field.new("id", :int64, nullable: false)
      refute field.nullable
    end

    test "rejects a name that is not a usable identifier" do
      assert Field.new("bad name", :int64) == {:error, {:invalid_identifier, "bad name"}}
    end

    test "rejects an unknown type" do
      assert Field.new("id", :int128) == {:error, {:unsupported_type, :int128}}
    end
  end

  describe "new!/3" do
    test "returns the field" do
      assert %Field{name: "ts"} = Field.new!("ts", :timestamp)
    end

    test "raises on an invalid field" do
      assert_raise ArgumentError, ~r/invalid field/, fn -> Field.new!("ts", :nope) end
    end
  end
end
