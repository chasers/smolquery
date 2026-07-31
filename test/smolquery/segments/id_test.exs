defmodule Smolquery.Segments.IdTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Id

  describe "generate/0" do
    test "produces a 26-character Crockford base32 id" do
      id = Id.generate()

      assert String.length(id) == 26
      assert id =~ ~r/^[0-9A-HJKMNP-TV-Z]{26}$/
    end

    test "does not repeat" do
      ids = Enum.map(1..1_000, fn _index -> Id.generate() end)

      assert Enum.uniq(ids) == ids
    end

    test "stamps the current time" do
      before = System.system_time(:millisecond)
      {:ok, stamped} = Id.timestamp(Id.generate())

      assert stamped >= before
      assert stamped <= System.system_time(:millisecond)
    end
  end

  describe "generate/1" do
    test "sorts chronologically by the millisecond prefix" do
      early = Id.generate(1_000_000_000_000)
      late = Id.generate(1_000_000_000_001)

      assert early < late
    end

    test "shares a prefix within the same millisecond" do
      timestamp = 1_785_000_000_000
      a = Id.generate(timestamp)
      b = Id.generate(timestamp)

      assert String.slice(a, 0, 10) == String.slice(b, 0, 10)
      assert a != b
    end
  end

  describe "timestamp/1" do
    test "recovers the millisecond it was stamped with" do
      assert Id.timestamp(Id.generate(1_785_000_000_000)) == {:ok, 1_785_000_000_000}
    end

    test "reports ids it cannot read" do
      assert Id.timestamp("nope") == :error
      assert Id.timestamp(nil) == :error
    end
  end

  describe "valid?/1" do
    test "accepts generated ids" do
      assert Id.valid?(Id.generate())
    end

    test "rejects the wrong length, alphabet, or type" do
      refute Id.valid?("")
      refute Id.valid?(String.duplicate("0", 25))
      refute Id.valid?(String.duplicate("0", 27))
      refute Id.valid?(String.duplicate("U", 26))
      refute Id.valid?("01KYWPEEGAM8FQVQS5S2QF26S!")
      refute Id.valid?(:not_a_binary)
    end

    test "rejects an id whose 130 encoded bits overflow 128" do
      refute Id.valid?("8" <> String.duplicate("0", 25))
      assert Id.valid?("7" <> String.duplicate("Z", 25))
    end
  end
end
