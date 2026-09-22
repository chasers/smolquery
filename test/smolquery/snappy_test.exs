defmodule Smolquery.SnappyTest do
  @moduledoc """
  Hand-encoded blocks for every element form and every refusal, a round trip
  through a literal-only reference encoder (valid snappy, if not a compact
  one), and the block vmagent v1.152.0 sent under `-remoteWrite.forcePromProto`
  (see `test/support/fixtures/victoriametrics/README.md`).
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Smolquery.Snappy

  @fixture Path.expand("../support/fixtures/victoriametrics/write_snappy.bin", __DIR__)

  defp varint(n) when n < 0x80, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 0x7F::7, varint(n >>> 7)::binary>>

  defp literal(bytes) do
    n = byte_size(bytes) - 1

    tag =
      cond do
        n < 60 -> <<n <<< 2>>
        n < 0x100 -> <<60 <<< 2, n>>
        n < 0x10000 -> <<61 <<< 2, n::little-16>>
        n < 0x1000000 -> <<62 <<< 2, n::little-24>>
        true -> <<63 <<< 2, n::little-32>>
      end

    tag <> bytes
  end

  defp encode(data, chunk) do
    literals = data |> pieces(chunk) |> Enum.map_join(&literal/1)
    varint(byte_size(data)) <> literals
  end

  defp pieces(<<>>, _chunk), do: []
  defp pieces(data, chunk) when byte_size(data) <= chunk, do: [data]

  defp pieces(data, chunk),
    do: [
      binary_part(data, 0, chunk)
      | pieces(binary_part(data, chunk, byte_size(data) - chunk), chunk)
    ]

  defp copy1(length, offset),
    do: <<(offset >>> 8) <<< 5 ||| (length - 4) <<< 2 ||| 1, offset &&& 0xFF>>

  defp copy2(length, offset), do: <<(length - 1) <<< 2 ||| 2, offset::little-16>>
  defp copy4(length, offset), do: <<(length - 1) <<< 2 ||| 3, offset::little-32>>

  describe "declared_length/1" do
    test "reads the preamble varint without decoding the body" do
      assert Snappy.declared_length(<<0>>) == {:ok, 0}
      assert Snappy.declared_length(<<0xFE, 0xFF, 0x7F, 0xFF, 0xFF>>) == {:ok, 2_097_150}
      assert Snappy.declared_length(<<0xFF, 0xFF, 0xFF, 0xFF, 0x0F>>) == {:ok, 0xFFFF_FFFF}
    end

    test "refuses a truncated, overlong or out-of-range preamble" do
      assert {:error, {:invalid_snappy, _message}} = Snappy.declared_length(<<>>)
      assert {:error, {:invalid_snappy, _message}} = Snappy.declared_length(<<0x80>>)

      assert {:error, {:invalid_snappy, _message}} =
               Snappy.declared_length(<<0x80, 0x80, 0x80, 0x80, 0x80, 0>>)

      assert {:error, {:invalid_snappy, _message}} =
               Snappy.declared_length(<<0xFF, 0xFF, 0xFF, 0xFF, 0x1F>>)
    end
  end

  describe "decode/2 literals" do
    test "an empty block is an empty binary" do
      assert Snappy.decode(<<0>>) == {:ok, ""}
    end

    test "a literal whose length is in the tag" do
      assert Snappy.decode(<<5, 4 <<< 2, "hello">>) == {:ok, "hello"}

      assert Snappy.decode(<<60, 59 <<< 2>> <> :binary.copy("x", 60)) ==
               {:ok, :binary.copy("x", 60)}
    end

    test "literals with 1-, 2-, 3- and 4-byte lengths" do
      for {length, header} <- [
            {61, <<60 <<< 2, 60>>},
            {256, <<60 <<< 2, 255>>},
            {300, <<61 <<< 2, 299::little-16>>},
            {70_000, <<62 <<< 2, 69_999::little-24>>},
            {70_000, <<63 <<< 2, 69_999::little-32>>}
          ] do
        bytes = :crypto.strong_rand_bytes(length)
        assert Snappy.decode(varint(length) <> header <> bytes) == {:ok, bytes}
      end
    end
  end

  describe "decode/2 copies" do
    test "a 1-byte-offset copy at both ends of its length range" do
      assert Snappy.decode(<<8, 3 <<< 2, "abcd">> <> copy1(4, 4)) == {:ok, "abcdabcd"}

      prefix = :crypto.strong_rand_bytes(300)
      block = varint(311) <> literal(prefix) <> copy1(11, 260)
      assert Snappy.decode(block) == {:ok, prefix <> binary_part(prefix, 40, 11)}
    end

    test "2-byte and 4-byte offset copies" do
      assert Snappy.decode(<<16, 7 <<< 2, "abcdefgh">> <> copy2(8, 8)) ==
               {:ok, "abcdefghabcdefgh"}

      assert Snappy.decode(<<14, 7 <<< 2, "abcdefgh">> <> copy4(6, 7)) == {:ok, "abcdefghbcdefg"}

      prefix = :crypto.strong_rand_bytes(70_000)
      block = varint(70_064) <> literal(prefix) <> copy4(64, 69_000)
      assert Snappy.decode(block) == {:ok, prefix <> binary_part(prefix, 1000, 64)}
    end

    test "an overlapping copy repeats the pattern it starts on" do
      assert Snappy.decode(<<9, 1 <<< 2, "ab">> <> copy2(7, 2)) == {:ok, "ababababa"}
      assert Snappy.decode(<<6, 0, "x">> <> copy1(5, 1)) == {:ok, "xxxxxx"}
      assert Snappy.decode(<<65, 0, "z">> <> copy2(64, 1)) == {:ok, :binary.copy("z", 65)}
    end
  end

  describe "decode/2 refusals" do
    test "a copy with offset zero" do
      assert {:error, {:invalid_snappy, message}} =
               Snappy.decode(<<6, 1 <<< 2, "ab">> <> copy2(4, 0))

      assert message =~ "offset 0"
    end

    test "a copy reaching before the start of the output" do
      assert {:error, {:invalid_snappy, message}} =
               Snappy.decode(<<6, 1 <<< 2, "ab">> <> copy2(4, 3))

      assert message =~ "offset 3"
      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<4>> <> copy1(4, 1))
    end

    test "a truncated literal or element header" do
      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<5, 4 <<< 2, "hell">>)
      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<100, 60 <<< 2>>)

      assert {:error, {:invalid_snappy, _message}} =
               Snappy.decode(<<6, 1 <<< 2, "ab", 3 <<< 2 ||| 2, 2>>)

      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<6, 1 <<< 2, "ab", 1>>)
    end

    test "a body shorter or longer than its preamble" do
      assert {:error, {:invalid_snappy, message}} = Snappy.decode(<<10, 4 <<< 2, "hello">>)
      assert message =~ "5 bytes but declares 10"
      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<3, 4 <<< 2, "hello">>)
      assert {:error, {:invalid_snappy, _message}} = Snappy.decode(<<3, 0, "a">> <> copy2(8, 1))
    end

    test ":max_bytes refuses by the declared length before decoding" do
      assert Snappy.decode(<<0xFF, 0xFF, 0x03, 0xFF>>, max_bytes: 1024) ==
               {:error, {:too_large, 65_535, 1024}}

      assert Snappy.decode(<<5, 4 <<< 2, "hello">>, max_bytes: 5) == {:ok, "hello"}
      assert Snappy.decode(<<5, 4 <<< 2, "hello">>, max_bytes: 4) == {:error, {:too_large, 5, 4}}
    end
  end

  describe "round trip through a literal-only encoder" do
    test "every size and literal width decodes to its input" do
      cases =
        for size <- [0, 1, 59, 60, 61, 255, 256, 257, 65_536, 65_537],
            chunk <- [1, 60, 256, 65_536] do
          {size, chunk}
        end

      for {size, chunk} <- [{16_777_217, 16_777_216} | cases] do
        data = :crypto.strong_rand_bytes(size)
        assert Snappy.decode(encode(data, chunk)) == {:ok, data}
      end
    end
  end

  describe "vmagent's Prometheus remote write body" do
    test "decodes to the WriteRequest it declares" do
      block = File.read!(@fixture)
      assert {:ok, declared} = Snappy.declared_length(block)
      assert {:ok, protobuf} = Snappy.decode(block, max_bytes: declared)
      assert byte_size(protobuf) == declared
      assert declared > byte_size(block)
      assert <<1::5, 2::3, _rest::binary>> = protobuf
      assert protobuf =~ "vm_app_version"
      assert protobuf =~ "__name__"
    end
  end
end
