defmodule Smolquery.VarintTest do
  use ExUnit.Case, async: true

  alias Smolquery.Varint

  test "reads single- and multi-byte varints and returns what follows" do
    assert Varint.decode(<<0, "rest">>) == {:ok, 0, "rest"}
    assert Varint.decode(<<0x7F>>) == {:ok, 127, ""}
    assert Varint.decode(<<0x80, 0x01, 0xFF>>) == {:ok, 128, <<0xFF>>}
    assert Varint.decode(<<0xAC, 0x02>>) == {:ok, 300, ""}
  end

  test "reads a ten-byte varint unmasked" do
    max = :binary.copy(<<0xFF>>, 9) <> <<0x01>>
    assert Varint.decode(max) == {:ok, 0xFFFF_FFFF_FFFF_FFFF, ""}
    assert Varint.decode(:binary.copy(<<0xFF>>, 9) <> <<0x7F>>) == {:ok, 2 ** 70 - 1, ""}
  end

  test "refuses a varint that ends early" do
    assert Varint.decode(<<>>) == {:error, :truncated}
    assert Varint.decode(<<0x80, 0x80>>) == {:error, :truncated}
  end

  test "refuses a varint longer than max_bytes" do
    assert Varint.decode(:binary.copy(<<0x80>>, 10) <> <<0>>) == {:error, :too_long}
    assert Varint.decode(<<0xFF, 0xFF, 0xFF, 0xFF, 0x0F>>, 5) == {:ok, 0xFFFF_FFFF, ""}
    assert Varint.decode(<<0xFF, 0xFF, 0xFF, 0xFF, 0x8F, 0>>, 5) == {:error, :too_long}
    assert Varint.decode(<<0x81, 0>>, 1) == {:error, :too_long}
  end
end
