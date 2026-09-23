defmodule Smolquery.ZstdTest do
  @moduledoc """
  Frames written by OTP's own `:zstd`, whole, cut short, concatenated and
  inflating past the limit, and the body vmagent v1.152.0 sent in its own
  remote write protocol (see `test/support/fixtures/victoriametrics/README.md`).
  """

  use ExUnit.Case, async: true

  alias Smolquery.Zstd

  @fixture Path.expand("../support/fixtures/victoriametrics/write_zstd.bin", __DIR__)
  @mib 1_048_576

  defp compress(data), do: data |> :zstd.compress() |> IO.iodata_to_binary()

  defp compress_unsized(data) do
    {:ok, context} = :zstd.context(:compress)
    head = feed(context, data, [])
    {:done, tail} = :zstd.finish(context, <<>>)
    IO.iodata_to_binary([head, tail])
  end

  defp feed(context, data, acc) do
    case :zstd.stream(context, data) do
      {:continue, output} -> [acc, output]
      {:continue, rest, output} -> feed(context, rest, [acc, output])
    end
  end

  defp skippable(bytes), do: <<0x184D2A53::little-32, byte_size(bytes)::little-32, bytes::binary>>

  test "a frame decodes to what was compressed" do
    data = :binary.copy("metric_name{job=\"x\"} ", 5000) <> :crypto.strong_rand_bytes(4096)
    assert Zstd.decode(compress(data), max_bytes: byte_size(data)) == {:ok, data}
  end

  test "a frame that declares no content size decodes" do
    data = :binary.copy("abcdefgh", 100_000)
    body = compress_unsized(data)
    assert {:ok, %{frameContentSize: :undefined}} = :zstd.get_frame_header(body)
    assert Zstd.decode(body, max_bytes: byte_size(data)) == {:ok, data}
  end

  test "concatenated frames and skippable frames decode in order" do
    body =
      skippable("ignored") <> compress("first ") <> skippable(<<>>) <> compress_unsized("second")

    assert Zstd.decode(body, max_bytes: 100) == {:ok, "first second"}
  end

  test "a body declaring more than max_bytes is refused before it is inflated" do
    body = compress(:binary.copy(<<0>>, 64 * @mib))
    assert byte_size(body) < 4096
    assert Zstd.decode(body, max_bytes: @mib) == {:error, {:too_large, 64 * @mib, @mib}}
  end

  test "the declared sizes of every frame count against max_bytes" do
    frame = compress(:binary.copy("x", 600))
    assert Zstd.decode(frame <> frame, max_bytes: 1000) == {:error, {:too_large, 1200, 1000}}
  end

  test "a frame declaring no size stops inflating once it passes max_bytes" do
    body = compress_unsized(:binary.copy(<<0>>, 64 * @mib))
    assert {:error, {:too_large, seen, @mib}} = Zstd.decode(body, max_bytes: @mib)
    assert seen > @mib and seen < 2 * @mib
  end

  test "a frame cut short is refused wherever it is cut" do
    body = compress(:crypto.strong_rand_bytes(300_000) <> :binary.copy("y", 300_000))

    for cut <- [1, 4, 5, 8, 20, div(byte_size(body), 2), byte_size(body) - 1] do
      assert {:error, {:invalid_zstd, _message}} =
               Zstd.decode(binary_part(body, 0, cut), max_bytes: @mib)
    end
  end

  test "an empty body, bytes that are not zstd, and a reserved bit are refused" do
    assert {:error, {:invalid_zstd, _message}} = Zstd.decode(<<>>, max_bytes: @mib)
    assert {:error, {:invalid_zstd, _message}} = Zstd.decode("not zstd at all", max_bytes: @mib)

    assert {:error, {:invalid_zstd, _message}} =
             Zstd.decode(compress("abc") <> "junk", max_bytes: @mib)

    <<magic::binary-size(4), descriptor, rest::binary>> = compress("abc")
    reserved = magic <> <<Bitwise.bor(descriptor, 0x08)>> <> rest
    assert {:error, {:invalid_zstd, message}} = Zstd.decode(reserved, max_bytes: @mib)
    assert message =~ "reserved"
  end

  test "corrupt block contents are refused through libzstd's own error" do
    body = compress(:binary.copy("abcdefgh", 1000))
    {:ok, %{headerSize: header_size}} = :zstd.get_frame_header(body)
    headers_size = header_size + 3
    <<headers::binary-size(^headers_size), block::binary>> = body
    corrupt = headers <> :binary.copy(<<0xFF>>, byte_size(block))
    assert {:error, {:invalid_zstd, _message}} = Zstd.decode(corrupt, max_bytes: @mib)
  end

  test "vmagent's body decodes to the size its frame declares" do
    body = File.read!(@fixture)
    assert {:ok, %{frameContentSize: declared}} = :zstd.get_frame_header(body)
    assert {:ok, protobuf} = Zstd.decode(body, max_bytes: declared)
    assert byte_size(protobuf) == declared
    assert protobuf =~ "vm_app_version"

    assert Zstd.decode(body, max_bytes: declared - 1) ==
             {:error, {:too_large, declared, declared - 1}}
  end

  test "declared_length/1 sums the frames' content sizes without inflating" do
    body = File.read!(@fixture)
    assert {:ok, %{frameContentSize: declared}} = :zstd.get_frame_header(body)

    assert Zstd.declared_length(body) == {:ok, declared}
    assert Zstd.declared_length(body <> body) == {:ok, 2 * declared}
    assert {:error, {:invalid_zstd, _message}} = Zstd.declared_length("junk")
    assert {:error, {:invalid_zstd, _message}} = Zstd.declared_length(<<>>)
  end
end
