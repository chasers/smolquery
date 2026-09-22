defmodule Smolquery.Snappy do
  @moduledoc """
  A decoder for snappy's raw block format, the compression Prometheus remote
  write 1.0 puts on its body (PL-70).

  Only the block format: a varint preamble declaring the uncompressed length,
  then a run of literal and copy elements. The framed stream format (the
  `sNaPpY` chunk header) is a different container and is refused. There is no
  encoder; nothing here sends snappy.

  Written in Elixir because the format is a page of spec and a NIF or a
  dependency for it is not worth carrying (PL-70 D5). The output is built by
  appending to one binary, which the runtime grows in place, and a copy reads
  its source back out of that binary, so decoding is one pass with no list of
  chunks to join. Measured on this project's dev box (aarch64, OTP 29), a
  1 MiB output decodes in 1.6 ms from 60-byte literals, 2.5 ms from 64-byte
  copies, 12 ms from a run of offset-1 copies and 16 ms from 8-byte copies.

  ## Bounds

  The preamble is read before anything is decoded, so `declared_length/1`
  lets a caller refuse a body by what it says it expands to, and `decode/2`
  does the same with `:max_bytes`. The declared length is then enforced: an
  element that would write past it, or a body that ends short of it, is
  refused. A small body therefore cannot decode into more than it declared.

  ## Errors

  `{:error, {:too_large, declared, max}}` when the declared length is past
  `:max_bytes`, before any element is read. Everything else that is not a
  well-formed block is `{:error, {:invalid_snappy, message}}`: a truncated
  preamble or element, a copy whose offset is zero or reaches before the start
  of the output, and a length that disagrees with the preamble.
  """

  import Bitwise

  alias Smolquery.Varint

  @max_declared 0xFFFF_FFFF

  @type error ::
          {:too_large, non_neg_integer(), non_neg_integer()} | {:invalid_snappy, String.t()}

  @doc """
  Reads the uncompressed length a block declares in its preamble, without
  decoding the block.
  """
  @spec declared_length(binary()) :: {:ok, non_neg_integer()} | {:error, error()}
  def declared_length(block) when is_binary(block) do
    with {:ok, length, _body} <- preamble(block), do: {:ok, length}
  end

  @doc """
  Decodes a snappy block.

  ## Options

    * `:max_bytes` — refuses a block that declares more than this many
      uncompressed bytes, with `{:error, {:too_large, declared, max}}`,
      before decoding it. Unbounded when absent.
  """
  @spec decode(binary(), keyword()) :: {:ok, binary()} | {:error, error()}
  def decode(block, opts \\ []) when is_binary(block) do
    with {:ok, declared, body} <- preamble(block),
         :ok <- within(declared, Keyword.get(opts, :max_bytes)) do
      elements(body, <<>>, 0, declared)
    end
  end

  defp within(_declared, nil), do: :ok
  defp within(declared, max) when declared <= max, do: :ok
  defp within(declared, max), do: {:error, {:too_large, declared, max}}

  defp preamble(block) do
    case Varint.decode(block, 5) do
      {:ok, length, rest} when length <= @max_declared -> {:ok, length, rest}
      {:ok, _length, _rest} -> invalid("the preamble declares a length past 2^32 - 1")
      {:error, :truncated} -> invalid("the body ends inside its length preamble")
      {:error, :too_long} -> invalid("the length preamble is longer than 5 bytes")
    end
  end

  defp elements(<<>>, out, size, declared) when size == declared, do: {:ok, out}

  defp elements(<<>>, _out, size, declared),
    do: invalid("the body decodes to #{size} bytes but declares #{declared}")

  defp elements(<<tag, rest::binary>>, out, size, declared) do
    case element(tag &&& 3, tag >>> 2, rest) do
      {:literal, length, rest} -> literal(rest, length, out, size, declared)
      {:copy, length, offset, rest} -> copy(rest, length, offset, out, size, declared)
      :truncated -> invalid("the body ends inside an element's header at byte #{size}")
    end
  end

  defp element(0, length, rest) when length < 60, do: {:literal, length + 1, rest}
  defp element(0, 60, <<length, rest::binary>>), do: {:literal, length + 1, rest}
  defp element(0, 61, <<length::little-16, rest::binary>>), do: {:literal, length + 1, rest}
  defp element(0, 62, <<length::little-24, rest::binary>>), do: {:literal, length + 1, rest}
  defp element(0, 63, <<length::little-32, rest::binary>>), do: {:literal, length + 1, rest}

  defp element(1, high, <<low, rest::binary>>),
    do: {:copy, 4 + (high &&& 7), (high >>> 3) <<< 8 ||| low, rest}

  defp element(2, length, <<offset::little-16, rest::binary>>),
    do: {:copy, length + 1, offset, rest}

  defp element(3, length, <<offset::little-32, rest::binary>>),
    do: {:copy, length + 1, offset, rest}

  defp element(_kind, _length, _rest), do: :truncated

  defp literal(_rest, length, _out, size, declared) when size + length > declared,
    do: overrun(declared)

  defp literal(rest, length, out, size, declared) do
    case rest do
      <<bytes::binary-size(^length), rest::binary>> ->
        elements(rest, <<out::binary, bytes::binary>>, size + length, declared)

      _short ->
        invalid("a #{length}-byte literal at byte #{size} runs past the end of the body")
    end
  end

  defp copy(_rest, length, _offset, _out, size, declared) when size + length > declared,
    do: overrun(declared)

  defp copy(_rest, _length, 0, _out, size, _declared),
    do: invalid("a copy at byte #{size} has offset 0")

  defp copy(_rest, _length, offset, _out, size, _declared) when offset > size,
    do: invalid("a copy at byte #{size} has offset #{offset}, outside the #{size} bytes written")

  defp copy(rest, length, offset, out, size, declared) do
    elements(
      rest,
      <<out::binary, copied(out, size - offset, offset, length)::binary>>,
      size + length,
      declared
    )
  end

  defp copied(out, start, offset, length) when offset >= length,
    do: binary_part(out, start, length)

  defp copied(out, start, offset, length) do
    repeated = :binary.copy(binary_part(out, start, offset), div(length, offset) + 1)
    binary_part(repeated, 0, length)
  end

  defp overrun(declared), do: invalid("the body decodes past the #{declared} bytes it declares")

  defp invalid(message), do: {:error, {:invalid_snappy, message}}
end
