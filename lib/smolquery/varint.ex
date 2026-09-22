defmodule Smolquery.Varint do
  @moduledoc """
  Unsigned LEB128 varints: seven bits a byte, least significant group first,
  the high bit set on every byte but the last.

  Three wire formats read them: ClickHouse RowBinary for lengths
  (`Smolquery.RowBinary`), protobuf for integers, keys and lengths
  (`SmolqueryVictoriaMetrics.RemoteWrite`), and snappy's length preamble
  (`Smolquery.Snappy`). Each caps the length differently, ten bytes for a
  64-bit value and five for snappy's 32-bit one, and words its own errors, so
  this module only reads and reports which way a varint went wrong.

  The value is not masked: a ten-byte varint can carry bits past 64, and a
  caller that needs a 64-bit value masks it.
  """

  import Bitwise

  @doc """
  Reads one varint of at most `max_bytes` bytes off the front of `binary`.

  `{:error, :truncated}` when `binary` ends before the varint does, and
  `{:error, :too_long}` when its `max_bytes`-th byte still has the high bit
  set.
  """
  @spec decode(binary(), pos_integer()) ::
          {:ok, non_neg_integer(), binary()} | {:error, :truncated | :too_long}
  def decode(binary, max_bytes \\ 10) when is_binary(binary) and max_bytes > 0,
    do: decode(binary, 0, 0, 7 * (max_bytes - 1))

  defp decode(<<0::1, bits::7, rest::binary>>, shift, acc, _last),
    do: {:ok, acc ||| bits <<< shift, rest}

  defp decode(<<1::1, bits::7, rest::binary>>, shift, acc, last) when shift < last,
    do: decode(rest, shift + 7, acc ||| bits <<< shift, last)

  defp decode(<<>>, _shift, _acc, _last), do: {:error, :truncated}
  defp decode(_binary, _shift, _acc, _last), do: {:error, :too_long}
end
