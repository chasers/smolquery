defmodule SmolqueryVictoriaMetrics.Eval.XXHash do
  @moduledoc """
  XXH64 with seed `0`, the hash of `github.com/cespare/xxhash/v2`, which
  VictoriaMetrics v1.152.0's `limitk` sorts series by so that it keeps the
  same series from one call to the next (PL-70, T-565). Pure Elixir: one
  hash per series of a `limitk` group.
  """

  import Bitwise

  @mask 0xFFFF_FFFF_FFFF_FFFF
  @p1 11_400_714_785_074_694_791
  @p2 14_029_467_366_897_019_727
  @p3 1_609_587_929_392_839_161
  @p4 9_650_029_242_287_828_579
  @p5 2_870_177_450_012_600_261

  @doc "The 64-bit hash of `data`."
  @spec hash(binary()) :: non_neg_integer()
  def hash(data) when is_binary(data) do
    size = byte_size(data)

    {acc, rest} =
      if size >= 32,
        do: stripes(data, {add(@p1, @p2), @p2, 0, band(-@p1, @mask)}),
        else: {@p5, data}

    acc
    |> add(size)
    |> tail(rest)
    |> avalanche()
  end

  defp stripes(
         <<a::little-64, b::little-64, c::little-64, d::little-64, rest::binary>>,
         {v1, v2, v3, v4}
       ),
       do:
         stripes(
           rest,
           {lane_round(v1, a), lane_round(v2, b), lane_round(v3, c), lane_round(v4, d)}
         )

  defp stripes(rest, {v1, v2, v3, v4}) do
    acc = add(add(rotl(v1, 1), rotl(v2, 7)), add(rotl(v3, 12), rotl(v4, 18)))
    acc = acc |> merge(v1) |> merge(v2) |> merge(v3) |> merge(v4)
    {acc, rest}
  end

  defp tail(acc, <<lane::little-64, rest::binary>>) do
    acc = bxor(acc, lane_round(0, lane))
    tail(add(mul(rotl(acc, 27), @p1), @p4), rest)
  end

  defp tail(acc, <<lane::little-32, rest::binary>>) do
    acc = bxor(acc, mul(lane, @p1))
    tail(add(mul(rotl(acc, 23), @p2), @p3), rest)
  end

  defp tail(acc, <<byte, rest::binary>>) do
    acc = bxor(acc, mul(byte, @p5))
    tail(mul(rotl(acc, 11), @p1), rest)
  end

  defp tail(acc, <<>>), do: acc

  defp avalanche(acc) do
    acc = mul(bxor(acc, acc >>> 33), @p2)
    acc = mul(bxor(acc, acc >>> 29), @p3)
    bxor(acc, acc >>> 32)
  end

  defp lane_round(acc, lane), do: mul(rotl(add(acc, mul(lane, @p2)), 31), @p1)
  defp merge(acc, value), do: add(mul(bxor(acc, lane_round(0, value)), @p1), @p4)

  defp rotl(x, r), do: band(bor(x <<< r, x >>> (64 - r)), @mask)
  defp add(a, b), do: band(a + b, @mask)
  defp mul(a, b), do: band(a * b, @mask)
end
