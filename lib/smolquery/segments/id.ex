defmodule Smolquery.Segments.Id do
  @moduledoc """
  ULIDs for segment filenames.

  A segment's id is its identity everywhere: the filename on disk, the path
  registered in the catalog, and the key a hot manifest entry is retired by.
  ULIDs suit that better than random ids — the 48-bit millisecond prefix makes
  a directory listing chronological, which is what retention, GC, and
  debugging all want, and the 80 random bits keep ids unique across buffer
  nodes without coordination.

  Ids sort chronologically to the millisecond; within a millisecond the random
  tail decides, so ordering is not strictly monotonic.
  """

  import Bitwise

  @alphabet ~c"0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  @symbols List.to_tuple(@alphabet)
  @values @alphabet |> Enum.with_index() |> Map.new()
  @encoded_length 26
  @random_bits 80

  @doc """
  A new ULID, stamped with the current time.
  """
  @spec generate() :: String.t()
  def generate, do: generate(System.system_time(:millisecond))

  @doc """
  A new ULID stamped with `timestamp` in milliseconds since the Unix epoch.
  """
  @spec generate(non_neg_integer()) :: String.t()
  def generate(timestamp) when is_integer(timestamp) and timestamp >= 0 do
    <<value::128>> = <<timestamp::48, :crypto.strong_rand_bytes(10)::binary>>

    encode(value)
  end

  @doc """
  The millisecond timestamp a ULID was stamped with.
  """
  @spec timestamp(term()) :: {:ok, non_neg_integer()} | :error
  def timestamp(id) do
    with {:ok, value} <- decode(id) do
      <<timestamp::48, _random::size(@random_bits)>> = <<value::128>>

      {:ok, timestamp}
    end
  end

  @doc """
  Whether `id` is a well-formed ULID.
  """
  @spec valid?(term()) :: boolean()
  def valid?(id) do
    match?({:ok, _value}, decode(id))
  end

  defp encode(value) do
    {symbols, _remaining} =
      Enum.map_reduce(1..@encoded_length, value, fn _index, remaining ->
        {elem(@symbols, remaining &&& 31), remaining >>> 5}
      end)

    symbols |> Enum.reverse() |> List.to_string()
  end

  defp decode(id) when is_binary(id) and byte_size(id) == @encoded_length do
    decoded =
      Enum.reduce_while(String.to_charlist(id), 0, fn symbol, acc ->
        case Map.fetch(@values, symbol) do
          {:ok, value} -> {:cont, (acc <<< 5) + value}
          :error -> {:halt, :error}
        end
      end)

    case decoded do
      :error -> :error
      value when value < 1 <<< 128 -> {:ok, value}
      _overflow -> :error
    end
  end

  defp decode(_id), do: :error
end
