defmodule Smolquery.Zstd do
  @moduledoc """
  Bounded decoding of a zstd body, the compression vmagent's own remote write
  protocol puts on its body (PL-70).

  OTP's `:zstd` does the decompression. It is not enough on its own for a body
  from the network, for two reasons found by driving it:

    * `:zstd.decompress/1` inflates the whole body in one call, so a few
      kilobytes can become gigabytes before the caller sees a size.
    * A frame cut short decodes without an error to whatever its complete
      blocks held, so a truncated body would be read as a shorter valid one.

  So the body's frames are walked first, reading only their headers and block
  headers: every frame must be whole, and the content sizes the frames declare
  are summed. A body that declares more than `:max_bytes` is refused there,
  before anything is inflated. The body is then decompressed through a
  streaming context, which hands output back 128 KiB at a time, and decoding
  stops as soon as the output passes `:max_bytes`, which catches a frame that
  declares no size. The context's window is capped at the power of two that
  covers `:max_bytes`, within the 8 MiB every zstd decoder is expected to
  accept and libzstd's own 128 MiB default, so a frame cannot make the decoder
  allocate a window the body could never need.

  Skippable frames are accepted and produce nothing. Dictionaries are not
  supported: a frame that names one fails to decode.

  ## Errors

  `{:error, {:too_large, bytes, max}}` when the declared or decoded size is
  past `:max_bytes`, where `bytes` is the declared total or how far decoding
  got. Anything that is not whole, well-formed zstd is
  `{:error, {:invalid_zstd, message}}`.
  """

  import Bitwise

  @frame_magic 0xFD2FB528
  @min_window_log 23
  @max_window_log 27

  @type error ::
          {:too_large, non_neg_integer(), non_neg_integer()} | {:invalid_zstd, String.t()}

  @doc """
  Decodes `body`, one or more zstd frames, to at most `max_bytes` bytes.
  """
  @spec decode(binary(), max_bytes: non_neg_integer()) :: {:ok, binary()} | {:error, error()}
  def decode(<<>>, _opts), do: invalid("the body is empty")

  def decode(body, opts) when is_binary(body) do
    max = Keyword.fetch!(opts, :max_bytes)

    with {:ok, declared} <- frames(body, 0),
         :ok <- within(declared, max) do
      inflate(body, max)
    end
  end

  defp within(declared, max) when is_integer(declared) and declared > max,
    do: {:error, {:too_large, declared, max}}

  defp within(_declared, _max), do: :ok

  defp frames(<<>>, declared), do: {:ok, declared}

  defp frames(<<@frame_magic::little-32, descriptor, rest::binary>>, declared) do
    with {:ok, size, rest} <- frame_header(descriptor, rest),
         {:ok, rest} <- blocks(rest),
         {:ok, rest} <- checksum(descriptor, rest) do
      frames(rest, add(declared, size))
    end
  end

  defp frames(<<magic::little-32, length::little-32, rest::binary>>, declared)
       when (magic &&& 0xFFFFFFF0) == 0x184D2A50 do
    case rest do
      <<_skipped::binary-size(^length), rest::binary>> -> frames(rest, declared)
      _short -> invalid("a skippable frame runs past the end of the body")
    end
  end

  defp frames(_body, _declared), do: invalid("the body is not a zstd frame")

  defp add(declared, size) when is_integer(declared) and is_integer(size), do: declared + size
  defp add(_declared, _size), do: :unknown

  defp frame_header(descriptor, _rest) when (descriptor &&& 0x08) != 0,
    do: invalid("a frame header sets its reserved bit")

  defp frame_header(descriptor, rest) do
    single_segment = (descriptor &&& 0x20) != 0
    window_bytes = if single_segment, do: 0, else: 1
    dictionary_bytes = elem({0, 1, 2, 4}, descriptor &&& 0x03)
    size_bytes = content_size_bytes(descriptor >>> 6, single_segment)
    size_bits = size_bytes * 8

    case rest do
      <<_window::binary-size(^window_bytes), _dictionary::binary-size(^dictionary_bytes),
        size::little-size(^size_bits), rest::binary>> ->
        {:ok, content_size(size_bytes, size), rest}

      _short ->
        invalid("the body ends inside a frame header")
    end
  end

  defp content_size_bytes(0, true), do: 1
  defp content_size_bytes(0, false), do: 0
  defp content_size_bytes(flag, _single_segment), do: elem({0, 2, 4, 8}, flag)

  defp content_size(0, _size), do: :unknown
  defp content_size(2, size), do: size + 256
  defp content_size(_bytes, size), do: size

  defp blocks(<<header::little-24, rest::binary>>) do
    with {:ok, rest} <- block(header >>> 1 &&& 0x03, header >>> 3, rest) do
      if (header &&& 0x01) == 1, do: {:ok, rest}, else: blocks(rest)
    end
  end

  defp blocks(_body), do: invalid("the body ends inside a block header")

  defp block(1, _size, <<_byte, rest::binary>>), do: {:ok, rest}

  defp block(type, size, rest) when type in [0, 2] and byte_size(rest) >= size,
    do: {:ok, binary_part(rest, size, byte_size(rest) - size)}

  defp block(3, _size, _rest), do: invalid("a block has the reserved type")
  defp block(_type, _size, _rest), do: invalid("a block runs past the end of the body")

  defp checksum(descriptor, <<_checksum::32, rest::binary>>) when (descriptor &&& 0x04) != 0,
    do: {:ok, rest}

  defp checksum(descriptor, _rest) when (descriptor &&& 0x04) != 0,
    do: invalid("the body ends inside a frame checksum")

  defp checksum(_descriptor, rest), do: {:ok, rest}

  defp inflate(body, max) do
    {:ok, context} = :zstd.context(:decompress, %{windowLogMax: window_log(max)})

    try do
      stream(context, body, [], 0, max)
    catch
      :error, {:zstd_error, reason} -> invalid(to_string(reason))
    after
      :zstd.close(context)
    end
  end

  defp window_log(max) do
    needed = max |> max(1) |> :math.log2() |> Float.ceil() |> trunc()
    needed |> max(@min_window_log) |> min(@max_window_log)
  end

  defp stream(context, input, acc, size, max) do
    case :zstd.stream(context, input) do
      {:continue, rest, output} ->
        more(context, rest, [acc, output], size + byte_size(output), max)

      {:continue, output} ->
        finished([acc, output], size + byte_size(output), max)
    end
  end

  defp more(_context, _rest, _acc, size, max) when size > max,
    do: {:error, {:too_large, size, max}}

  defp more(context, rest, acc, size, max), do: stream(context, rest, acc, size, max)

  defp finished(_acc, size, max) when size > max, do: {:error, {:too_large, size, max}}
  defp finished(acc, _size, _max), do: {:ok, IO.iodata_to_binary(acc)}

  defp invalid(message), do: {:error, {:invalid_zstd, message}}
end
