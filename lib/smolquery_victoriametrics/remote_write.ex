defmodule SmolqueryVictoriaMetrics.RemoteWrite do
  @moduledoc """
  Prometheus remote write bodies, decompressed and decoded to series (PL-70).

  vmagent, Prometheus and the OpenTelemetry collector push samples as a
  protobuf `prometheus.WriteRequest`. This module reads exactly that message,
  by hand, as `Smolquery.RowBinary` reads RowBinary: no protobuf dependency and
  no generated code for four small messages (PL-70 D5).

      WriteRequest { repeated TimeSeries timeseries = 1; repeated MetricMetadata metadata = 3; }
      TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2;
                     repeated Exemplar exemplars = 3; repeated Histogram histograms = 4; }
      Label        { string name = 1; string value = 2; }
      Sample       { double value = 1; int64 timestamp = 2; }

  A series comes out as `%{name: name, labels: labels, samples: samples}`:
  `name` is the `__name__` label, or `nil` when there is none; `labels` is
  every other label as `{name, value}` in the order sent, which is not always
  sorted: vmagent v1.152.0 sends a series' own labels before the `instance`
  and `job` it adds. `samples` is `{timestamp_ms, value}` in the order sent.

  ## What is dropped, and what is changed

  Elixir floats cannot hold NaN or the infinities, and neither can the JSON
  row path the samples are written through (PL-70 D6). So:

    * a NaN sample, which is also Prometheus' staleness marker, is left out of
      its series and counted in `dropped.nan`;
    * `+Inf` and `-Inf` are decoded as `1.7976931348623157e308` and
      `-1.7976931348623157e308`, the largest finite doubles, and stored as
      such.

  Exemplars, native histograms and metric metadata are counted in `dropped`
  and not decoded. A series whose every sample was NaN is kept, with no
  samples.

  Any other field, in any message, is skipped by its wire type, as protobuf
  requires of a reader meeting a field it does not know. A known field that
  arrives with the wrong wire type is skipped the same way, as the Go protobuf
  runtime does. Missing fields take protobuf's defaults: an empty string, a
  zero timestamp, a zero value.

  ## Bodies

  `decode_body/3` takes the body as it came off the wire. `encoding/1` reads
  the `Content-Encoding` header: `snappy` is Prometheus remote write 1.0 and is
  decoded by `Smolquery.Snappy`; `zstd` is vmagent's own protocol and is
  decoded by `Smolquery.Zstd`; no header is a body sent as is. In each case the
  uncompressed size is held to `:max_bytes` before the body is inflated.

  ## Errors

    * `{:too_large, bytes, max}` — the body is, declares, or inflates to more
      than `:max_bytes`.
    * `{:invalid_snappy, message}`, `{:invalid_zstd, message}` — the body is
      not whole, well-formed data in its encoding.
    * `{:invalid_write_request, message}` — the protobuf is malformed: a
      length or value running past its message, a varint longer than ten
      bytes, a group (deprecated, and never in a `WriteRequest`), field
      number zero, or a label name or value that is not UTF-8.

  Every message is a sentence a 400 can carry as it is.
  """

  import Bitwise

  alias Smolquery.Snappy
  alias Smolquery.Varint
  alias Smolquery.Zstd

  @max_double 1.797_693_134_862_315_7e308
  @exponent_mask 0x7FF0_0000_0000_0000
  @mantissa_mask 0x000F_FFFF_FFFF_FFFF
  @uint64_mask 0xFFFF_FFFF_FFFF_FFFF
  @int64_max 0x7FFF_FFFF_FFFF_FFFF

  @type encoding :: :snappy | :zstd | :identity

  @type series :: %{
          name: String.t() | nil,
          labels: [{String.t(), String.t()}],
          samples: [{integer(), float()}]
        }

  @type dropped :: %{
          nan: non_neg_integer(),
          histograms: non_neg_integer(),
          exemplars: non_neg_integer(),
          metadata: non_neg_integer()
        }

  @type decoded :: %{timeseries: [series()], dropped: dropped()}

  @type error ::
          {:too_large, non_neg_integer(), non_neg_integer()}
          | {:invalid_snappy, String.t()}
          | {:invalid_zstd, String.t()}
          | {:invalid_write_request, String.t()}

  @doc """
  Reads a `Content-Encoding` header value as the encoding of a remote write
  body. Case and surrounding whitespace are ignored; `nil`, an empty value and
  `identity` all mean the body is not compressed.
  """
  @spec encoding(String.t() | nil) :: {:ok, encoding()} | {:error, :unsupported_encoding}
  def encoding(nil), do: {:ok, :identity}

  def encoding(header) when is_binary(header) do
    case header |> String.trim() |> String.downcase() do
      "snappy" -> {:ok, :snappy}
      "zstd" -> {:ok, :zstd}
      identity when identity in ["", "identity"] -> {:ok, :identity}
      _other -> {:error, :unsupported_encoding}
    end
  end

  @doc """
  Inflates a body in `encoding`, holding its uncompressed size to
  `:max_bytes`, and decodes the `WriteRequest` inside it.

  ## Options

    * `:max_bytes` — required; the most bytes the uncompressed protobuf may
      be.
  """
  @spec decode_body(binary(), encoding(), max_bytes: non_neg_integer()) ::
          {:ok, decoded()} | {:error, error()}
  def decode_body(body, encoding, opts) when is_binary(body) do
    max = Keyword.fetch!(opts, :max_bytes)

    with {:ok, protobuf} <- inflate(body, encoding, max), do: decode(protobuf)
  end

  defp inflate(body, :snappy, max), do: Snappy.decode(body, max_bytes: max)
  defp inflate(body, :zstd, max), do: Zstd.decode(body, max_bytes: max)

  defp inflate(body, :identity, max) when byte_size(body) > max,
    do: {:error, {:too_large, byte_size(body), max}}

  defp inflate(body, :identity, _max), do: {:ok, body}

  @doc """
  Decodes an uncompressed protobuf `WriteRequest`.
  """
  @spec decode(binary()) :: {:ok, decoded()} | {:error, {:invalid_write_request, String.t()}}
  def decode(protobuf) when is_binary(protobuf) do
    dropped = %{nan: 0, histograms: 0, exemplars: 0, metadata: 0}

    with {:ok, {series, dropped}} <- fields(protobuf, {[], dropped}, &request_field/4) do
      {:ok, %{timeseries: Enum.reverse(series), dropped: dropped}}
    end
  end

  defp request_field(1, 2, message, {series, dropped}) do
    with {:ok, one, dropped} <- series(message, dropped), do: {:ok, {[one | series], dropped}}
  end

  defp request_field(3, 2, _message, {series, dropped}),
    do: {:ok, {series, count(dropped, :metadata)}}

  defp request_field(_field, _wire, _value, acc), do: {:ok, acc}

  defp series(message, dropped) do
    with {:ok, {name, labels, samples, dropped}} <-
           fields(message, {nil, [], [], dropped}, &series_field/4) do
      {:ok, %{name: name, labels: Enum.reverse(labels), samples: Enum.reverse(samples)}, dropped}
    end
  end

  defp series_field(1, 2, message, {name, labels, samples, dropped}) do
    with {:ok, label} <- label(message) do
      case label do
        {"__name__", value} -> {:ok, {value, labels, samples, dropped}}
        label -> {:ok, {name, [label | labels], samples, dropped}}
      end
    end
  end

  defp series_field(2, 2, message, {name, labels, samples, dropped}) do
    with {:ok, sample} <- fields(message, {0, 0.0}, &sample_field/4) do
      case sample do
        {_timestamp, :nan} -> {:ok, {name, labels, samples, count(dropped, :nan)}}
        sample -> {:ok, {name, labels, [sample | samples], dropped}}
      end
    end
  end

  defp series_field(3, 2, _message, {name, labels, samples, dropped}),
    do: {:ok, {name, labels, samples, count(dropped, :exemplars)}}

  defp series_field(4, 2, _message, {name, labels, samples, dropped}),
    do: {:ok, {name, labels, samples, count(dropped, :histograms)}}

  defp series_field(_field, _wire, _value, acc), do: {:ok, acc}

  defp label(message) do
    with {:ok, {name, value}} <- fields(message, {"", ""}, &label_field/4) do
      cond do
        not String.valid?(name) -> invalid("a label name is not valid UTF-8")
        not String.valid?(value) -> invalid("the value of label #{name} is not valid UTF-8")
        true -> {:ok, {name, value}}
      end
    end
  end

  defp label_field(1, 2, name, {_name, value}), do: {:ok, {name, value}}
  defp label_field(2, 2, value, {name, _value}), do: {:ok, {name, value}}
  defp label_field(_field, _wire, _value, acc), do: {:ok, acc}

  defp sample_field(1, 1, <<bits::little-64>>, {timestamp, _value}),
    do: {:ok, {timestamp, double(bits)}}

  defp sample_field(2, 0, varint, {_timestamp, value}), do: {:ok, {int64(varint), value}}
  defp sample_field(_field, _wire, _value, acc), do: {:ok, acc}

  defp double(bits)
       when (bits &&& @exponent_mask) == @exponent_mask and (bits &&& @mantissa_mask) != 0,
       do: :nan

  defp double(bits) when (bits &&& @exponent_mask) == @exponent_mask and bits >>> 63 == 1,
    do: -@max_double

  defp double(bits) when (bits &&& @exponent_mask) == @exponent_mask, do: @max_double

  defp double(bits) do
    <<value::float-64>> = <<bits::64>>
    value
  end

  defp int64(varint) when varint > @int64_max, do: varint - (@uint64_mask + 1)
  defp int64(varint), do: varint

  defp count(dropped, key), do: Map.update!(dropped, key, &(&1 + 1))

  defp fields(<<>>, acc, _field), do: {:ok, acc}

  defp fields(message, acc, field) do
    with {:ok, key, rest} <- varint(message),
         {:ok, number} <- field_number(key >>> 3),
         {:ok, value, rest} <- value(key &&& 7, rest),
         {:ok, acc} <- field.(number, key &&& 7, value, acc) do
      fields(rest, acc, field)
    end
  end

  defp field_number(0), do: invalid("a field has number 0, which protobuf reserves")
  defp field_number(number), do: {:ok, number}

  defp value(0, message), do: varint(message)
  defp value(1, <<bits::binary-size(8), rest::binary>>), do: {:ok, bits, rest}
  defp value(5, <<bits::binary-size(4), rest::binary>>), do: {:ok, bits, rest}

  defp value(2, message) do
    with {:ok, length, rest} <- varint(message) do
      case rest do
        <<bytes::binary-size(^length), rest::binary>> -> {:ok, bytes, rest}
        _short -> invalid("a #{length}-byte field runs past the end of its message")
      end
    end
  end

  defp value(wire, _message) when wire in [3, 4],
    do: invalid("the body holds a group, which a WriteRequest never does")

  defp value(wire, _message) when wire in [1, 5],
    do: invalid("a fixed-width field runs past the end of its message")

  defp value(wire, _message),
    do: invalid("a field has wire type #{wire}, which protobuf does not define")

  defp varint(message) do
    case Varint.decode(message) do
      {:ok, value, rest} -> {:ok, value &&& @uint64_mask, rest}
      {:error, :truncated} -> invalid("the body ends inside a varint")
      {:error, :too_long} -> invalid("a varint is longer than 10 bytes")
    end
  end

  defp invalid(message), do: {:error, {:invalid_write_request, message}}
end
