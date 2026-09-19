defmodule Smolquery.RowBinary do
  @moduledoc """
  ClickHouse `RowBinary` bodies, decoded against a table schema into the
  NDJSON the flush already reads (PL-64).

  DuckDB has no RowBinary reader, so these bytes cannot pass through to the
  flush the way an NDJSON body does. This module transcodes them in one pass
  over the body: each value is read off the binary and written straight out as
  JSON text, so no row ever becomes a map and the flush's `read_json` sees the
  same bytes an NDJSON client would have sent.

  ## Formats

    * `:with_names_and_types` — `RowBinaryWithNamesAndTypes`: a column count,
      the names, then the ClickHouse type of each. Names bind to the schema's
      columns and every type is checked against its column before a row is
      read, so a body that cannot be stored fails at its header.
    * `:with_names` — `RowBinaryWithNames`: names only. Each column is read as
      the type the schema implies (below).
    * `:row_binary` — `RowBinary`: no header. The columns are the schema's
      regular fields, in order, each read as the type the schema implies.

  The untyped variants read a column as the widest ClickHouse type that holds
  its values, wrapped in `Nullable` when the column is nullable: `Int64`,
  `Float64`, `String`, `Bool`, `DateTime64(6)`, `DateTime64(9)` for a `TIMESTAMP_NS`
  column, `Date32`, `Decimal(P, S)`, and
  `String` for a variant. A map is read as `Map(String, String)` whether or not
  its column is nullable, because ClickHouse cannot declare a `Nullable(Map)`.
  A producer whose column types differ must send the typed variant: RowBinary
  carries no framing, so a column read at the wrong width misreads every byte
  after it.

  ## Types

  | ClickHouse | column |
  |---|---|
  | `Int8`..`Int64`, `UInt8`..`UInt64` | `INT64`; a `UInt64` past `2^63-1` is refused |
  | `Float32`, `Float64` | `FLOAT64`; NaN and the infinities are written as DuckDB's strings for them |
  | `String`, `FixedString(N)` | `STRING`; must be UTF-8, and a `FixedString`'s trailing NULs are dropped |
  | `String` | `VARIANT`; must hold JSON text |
  | `UUID` | `STRING`, as the 36-character lowercase text ClickHouse prints |
  | `Bool` | `BOOL` |
  | `DateTime`, `DateTime64(P)` | `TIMESTAMP`, at microseconds; the zone argument is ignored, the value is UTC |
  | `DateTime`, `DateTime64(P)` | `TIMESTAMP_NS`, every digit, from 1677-09-22 to 2262-04-11 23:47:16.854775806; outside that the row is refused |
  | `Date`, `Date32` | `DATE` |
  | `Decimal(P, S)`, `Decimal32/64/128(S)` | `NUMERIC(_, S)`, scale equal; a value past the column's precision is refused |
  | `Map(K, V)` with string keys and string or `Nullable` string values | `MAP(STRING, STRING)` |
  | `Nullable(T)`, `LowCardinality(T)` | as `T`; RowBinary writes `LowCardinality(T)` as `T` |

  Anything else — `Array`, `Tuple`, `Enum8`, `Int128`, `Decimal256`,
  `JSON`, `Dynamic` — is refused at the header, naming the column.

  ## Errors

  A header that names an unknown or materialized column, omits a column that
  must not be null, or declares a type the column cannot take fails the
  request: `{:error, {:invalid_rowbinary, message}}`, with every problem found
  in the message. So does a body that ends mid-value or carries a marker byte
  RowBinary never writes. RowBinary is positional, so nothing after a bad byte
  can be read, and there are no rows to salvage.

  A value that decodes cleanly but that its column cannot hold — a null in a
  column that must not be null, an out-of-range integer, text that is not
  UTF-8 — refuses only its row. The row is left out of the NDJSON and reported
  at its index in the body, in the shape `Smolquery.IngestService.Validator`
  reports rejections.
  """

  import Bitwise

  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @type format :: :row_binary | :with_names | :with_names_and_types

  @typedoc """
  A ClickHouse type as this module reads it off the wire.
  """
  @type wire ::
          {:int, 8 | 16 | 32 | 64, :signed | :unsigned}
          | {:float, 32 | 64}
          | :string
          | {:fixed_string, pos_integer()}
          | :bool
          | :datetime
          | {:datetime64, 0..9}
          | :date
          | :date32
          | :uuid
          | {:decimal, 1..38, non_neg_integer()}
          | {:map, wire(), wire()}
          | {:nullable, wire()}

  @type row_errors :: %{index: non_neg_integer(), errors: [%{message: String.t()}]}

  @type result :: %{ndjson: iodata(), row_count: non_neg_integer(), errors: [row_errors()]}

  @int64_max 9_223_372_036_854_775_807
  @epoch_gregorian_days 719_528
  @min_micros -62_135_596_800_000_000
  @max_micros 253_402_300_799_999_999
  @min_nanos -9_223_286_400_000_000_000
  @max_nanos 9_223_372_036_854_775_806
  @min_days -719_162
  @max_days 2_932_896
  @two_digits List.to_tuple(
                for n <- 0..99, do: n |> Integer.to_string() |> String.pad_leading(2, "0")
              )

  @nullary %{
    "Int8" => {:int, 8, :signed},
    "Int16" => {:int, 16, :signed},
    "Int32" => {:int, 32, :signed},
    "Int64" => {:int, 64, :signed},
    "UInt8" => {:int, 8, :unsigned},
    "UInt16" => {:int, 16, :unsigned},
    "UInt32" => {:int, 32, :unsigned},
    "UInt64" => {:int, 64, :unsigned},
    "Float32" => {:float, 32},
    "Float64" => {:float, 64},
    "String" => :string,
    "Bool" => :bool,
    "DateTime" => :datetime,
    "Date" => :date,
    "Date32" => :date32,
    "UUID" => :uuid
  }

  @doc """
  Decodes a RowBinary `body` in `format` against `schema`.

  `{:ok, result}` carries the NDJSON of every row the schema takes, how many
  that is, and each refused row's index and reasons. An empty body is zero
  rows in every format.

  ## Options

    * `:columns` — for `:row_binary`, the names of the body's columns in order,
      as an `INSERT` statement lists them; each is read as the type its schema
      column implies, and a column left out is null. Without it, the schema's
      regular columns in order. The header formats name their own columns and
      ignore it.
    * `:max_bytes` — stops decoding once the NDJSON written and the refusals
      reported pass this many bytes, with
      `{:error, {:decoded_too_large, bytes, limit}}`, where `bytes` is how far
      it got. A RowBinary body can decode to many times its size — a row of
      one `Int8` is a byte on the wire and a JSON object in the NDJSON — so the
      limit is enforced while decoding, not after.
  """
  @spec decode(Schema.t(), binary(), format(), keyword()) ::
          {:ok, result()}
          | {:error, {:invalid_rowbinary, String.t()}}
          | {:error, {:decoded_too_large, non_neg_integer(), pos_integer()}}
  def decode(schema, body, format, opts \\ [])

  def decode(%Schema{}, <<>>, _format, _opts), do: {:ok, %{ndjson: [], row_count: 0, errors: []}}

  def decode(%Schema{} = schema, body, format, opts) when is_binary(body) do
    case header(schema, body, format, Keyword.get(opts, :columns)) do
      {:ok, [], <<_byte, _rest::binary>>} ->
        invalid("the header names no columns, but bytes follow it")

      {:ok, columns, rows} ->
        rows(columns, rows, {0, 0, Keyword.get(opts, :max_bytes)}, [], 0, [])

      {:malformed, detail} ->
        invalid("header: " <> detail)

      {:error, message} ->
        invalid(message)
    end
  end

  @doc """
  Parses a ClickHouse type name as a `RowBinaryWithNamesAndTypes` header
  spells it, such as `"LowCardinality(Nullable(String))"` or
  `"DateTime64(9, 'UTC')"`.

  `:error` for text that does not parse and for a type this module cannot
  read. `LowCardinality` is unwrapped, since RowBinary writes its values as
  the inner type.
  """
  @spec parse_type(String.t()) :: {:ok, wire()} | :error
  def parse_type(text) when is_binary(text) do
    case type_node(text) do
      {:ok, node, rest} -> if String.trim(rest) == "", do: wire(node), else: :error
      :error -> :error
    end
  end

  defp invalid(message), do: {:error, {:invalid_rowbinary, message}}

  defp header(schema, body, :row_binary, nil),
    do: {:ok, columns(implied(Schema.regular_fields(schema))), body}

  defp header(schema, body, :row_binary, names) when is_list(names) do
    with {:ok, fields} <- named_fields(schema, names), do: {:ok, columns(implied(fields)), body}
  end

  defp header(schema, body, format, _names) when format in [:with_names, :with_names_and_types] do
    with {:ok, count, rest} <- varint(body),
         {:ok, names, rest} <- strings(count, rest, []),
         {:ok, fields} <- named_fields(schema, names),
         {:ok, pairs, rest} <- wires(format, fields, count, rest) do
      {:ok, columns(pairs), rest}
    end
  end

  defp wires(:with_names, fields, _count, rest), do: {:ok, implied(fields), rest}

  defp wires(:with_names_and_types, fields, count, body) do
    with {:ok, types, rest} <- strings(count, body, []),
         {:ok, pairs} <- typed(fields, types) do
      {:ok, pairs, rest}
    end
  end

  defp implied(fields), do: Enum.map(fields, &{&1, schema_wire(&1)})

  defp strings(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp strings(count, body, acc) do
    with {:ok, bytes, rest} <- string(body), do: strings(count - 1, rest, [bytes | acc])
  end

  defp named_fields(schema, names) do
    problems =
      duplicated(names) ++
        Enum.flat_map(names, &name_problem(schema, &1)) ++ missing(schema, names)

    case problems do
      [] -> {:ok, Enum.map(names, &elem(Schema.field(schema, &1), 1))}
      problems -> {:error, Enum.join(problems, "; ")}
    end
  end

  defp duplicated(names) do
    for {name, count} <- Enum.sort(Enum.frequencies(names)),
        count > 1,
        do: "column #{name} appears more than once"
  end

  defp name_problem(schema, name) do
    case Schema.field(schema, name) do
      :error -> ["unknown column: #{inspect(name)}"]
      {:ok, %Field{materialized: nil}} -> []
      {:ok, %Field{}} -> ["column #{name} is materialized; it takes no value"]
    end
  end

  defp missing(schema, names) do
    given = MapSet.new(names)

    for %Field{nullable: false, name: name} <- Schema.regular_fields(schema),
        not MapSet.member?(given, name),
        do: "column #{name} must not be null, and the header does not name it"
  end

  defp typed(fields, types) do
    bound = Enum.zip_with(fields, types, &bind/2)

    case for({:error, message} <- bound, do: message) do
      [] -> {:ok, Enum.zip(fields, for({:ok, wire} <- bound, do: wire))}
      problems -> {:error, Enum.join(problems, "; ")}
    end
  end

  defp bind(%Field{} = field, text) do
    case parse_type(text) do
      {:ok, wire} ->
        if storable?(wire, field.type), do: {:ok, wire}, else: mismatch(field, text)

      :error ->
        {:error, "column #{field.name} has ClickHouse type #{text}, which smolquery cannot store"}
    end
  end

  defp mismatch(%Field{} = field, text) do
    {:ok, api} = Schema.api_type(field.type)

    {:error, "column #{field.name} is #{api}; ClickHouse type #{text} cannot be written to it"}
  end

  defp storable?({:nullable, inner}, type), do: storable?(inner, type)
  defp storable?({:int, _bits, _signedness}, :int64), do: true
  defp storable?({:float, _bits}, :float64), do: true
  defp storable?(:string, type) when type in [:string, :variant], do: true
  defp storable?({:fixed_string, _size}, :string), do: true
  defp storable?(:uuid, :string), do: true
  defp storable?(:bool, :bool), do: true
  defp storable?(:datetime, :timestamp), do: true
  defp storable?({:datetime64, _precision}, :timestamp), do: true
  defp storable?(:datetime, :timestamp_ns), do: true
  defp storable?({:datetime64, _precision}, :timestamp_ns), do: true
  defp storable?(date, :date) when date in [:date, :date32], do: true
  defp storable?({:decimal, _precision, scale}, {:numeric, _target, scale}), do: true

  defp storable?({:map, key, entry}, {:map, :string, :string}),
    do: map_key?(key) and storable?(entry, :string)

  defp storable?(_wire, _type), do: false

  defp map_key?({:nullable, _inner}), do: false
  defp map_key?(key), do: storable?(key, :string)

  defp schema_wire(%Field{type: {:map, :string, :string}}), do: {:map, :string, :string}
  defp schema_wire(%Field{type: type, nullable: true}), do: {:nullable, base_wire(type)}
  defp schema_wire(%Field{type: type}), do: base_wire(type)

  defp base_wire(:int64), do: {:int, 64, :signed}
  defp base_wire(:float64), do: {:float, 64}
  defp base_wire(:bool), do: :bool
  defp base_wire(:timestamp), do: {:datetime64, 6}
  defp base_wire(:timestamp_ns), do: {:datetime64, 9}
  defp base_wire(:date), do: :date32
  defp base_wire({:numeric, precision, scale}), do: {:decimal, precision, scale}
  defp base_wire(text) when text in [:string, :variant], do: :string

  defp columns(pairs) do
    pairs
    |> Enum.with_index()
    |> Enum.map(fn {{%Field{} = field, wire}, index} -> {key(field.name, index), field, wire} end)
  end

  defp key(name, 0), do: IO.iodata_to_binary([JSON.encode!(name), ?:])
  defp key(name, _index), do: IO.iodata_to_binary([?,, JSON.encode!(name), ?:])

  defp rows(_columns, _body, {_index, bytes, limit}, _lines, _count, _errors)
       when is_integer(limit) and bytes > limit,
       do: {:error, {:decoded_too_large, bytes, limit}}

  defp rows(_columns, <<>>, _progress, lines, count, errors),
    do: {:ok, %{ndjson: Enum.reverse(lines), row_count: count, errors: Enum.reverse(errors)}}

  defp rows(columns, body, {index, bytes, limit}, lines, count, errors) do
    case row(columns, body, [], []) do
      {:ok, line, rest} ->
        progress = {index + 1, bytes + IO.iodata_length(line), limit}
        rows(columns, rest, progress, [line | lines], count + 1, errors)

      {:refused, problems, rest} ->
        refusal = %{index: index, errors: problems}
        reported = Enum.reduce(problems, 0, &(byte_size(&1.message) + &2))

        rows(columns, rest, {index + 1, bytes + reported, limit}, lines, count, [refusal | errors])

      {:malformed, column, detail} ->
        invalid("row #{index}, column #{column}: #{detail}")
    end
  end

  defp row([], rest, parts, []), do: {:ok, [?{ | Enum.reverse(parts, ["}\n"])], rest}
  defp row([], rest, _parts, problems), do: {:refused, Enum.reverse(problems), rest}

  defp row([{key, %Field{} = field, wire} | columns], body, parts, problems) do
    case {value(wire, field.type, body), field.nullable} do
      {{:ok, json, rest}, _nullable} ->
        row(columns, rest, [[key, json] | parts], problems)

      {{:null, rest}, true} ->
        row(columns, rest, [[key, "null"] | parts], problems)

      {{:null, rest}, false} ->
        row(columns, rest, parts, [problem(field, :null) | problems])

      {{:refused, shown, rest}, _nullable} ->
        row(columns, rest, parts, [problem(field, shown) | problems])

      {{:malformed, detail}, _nullable} ->
        {:malformed, field.name, detail}
    end
  end

  defp problem(%Field{name: name}, :null), do: %{message: "column #{name} must not be null"}

  defp problem(%Field{name: name, type: type}, shown) do
    {:ok, api} = Schema.api_type(type)

    %{message: "column #{name} (#{api}) cannot accept #{shown}"}
  end

  defp value({:nullable, _inner}, _type, <<1, rest::binary>>), do: {:null, rest}
  defp value({:nullable, inner}, type, <<0, rest::binary>>), do: value(inner, type, rest)

  defp value({:nullable, _inner}, _type, <<marker, _rest::binary>>),
    do: {:malformed, "a Nullable marker is #{marker}, not 0 or 1"}

  defp value({:int, bits, signedness}, _type, body), do: integer(bits, signedness, body)
  defp value({:float, bits}, _type, body), do: float(bits, body)

  defp value(:string, type, body) do
    with {:ok, bytes, rest} <- string(body), do: text(type, bytes, rest)
  end

  defp value({:fixed_string, size}, type, body), do: fixed_string(size, type, body)
  defp value(:bool, _type, <<0, rest::binary>>), do: {:ok, "false", rest}
  defp value(:bool, _type, <<1, rest::binary>>), do: {:ok, "true", rest}

  defp value(:bool, _type, <<byte, _rest::binary>>),
    do: {:malformed, "a Bool is #{byte}, not 0 or 1"}

  defp value(:datetime, :timestamp_ns, <<seconds::little-unsigned-32, rest::binary>>),
    do: {:ok, timestamp_ns(seconds * 1_000_000_000), rest}

  defp value({:datetime64, precision}, :timestamp_ns, <<ticks::little-signed-64, rest::binary>>),
    do: nanos(ticks * Integer.pow(10, 9 - precision), ticks, rest)

  defp value(:datetime, _type, <<seconds::little-unsigned-32, rest::binary>>),
    do: {:ok, timestamp(seconds * 1_000_000), rest}

  defp value({:datetime64, precision}, _type, <<ticks::little-signed-64, rest::binary>>),
    do: micros(rescale(ticks, precision), ticks, rest)

  defp value(:date, _type, <<days::little-unsigned-16, rest::binary>>),
    do: {:ok, date(days), rest}

  defp value(:date32, _type, <<days::little-signed-32, rest::binary>>)
       when days in @min_days..@max_days,
       do: {:ok, date(days), rest}

  defp value(:date32, _type, <<days::little-signed-32, rest::binary>>),
    do: {:refused, "#{days} days from 1970-01-01", rest}

  defp value({:decimal, precision, scale}, {:numeric, target, _scale}, body),
    do: decimal(decimal_bits(precision), scale, target, body)

  defp value(:uuid, _type, <<high::little-unsigned-64, low::little-unsigned-64, rest::binary>>),
    do: {:ok, uuid(<<high::64, low::64>>), rest}

  defp value({:map, key, entry}, _type, body) do
    with {:ok, count, rest} <- varint(body), do: entries(key, entry, count, rest, [], nil)
  end

  defp value(_wire, _type, _body), do: truncated()

  defp truncated, do: {:malformed, "the body ends mid-value"}

  defp uuid(
         <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2),
           e::binary-size(6)>>
       ) do
    [?", hex(a), ?-, hex(b), ?-, hex(c), ?-, hex(d), ?-, hex(e), ?"]
  end

  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  defp varint(body), do: varint(body, 0, 0)

  defp varint(<<0::1, byte::7, rest::binary>>, shift, acc),
    do: {:ok, acc ||| byte <<< shift, rest}

  defp varint(<<1::1, byte::7, rest::binary>>, shift, acc) when shift < 63,
    do: varint(rest, shift + 7, acc ||| byte <<< shift)

  defp varint(<<_byte, _rest::binary>>, _shift, _acc),
    do: {:malformed, "a length runs past 10 bytes"}

  defp varint(<<>>, _shift, _acc), do: truncated()

  defp string(body) do
    with {:ok, size, rest} <- varint(body) do
      case rest do
        <<bytes::binary-size(^size), remaining::binary>> -> {:ok, bytes, remaining}
        _short -> truncated()
      end
    end
  end

  defp integer(bits, :signed, body) do
    case body do
      <<n::little-signed-size(^bits), rest::binary>> -> {:ok, Integer.to_string(n), rest}
      _short -> truncated()
    end
  end

  defp integer(bits, :unsigned, body) do
    case body do
      <<n::little-unsigned-size(^bits), rest::binary>> when n > @int64_max ->
        {:refused, Integer.to_string(n), rest}

      <<n::little-unsigned-size(^bits), rest::binary>> ->
        {:ok, Integer.to_string(n), rest}

      _short ->
        truncated()
    end
  end

  defp float(64, <<bits::little-unsigned-64, rest::binary>>),
    do: {:ok, float_json(<<bits::64>>, bits >>> 63, bits &&& 0xFFFFFFFFFFFFF), rest}

  defp float(32, <<bits::little-unsigned-32, rest::binary>>),
    do: {:ok, float_json(<<bits::32>>, bits >>> 31, bits &&& 0x7FFFFF), rest}

  defp float(_bits, _body), do: truncated()

  defp float_json(<<f::float-64>>, _sign, _mantissa), do: Float.to_string(f)
  defp float_json(<<f::float-32>>, _sign, _mantissa), do: Float.to_string(f)
  defp float_json(_special, _sign, mantissa) when mantissa != 0, do: ~s("NaN")
  defp float_json(_special, 0, 0), do: ~s("Infinity")
  defp float_json(_special, 1, 0), do: ~s("-Infinity")

  defp fixed_string(size, type, body) do
    case body do
      <<bytes::binary-size(^size), rest::binary>> ->
        text(type, String.trim_trailing(bytes, <<0>>), rest)

      _short ->
        truncated()
    end
  end

  defp text(:variant, bytes, rest) do
    case JSON.decode(bytes) do
      {:ok, term} -> {:ok, JSON.encode_to_iodata!(term), rest}
      {:error, _reason} -> {:refused, shown(bytes), rest}
    end
  end

  defp text(_string, bytes, rest) do
    if String.valid?(bytes),
      do: {:ok, JSON.encode_to_iodata!(bytes), rest},
      else: {:refused, shown(bytes), rest}
  end

  defp shown(bytes), do: inspect(bytes, limit: 32, printable_limit: 64)

  defp micros(us, _ticks, rest) when us in @min_micros..@max_micros,
    do: {:ok, timestamp(us), rest}

  defp micros(_us, ticks, rest), do: {:refused, Integer.to_string(ticks), rest}

  defp rescale(ticks, precision) when precision <= 6,
    do: ticks * Integer.pow(10, 6 - precision)

  defp rescale(ticks, precision), do: Integer.floor_div(ticks, Integer.pow(10, precision - 6))

  defp timestamp(us) do
    seconds = Integer.floor_div(us, 1_000_000)
    days = Integer.floor_div(seconds, 86_400)
    second_of_day = seconds - days * 86_400
    fraction = us - seconds * 1_000_000

    [
      ?",
      calendar_date(days),
      ?\s,
      two_digits(div(second_of_day, 3600)),
      ?:,
      two_digits(rem(div(second_of_day, 60), 60)),
      ?:,
      two_digits(rem(second_of_day, 60)),
      ?.,
      two_digits(div(fraction, 10_000)),
      two_digits(rem(div(fraction, 100), 100)),
      two_digits(rem(fraction, 100)),
      ?"
    ]
  end

  defp nanos(ns, _ticks, rest) when ns in @min_nanos..@max_nanos,
    do: {:ok, timestamp_ns(ns), rest}

  defp nanos(_ns, ticks, rest), do: {:refused, Integer.to_string(ticks), rest}

  defp timestamp_ns(ns) do
    us = Integer.floor_div(ns, 1_000)
    below = ns - us * 1_000

    List.insert_at(timestamp(us), -2, [two_digits(div(below, 10)), ?0 + rem(below, 10)])
  end

  defp date(days), do: [?", calendar_date(days), ?"]

  defp calendar_date(days) do
    {year, month, day} = :calendar.gregorian_days_to_date(days + @epoch_gregorian_days)

    [
      two_digits(div(year, 100)),
      two_digits(rem(year, 100)),
      ?-,
      two_digits(month),
      ?-,
      two_digits(day)
    ]
  end

  defp two_digits(n), do: elem(@two_digits, n)

  defp decimal_bits(precision) when precision <= 9, do: 32
  defp decimal_bits(precision) when precision <= 18, do: 64
  defp decimal_bits(_precision), do: 128

  defp decimal(bits, scale, target, body) do
    case body do
      <<n::little-signed-size(^bits), rest::binary>> ->
        if abs(n) < Integer.pow(10, target),
          do: {:ok, [?", decimal_text(n, scale), ?"], rest},
          else: {:refused, IO.iodata_to_binary(decimal_text(n, scale)), rest}

      _short ->
        truncated()
    end
  end

  defp decimal_text(n, 0), do: Integer.to_string(n)

  defp decimal_text(n, scale) do
    digits = n |> abs() |> Integer.to_string() |> String.pad_leading(scale + 1, "0")
    {whole, fraction} = String.split_at(digits, -scale)

    [sign(n), whole, ?., fraction]
  end

  defp sign(n) when n < 0, do: ?-
  defp sign(_n), do: []

  defp entries(_key, _entry, 0, rest, pairs, nil),
    do: {:ok, [?{, Enum.intersperse(Enum.reverse(pairs), ?,), ?}], rest}

  defp entries(_key, _entry, 0, rest, _pairs, refused), do: {:refused, refused, rest}

  defp entries(key, entry, count, body, pairs, refused) do
    with {:ok, name, rest, refused} <- entry_part(key, body, refused),
         {:ok, text, rest, refused} <- entry_part(entry, rest, refused) do
      entries(key, entry, count - 1, rest, [[name, ?:, text] | pairs], refused)
    end
  end

  defp entry_part(wire, body, refused) do
    case value(wire, :string, body) do
      {:ok, json, rest} -> {:ok, json, rest, refused}
      {:null, rest} -> {:ok, "null", rest, refused}
      {:refused, shown, rest} -> {:ok, [], rest, refused || shown}
      {:malformed, _detail} = malformed -> malformed
    end
  end

  defp type_node(text) do
    case String.trim_leading(text) do
      <<?', rest::binary>> ->
        quoted(rest, <<>>)

      <<digit, _rest::binary>> = number when digit in ?0..?9 ->
        {n, rest} = Integer.parse(number)
        {:ok, n, rest}

      named ->
        named(named)
    end
  end

  defp quoted(<<?\\, char, rest::binary>>, acc), do: quoted(rest, <<acc::binary, char>>)
  defp quoted(<<?', rest::binary>>, acc), do: {:ok, {:quoted, acc}, rest}
  defp quoted(<<char, rest::binary>>, acc), do: quoted(rest, <<acc::binary, char>>)
  defp quoted(<<>>, _acc), do: :error

  defp named(text) do
    case identifier(text, <<>>) do
      {<<>>, _rest} -> :error
      {name, rest} -> arguments_of(name, String.trim_leading(rest))
    end
  end

  defp identifier(<<?_, rest::binary>>, acc), do: identifier(rest, <<acc::binary, ?_>>)

  defp identifier(<<char, rest::binary>>, acc)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9,
       do: identifier(rest, <<acc::binary, char>>)

  defp identifier(rest, acc), do: {acc, rest}

  defp arguments_of(name, "(" <> rest) do
    with {:ok, arguments, remaining} <- arguments(rest, []),
         do: {:ok, {name, arguments}, remaining}
  end

  defp arguments_of(name, rest), do: {:ok, {name, []}, rest}

  defp arguments(text, acc) do
    with {:ok, argument, rest} <- type_node(text) do
      case String.trim_leading(rest) do
        "," <> remaining -> arguments(remaining, [argument | acc])
        ")" <> remaining -> {:ok, Enum.reverse([argument | acc]), remaining}
        _unclosed -> :error
      end
    end
  end

  defp wire({name, []}), do: Map.fetch(@nullary, name)
  defp wire({"DateTime", [{:quoted, _zone}]}), do: {:ok, :datetime}

  defp wire({"DateTime64", [precision]}) when precision in 0..9,
    do: {:ok, {:datetime64, precision}}

  defp wire({"DateTime64", [precision, {:quoted, _zone}]}) when precision in 0..9,
    do: {:ok, {:datetime64, precision}}

  defp wire({"FixedString", [size]}) when is_integer(size) and size > 0,
    do: {:ok, {:fixed_string, size}}

  defp wire({"Decimal", [precision, scale]})
       when precision in 1..38 and is_integer(scale) and scale >= 0 and scale <= precision,
       do: {:ok, {:decimal, precision, scale}}

  defp wire({"Decimal32", [scale]}) when scale in 0..9, do: {:ok, {:decimal, 9, scale}}
  defp wire({"Decimal64", [scale]}) when scale in 0..18, do: {:ok, {:decimal, 18, scale}}
  defp wire({"Decimal128", [scale]}) when scale in 0..38, do: {:ok, {:decimal, 38, scale}}

  defp wire({"Nullable", [inner]}) do
    with {:ok, wire} <- wire(inner), do: {:ok, {:nullable, wire}}
  end

  defp wire({"LowCardinality", [inner]}), do: wire(inner)

  defp wire({"Map", [key, entry]}) do
    with {:ok, key_wire} <- wire(key),
         {:ok, entry_wire} <- wire(entry),
         do: {:ok, {:map, key_wire, entry_wire}}
  end

  defp wire(_node), do: :error
end
