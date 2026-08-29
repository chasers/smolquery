defmodule SmolqueryPg.Types do
  @moduledoc """
  How a result frame's columns and values cross the Postgres wire (PL-58).

  A `RowDescription` names each column's Postgres type by OID, and a
  `DataRow` carries each value in that type's text form. Both derive from
  the frame's Explorer dtype — the only type information a
  `Smolquery.QueryService` result carries.

  ## The mapping

  | smolquery | Explorer dtype | Postgres | OID |
  |---|---|---|---|
  | `INT64` | `{:s, 64}` | `bigint` | 20 |
  | `FLOAT64` | `{:f, 64}` | `double precision` | 701 |
  | `STRING` | `:string` | `text` | 25 |
  | `BOOL` | `:boolean` | `boolean` | 16 |
  | `TIMESTAMP` | `{:naive_datetime, _}` | `timestamp` | 1114 |
  | `DATE` | `:date` | `date` | 1082 |
  | `NUMERIC(p,s)` | `{:decimal, p, s}` | `numeric(p,s)` | 1700 |
  | `MAP(STRING, STRING)` | list of key/value structs | `jsonb` | 3802 |
  | `VARIANT` | JSON text (`job.json_columns`) | `jsonb` | 3802 |

  Smaller integers arrive as `bigint` — their values are the same terms.
  A `{:datetime, _, _}` answers `timestamptz`, a `{:time, _}` answers
  `time`, a `:binary` answers `bytea`. A list or struct has no Postgres
  scalar; it answers `jsonb`, and a value that JSON cannot carry answers its
  inspected form as `text`.

  ## Text forms

  The text is what Postgres's own input functions read, so a driver or a
  `postgres_fdw` casts it without help: `t`/`f` for a boolean, ISO 8601
  with a space for a timestamp, a plain decimal string for a numeric,
  `NaN`/`Infinity` for a non-finite double.

  ## Binary forms

  A driver that binds through the extended protocol usually asks for
  binary results — Postgrex does on every column. `encode/4` answers the
  Postgres binary form of each type: a big-endian integer, an IEEE double,
  a byte for a boolean, microseconds since 2000-01-01 for a timestamp, days
  since 2000-01-01 for a date, base-10000 digit groups for a numeric, and
  the version byte `1` before the text of a `jsonb`.

  ## Parameters

  `decode_param/3` reads a `Bind` value the other way, by its declared OID
  and format, into a term `SmolqueryPg.Params.literal/1` renders.
  """

  import Bitwise

  @type dtype :: term()

  @type param ::
          nil
          | boolean()
          | integer()
          | float()
          | :nan
          | :infinity
          | :neg_infinity
          | {:text, String.t()}
          | {:numeric, String.t()}
          | {:timestamp, NaiveDateTime.t() | :infinity | :neg_infinity}
          | {:date, Date.t()}
          | {:json, String.t()}
          | {:bytea, binary()}

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}
  @epoch_us 946_684_800_000_000

  @doc """
  The Postgres type of an Explorer `dtype`: `{oid, typlen, typmod}`.

  A `json?` column carries JSON text whatever its dtype says.
  """
  @spec describe(dtype(), boolean()) :: {pos_integer(), integer(), integer()}
  def describe(_dtype, true), do: {3802, -1, -1}
  def describe(dtype, false), do: describe(dtype)

  defp describe({:duckdb, type}), do: describe_duckdb(type)
  defp describe({:pg_array, _inner}), do: {25, -1, -1}
  defp describe(:boolean), do: {16, 1, -1}
  defp describe({:s, _bits}), do: {20, 8, -1}
  defp describe({:u, _bits}), do: {20, 8, -1}
  defp describe({:f, _bits}), do: {701, 8, -1}
  defp describe(:string), do: {25, -1, -1}
  defp describe(:category), do: {25, -1, -1}
  defp describe(:binary), do: {17, -1, -1}
  defp describe(:date), do: {1082, 4, -1}
  defp describe({:time, _precision}), do: {1083, 8, -1}
  defp describe({:naive_datetime, _precision}), do: {1114, 8, -1}
  defp describe({:datetime, _precision, _zone}), do: {1184, 8, -1}
  defp describe({:decimal, precision, scale}), do: {1700, -1, numeric_typmod(precision, scale)}
  defp describe(@map_dtype), do: {3802, -1, -1}
  defp describe({:list, _inner}), do: {3802, -1, -1}
  defp describe({:struct, _fields}), do: {3802, -1, -1}
  defp describe(_other), do: {25, -1, -1}

  defp numeric_typmod(precision, scale) when is_integer(precision) and is_integer(scale),
    do: (precision <<< 16 ||| scale) + 4

  defp numeric_typmod(_precision, _scale), do: -1

  @doc """
  The Postgres type of a DuckDB type name, as `DESCRIBE` reports it.
  """
  @duckdb_json {3802, -1, -1}
  @duckdb_text {25, -1, -1}
  @duckdb_scalars %{
    "BOOLEAN" => {16, 1, -1},
    "DOUBLE" => {701, 8, -1},
    "FLOAT" => {701, 8, -1},
    "REAL" => {701, 8, -1},
    "VARCHAR" => {25, -1, -1},
    "BLOB" => {17, -1, -1},
    "DATE" => {1082, 4, -1},
    "TIMESTAMP" => {1114, 8, -1},
    "TIMESTAMP WITH TIME ZONE" => {1184, 8, -1},
    "TIMESTAMPTZ" => {1184, 8, -1},
    "JSON" => {3802, -1, -1},
    "VARIANT" => {3802, -1, -1}
  }
  @duckdb_integers Map.new(
                     ~w(TINYINT SMALLINT INTEGER BIGINT HUGEINT UTINYINT USMALLINT UINTEGER UBIGINT UHUGEINT),
                     &{&1, {20, 8, -1}}
                   )

  @spec describe_duckdb(String.t()) :: {pos_integer(), integer(), integer()}
  def describe_duckdb(type) do
    upper = String.upcase(type)

    case Map.get(@duckdb_scalars, upper) || Map.get(@duckdb_integers, upper) do
      nil -> describe_duckdb_shape(upper)
      row -> row
    end
  end

  defp describe_duckdb_shape("DECIMAL(" <> rest), do: decimal_typmod(rest)
  defp describe_duckdb_shape("TIME" <> _rest), do: {1083, 8, -1}
  defp describe_duckdb_shape("MAP(" <> _rest), do: @duckdb_json
  defp describe_duckdb_shape("STRUCT(" <> _rest), do: @duckdb_json
  defp describe_duckdb_shape("UNION(" <> _rest), do: @duckdb_json
  defp describe_duckdb_shape("LIST(" <> _rest), do: @duckdb_json

  defp describe_duckdb_shape(upper),
    do: if(String.ends_with?(upper, "[]"), do: @duckdb_json, else: @duckdb_text)

  defp decimal_typmod(rest) do
    case Regex.run(~r/^(\d+),(\d+)/, rest, capture: :all_but_first) do
      [precision, scale] ->
        {1700, -1, numeric_typmod(String.to_integer(precision), String.to_integer(scale))}

      nil ->
        {1700, -1, -1}
    end
  end

  @type_names %{
    "bigint" => 20,
    "int8" => 20,
    "int" => 20,
    "integer" => 20,
    "int4" => 20,
    "smallint" => 20,
    "int2" => 20,
    "text" => 25,
    "varchar" => 25,
    "character varying" => 25,
    "string" => 25,
    "double" => 701,
    "double precision" => 701,
    "float8" => 701,
    "float" => 701,
    "real" => 701,
    "float4" => 701,
    "boolean" => 16,
    "bool" => 16,
    "timestamp" => 1114,
    "timestamp without time zone" => 1114,
    "datetime" => 1114,
    "timestamptz" => 1184,
    "timestamp with time zone" => 1184,
    "date" => 1082,
    "numeric" => 1700,
    "decimal" => 1700,
    "json" => 3802,
    "jsonb" => 3802,
    "bytea" => 17,
    "blob" => 17
  }

  @doc """
  The OID a cast like `$1::bigint` names, or `text` for a name this edge
  does not know.
  """
  @spec oid_for_type_name(String.t()) :: pos_integer()
  def oid_for_type_name(name) do
    key =
      name
      |> String.trim()
      |> String.trim("\"")
      |> String.downcase()
      |> String.replace(~r/\s+/, " ")

    Map.get(@type_names, key, 25)
  end

  @doc """
  The DuckDB type a parameter of `oid` is cast to when its value is
  unknown.
  """
  @spec duckdb_type_for_oid(non_neg_integer()) :: String.t()
  def duckdb_type_for_oid(oid) when oid in [20, 21, 23, 26], do: "BIGINT"
  def duckdb_type_for_oid(oid) when oid in [700, 701], do: "DOUBLE"
  def duckdb_type_for_oid(16), do: "BOOLEAN"
  def duckdb_type_for_oid(1114), do: "TIMESTAMP"
  def duckdb_type_for_oid(1184), do: "TIMESTAMP WITH TIME ZONE"
  def duckdb_type_for_oid(1082), do: "DATE"
  def duckdb_type_for_oid(1700), do: "DECIMAL(38,9)"
  def duckdb_type_for_oid(17), do: "BLOB"
  def duckdb_type_for_oid(oid) when oid in [114, 3802], do: "JSON"
  def duckdb_type_for_oid(_oid), do: "VARCHAR"

  @doc """
  A `Bind` value as a term, by its declared `oid` and wire `format`.
  """
  @spec decode_param(non_neg_integer(), 0 | 1, binary() | nil) ::
          {:ok, param()} | {:error, term()}
  def decode_param(_oid, _format, nil), do: {:ok, nil}
  def decode_param(oid, 0, text), do: decode_text_param(oid, text)
  def decode_param(oid, 1, bytes), do: decode_binary_param(oid, bytes)

  defp decode_text_param(oid, text) when oid in [20, 21, 23, 26] do
    case Integer.parse(text) do
      {int, ""} -> {:ok, int}
      _invalid -> {:error, {:invalid_integer, text}}
    end
  end

  defp decode_text_param(oid, text) when oid in [700, 701], do: parse_float(text)

  defp decode_text_param(16, text) do
    case String.downcase(text) do
      truthy when truthy in ~w(t true y yes on 1) -> {:ok, true}
      falsy when falsy in ~w(f false n no off 0) -> {:ok, false}
      _invalid -> {:error, {:invalid_boolean, text}}
    end
  end

  defp decode_text_param(1700, text) do
    if Regex.match?(~r/^-?\d+(\.\d+)?([eE][-+]?\d+)?$/, text),
      do: {:ok, {:numeric, text}},
      else: parse_float(text)
  end

  defp decode_text_param(oid, text) when oid in [1114, 1184] do
    case NaiveDateTime.from_iso8601(String.replace(text, ~r/[+-]\d\d(:?\d\d)?$/, "")) do
      {:ok, value} -> {:ok, {:timestamp, value}}
      {:error, _reason} -> {:ok, {:text, text}}
    end
  end

  defp decode_text_param(1082, text) do
    case Date.from_iso8601(text) do
      {:ok, value} -> {:ok, {:date, value}}
      {:error, _reason} -> {:ok, {:text, text}}
    end
  end

  defp decode_text_param(oid, text) when oid in [114, 3802], do: {:ok, {:json, text}}

  defp decode_text_param(17, "\\x" <> hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, bytes} -> {:ok, {:bytea, bytes}}
      :error -> {:error, {:invalid_bytea, hex}}
    end
  end

  defp decode_text_param(_oid, text), do: {:ok, {:text, text}}

  defp parse_float(text) do
    case {String.downcase(text), Float.parse(text)} do
      {"nan", _parsed} -> {:ok, :nan}
      {"infinity", _parsed} -> {:ok, :infinity}
      {"-infinity", _parsed} -> {:ok, :neg_infinity}
      {_text, {float, ""}} -> {:ok, float}
      _invalid -> {:error, {:invalid_float, text}}
    end
  end

  defp decode_binary_param(21, <<int::16-signed>>), do: {:ok, int}
  defp decode_binary_param(23, <<int::32-signed>>), do: {:ok, int}
  defp decode_binary_param(20, <<int::64-signed>>), do: {:ok, int}
  defp decode_binary_param(26, <<int::32>>), do: {:ok, int}
  defp decode_binary_param(700, <<float::float-32>>), do: {:ok, float}
  defp decode_binary_param(701, <<float::float-64>>), do: {:ok, float}
  defp decode_binary_param(701, <<0x7FF0000000000000::64>>), do: {:ok, :infinity}
  defp decode_binary_param(701, <<0xFFF0000000000000::64>>), do: {:ok, :neg_infinity}
  defp decode_binary_param(701, <<_nan::64>>), do: {:ok, :nan}
  defp decode_binary_param(16, <<0>>), do: {:ok, false}
  defp decode_binary_param(16, <<_true>>), do: {:ok, true}

  defp decode_binary_param(oid, <<0x7FFFFFFFFFFFFFFF::64-signed>>) when oid in [1114, 1184],
    do: {:ok, {:timestamp, :infinity}}

  defp decode_binary_param(oid, <<-0x8000000000000000::64-signed>>) when oid in [1114, 1184],
    do: {:ok, {:timestamp, :neg_infinity}}

  defp decode_binary_param(oid, <<us::64-signed>>) when oid in [1114, 1184] do
    case DateTime.from_unix(us + @epoch_us, :microsecond) do
      {:ok, value} -> {:ok, {:timestamp, DateTime.to_naive(value)}}
      {:error, _reason} -> {:error, {:timestamp_out_of_range, us}}
    end
  end

  defp decode_binary_param(1082, <<days::32-signed>>),
    do: {:ok, {:date, Date.add(~D[2000-01-01], days)}}

  defp decode_binary_param(1700, bytes), do: decode_numeric(bytes)
  defp decode_binary_param(17, bytes), do: {:ok, {:bytea, bytes}}
  defp decode_binary_param(114, text), do: {:ok, {:json, text}}
  defp decode_binary_param(3802, <<1, text::binary>>), do: {:ok, {:json, text}}

  defp decode_binary_param(2950, <<a::32, b::16, c::16, d::16, e::48>>) do
    {:ok,
     {:text,
      Enum.map_join([{a, 8}, {b, 4}, {c, 4}, {d, 4}, {e, 12}], "-", fn {part, width} ->
        part |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")
      end)}}
  end

  defp decode_binary_param(oid, text) when oid in [25, 1043, 1042, 19, 705, 0],
    do: {:ok, {:text, text}}

  defp decode_binary_param(oid, _bytes), do: {:error, {:unsupported_binary_parameter, oid}}

  defp decode_numeric(<<0::16, _weight::16, 0xC000::16, _scale::16>>), do: {:ok, :nan}
  defp decode_numeric(<<0::16, _weight::16, 0xD000::16, _scale::16>>), do: {:ok, :infinity}
  defp decode_numeric(<<0::16, _weight::16, 0xF000::16, _scale::16>>), do: {:ok, :neg_infinity}

  defp decode_numeric(<<ndigits::16, weight::16-signed, sign::16, scale::16, digits::binary>>)
       when byte_size(digits) == ndigits * 2 do
    groups = for <<group::16 <- digits>>, do: group
    integer = Enum.reduce(groups, 0, &(&2 * 10_000 + &1))
    exponent = (weight + 1 - ndigits) * 4
    coef = if exponent >= 0, do: integer * Integer.pow(10, exponent), else: integer
    exp = min(exponent, 0)
    decimal = %Decimal{sign: if(sign == 0x4000, do: -1, else: 1), coef: coef, exp: exp}

    {:ok, {:numeric, decimal |> Decimal.round(scale) |> Decimal.to_string(:normal)}}
  end

  defp decode_numeric(bytes), do: {:error, {:invalid_numeric, bytes}}

  @doc """
  `value` in wire `format` `0` (text) or `1` (binary).
  """
  @spec encode(dtype(), boolean(), term(), 0 | 1) :: iodata() | nil
  def encode(dtype, json?, value, 0), do: encode_text(dtype, json?, value)
  def encode(dtype, json?, value, 1), do: encode_binary(dtype, json?, value)

  @doc """
  The binary form of `value` for a column of `dtype`.
  """
  @spec encode_binary(dtype(), boolean(), term()) :: iodata() | nil
  def encode_binary(_dtype, _json?, nil), do: nil
  def encode_binary(_dtype, true, value), do: [1, JSON.encode!(value)]
  def encode_binary(:boolean, false, true), do: <<1>>
  def encode_binary(:boolean, false, false), do: <<0>>
  def encode_binary({:f, _bits}, false, value), do: binary_float(value)
  def encode_binary(:binary, false, value), do: value

  def encode_binary({:naive_datetime, _precision}, false, %NaiveDateTime{} = value),
    do: binary_timestamp(value)

  def encode_binary({:datetime, _precision, _zone}, false, %DateTime{} = value),
    do: value |> DateTime.to_naive() |> binary_timestamp()

  def encode_binary(:date, false, %Date{} = value),
    do: <<Date.diff(value, ~D[2000-01-01])::32-signed>>

  def encode_binary({:time, _precision}, false, %Time{} = value) do
    {seconds, us} = Time.to_seconds_after_midnight(value)

    <<seconds * 1_000_000 + us::64-signed>>
  end

  def encode_binary({:decimal, _precision, _scale}, false, %Decimal{} = value),
    do: binary_numeric(value)

  def encode_binary(@map_dtype, false, value) when is_map(value), do: [1, JSON.encode!(value)]
  def encode_binary({:list, _inner}, false, value), do: [1, json_or_inspect(value)]
  def encode_binary({:struct, _fields}, false, value), do: [1, json_or_inspect(value)]
  def encode_binary(_dtype, false, value) when is_integer(value), do: <<value::64-signed>>
  def encode_binary(_dtype, false, %Decimal{} = value), do: binary_numeric(value)
  def encode_binary(dtype, false, value), do: encode_text(dtype, false, value)

  defp binary_float(:nan), do: <<0x7FF8000000000000::64>>
  defp binary_float(:infinity), do: <<0x7FF0000000000000::64>>
  defp binary_float(:neg_infinity), do: <<0xFFF0000000000000::64>>
  defp binary_float(value) when is_number(value), do: <<value::float-64>>
  defp binary_float(value), do: inspect(value)

  defp binary_timestamp(%NaiveDateTime{} = value) do
    us = value |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:microsecond)

    <<us - @epoch_us::64-signed>>
  end

  defp binary_numeric(%Decimal{coef: coef}) when coef in [:NaN, :qNaN, :sNaN],
    do: <<0::16, 0::16, 0xC000::16, 0::16>>

  defp binary_numeric(%Decimal{coef: :inf, sign: 1}), do: <<0::16, 0::16, 0xD000::16, 0::16>>
  defp binary_numeric(%Decimal{coef: :inf, sign: -1}), do: <<0::16, 0::16, 0xF000::16, 0::16>>

  defp binary_numeric(%Decimal{sign: sign, coef: coef, exp: exp}) do
    scale = max(-exp, 0)
    coef = if exp > 0, do: coef * Integer.pow(10, exp), else: coef
    divisor = Integer.pow(10, scale)
    integer = div(coef, divisor)
    fraction = rem(coef, divisor)
    integer_groups = groups(integer)
    fraction_groups = fraction_groups(fraction, scale)
    digits = trim_trailing_zeros(integer_groups ++ fraction_groups)
    weight = length(integer_groups) - 1

    [
      <<length(digits)::16, weight::16-signed, if(sign == -1, do: 0x4000, else: 0)::16,
        scale::16>>,
      Enum.map(digits, &<<&1::16>>)
    ]
  end

  defp groups(0), do: []
  defp groups(int), do: groups(int, [])
  defp groups(0, acc), do: acc
  defp groups(int, acc), do: groups(div(int, 10_000), [rem(int, 10_000) | acc])

  defp fraction_groups(_fraction, 0), do: []

  defp fraction_groups(fraction, scale) do
    padded = Integer.to_string(fraction) |> String.pad_leading(scale, "0")
    width = div(scale + 3, 4) * 4

    padded
    |> String.pad_trailing(width, "0")
    |> String.to_charlist()
    |> Enum.chunk_every(4)
    |> Enum.map(&List.to_integer/1)
  end

  defp trim_trailing_zeros(digits),
    do: digits |> Enum.reverse() |> Enum.drop_while(&(&1 == 0)) |> Enum.reverse()

  @doc """
  The text form of `value`, a term `Smolquery.Engine.Frame.to_rows/2`
  answered for a column of `dtype`. `nil` stays `nil`: the row encoder
  writes SQL NULL for it.
  """
  @spec encode_text(dtype(), boolean(), term()) :: iodata() | nil
  def encode_text(_dtype, _json?, nil), do: nil
  def encode_text(_dtype, true, value), do: JSON.encode!(value)
  def encode_text(:boolean, false, true), do: "t"
  def encode_text(:boolean, false, false), do: "f"
  def encode_text({:f, _bits}, false, value), do: float(value)
  def encode_text(:binary, false, value), do: ["\\x", Base.encode16(value, case: :lower)]

  def encode_text({:naive_datetime, _precision}, false, %NaiveDateTime{} = value),
    do: NaiveDateTime.to_string(value)

  def encode_text({:datetime, _precision, _zone}, false, %DateTime{} = value),
    do: [DateTime.to_naive(value) |> NaiveDateTime.to_string(), "+00"]

  def encode_text(:date, false, %Date{} = value), do: Date.to_string(value)
  def encode_text({:time, _precision}, false, %Time{} = value), do: Time.to_string(value)

  def encode_text({:decimal, _precision, _scale}, false, %Decimal{} = value),
    do: Decimal.to_string(value, :normal)

  def encode_text({:pg_array, _inner}, false, value) when is_list(value), do: pg_array(value)
  def encode_text(@map_dtype, false, value) when is_map(value), do: JSON.encode!(value)
  def encode_text({:list, _inner}, false, value), do: json_or_inspect(value)
  def encode_text({:struct, _fields}, false, value), do: json_or_inspect(value)
  def encode_text(_dtype, false, value) when is_binary(value), do: value
  def encode_text(_dtype, false, value) when is_integer(value), do: Integer.to_string(value)
  def encode_text(_dtype, false, value) when is_float(value), do: float(value)
  def encode_text(_dtype, false, %Decimal{} = value), do: Decimal.to_string(value, :normal)
  def encode_text(_dtype, false, value), do: inspect(value)

  defp float(:nan), do: "NaN"
  defp float(:infinity), do: "Infinity"
  defp float(:neg_infinity), do: "-Infinity"
  defp float(value) when is_float(value), do: Float.to_string(value)
  defp float(value) when is_integer(value), do: Integer.to_string(value)
  defp float(value), do: inspect(value)

  defp pg_array(values), do: ["{", Enum.map_join(values, ",", &pg_array_element/1), "}"]

  defp pg_array_element(nil), do: "NULL"
  defp pg_array_element(value) when is_integer(value), do: Integer.to_string(value)
  defp pg_array_element(value) when is_float(value), do: Float.to_string(value)

  defp pg_array_element(value) when is_binary(value) do
    if value != "" and not String.contains?(value, ["\"", "\\", ",", "{", "}", " "]),
      do: value,
      else: "\"" <> String.replace(String.replace(value, "\\", "\\\\"), "\"", "\\\"") <> "\""
  end

  defp pg_array_element(value), do: inspect(value)

  defp json_or_inspect(value) do
    JSON.encode!(value)
  rescue
    Protocol.UndefinedError -> inspect(value)
  end
end
