defmodule SmolqueryClickHouse.Format do
  @moduledoc """
  The output formats a ClickHouse client can ask a query to answer in (T-478).

  | name | answer |
  |---|---|
  | `TabSeparated`, `TSV` (the default) | one line per row, values tab-separated |
  | `TabSeparatedWithNames`, `TSVWithNames` | a line of column names first |
  | `TabSeparatedWithNamesAndTypes`, `TSVWithNamesAndTypes` | then a line of types |
  | `TabSeparatedRaw`, `TSVRaw` | `TabSeparated` with no escaping |
  | `JSON` | `meta`, `data` as objects, `rows`, `statistics` |
  | `JSONCompact` | the same, with `data` as arrays |
  | `JSONEachRow` | one JSON object per line |
  | `JSONCompactEachRow` | one JSON array per line |
  | `JSONCompactEachRowWithNames` | a line of column names first |
  | `JSONCompactEachRowWithNamesAndTypes` | then a line of types |
  | `RowBinaryWithNamesAndTypes` | a header of names and types, then binary rows |

  Names match without regard to case. ClickHouse's own are case-sensitive,
  so a client that spells them as ClickHouse does is always understood.
  `RowBinaryWithNamesAndTypes` is what `ch`, the Elixir client, asks for and
  decodes. `JSONCompactEachRowWithNamesAndTypes` is what HyperDX streams its
  results table from (T-493).

  ## Types

  Each column's ClickHouse type derives from the result frame's Explorer
  dtype, the only type a query result carries. A frame does not say whether
  a column can hold `NULL`, so every type but a map is `Nullable`.

  | dtype | ClickHouse type |
  |---|---|
  | `{:s, n}`, `{:u, n}` | `Int<n>`, `UInt<n>` |
  | `{:f, n}` | `Float<n>` |
  | `:string` | `String` |
  | `:boolean` | `Bool` |
  | `{:naive_datetime, _}` | `DateTime64(6)`, or `DateTime64(9)` at nanoseconds |
  | `:date` | `Date32` |
  | `{:decimal, p, s}` | `Decimal(p, s)` |
  | `MAP(STRING, STRING)` | `Map(String, String)` |
  | a list | `Array(Nullable(T))`, and `Array(Array(...))` for a list of lists |
  | `VARIANT`, a struct, anything else | `String`, holding JSON or text |

  ## Values

  Text follows ClickHouse's defaults. In a tab-separated row a backslash, a
  tab, a newline, a carriage return, a NUL and a single quote are escaped
  with a backslash, and `NULL` is `\\N`. In JSON a 64-bit integer is a
  quoted string (`output_format_json_quote_64bit_integers`), a decimal is a
  bare number, and a non-finite float is `null`. A timestamp is
  `YYYY-MM-DD hh:mm:ss.ffffff` in both, or `YYYY-MM-DDThh:mm:ss.ffffffZ`
  under `date_time: :iso`, which is ClickHouse's
  `date_time_output_format = 'iso'`. In RowBinary a `NULL` map value is
  written as the empty string, since the map's values are not `Nullable`.

  An array is a JSON array, `['a','b']` in a tab-separated row, and a
  length then its elements in RowBinary. A `NULL` array answers as the empty
  one, as a `NULL` map does.

  A `NULL` map answers as the empty map in every format, for the same
  reason: its column's type is not `Nullable`, and in RowBinary a `Nullable`
  marker there would be read as a map's size.

  JSON text must be UTF-8, so a value that is not has its invalid bytes
  replaced with U+FFFD in the JSON formats. `TabSeparated` and RowBinary
  carry a string's bytes as they are.
  """

  import Bitwise

  @type t ::
          :tsv
          | :tsv_names
          | :tsv_names_types
          | :tsv_raw
          | :json
          | :json_compact
          | :json_each_row
          | :json_compact_each_row
          | :json_compact_each_row_names
          | :json_compact_each_row_names_types
          | :row_binary_with_names_and_types

  @typedoc """
  A result column: its name, its Explorer dtype, and whether it holds JSON
  text (`job.json_columns`).
  """
  @type column :: {String.t(), term(), boolean()}

  @names %{
    "tabseparated" => :tsv,
    "tsv" => :tsv,
    "tabseparatedwithnames" => :tsv_names,
    "tsvwithnames" => :tsv_names,
    "tabseparatedwithnamesandtypes" => :tsv_names_types,
    "tsvwithnamesandtypes" => :tsv_names_types,
    "tabseparatedraw" => :tsv_raw,
    "tsvraw" => :tsv_raw,
    "json" => :json,
    "jsoncompact" => :json_compact,
    "jsoneachrow" => :json_each_row,
    "jsoncompacteachrow" => :json_compact_each_row,
    "jsoncompacteachrowwithnames" => :json_compact_each_row_names,
    "jsoncompacteachrowwithnamesandtypes" => :json_compact_each_row_names_types,
    "rowbinarywithnamesandtypes" => :row_binary_with_names_and_types
  }

  @canonical %{
    tsv: "TabSeparated",
    tsv_names: "TabSeparatedWithNames",
    tsv_names_types: "TabSeparatedWithNamesAndTypes",
    tsv_raw: "TabSeparatedRaw",
    json: "JSON",
    json_compact: "JSONCompact",
    json_each_row: "JSONEachRow",
    json_compact_each_row: "JSONCompactEachRow",
    json_compact_each_row_names: "JSONCompactEachRowWithNames",
    json_compact_each_row_names_types: "JSONCompactEachRowWithNamesAndTypes",
    row_binary_with_names_and_types: "RowBinaryWithNamesAndTypes"
  }

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}
  @unix_epoch ~N[1970-01-01 00:00:00]

  @doc """
  The format a name spells, in any case.
  """
  @spec fetch(String.t()) :: {:ok, t()} | :error
  def fetch(name) when is_binary(name), do: Map.fetch(@names, String.downcase(name))

  @doc """
  The name ClickHouse spells `format` with, as `X-ClickHouse-Format` carries it.
  """
  @spec name(t()) :: String.t()
  def name(format), do: Map.fetch!(@canonical, format)

  @doc """
  The `content-type` of an answer in `format`.
  """
  @spec content_type(t()) :: String.t()
  def content_type(format) when format in [:tsv, :tsv_names, :tsv_names_types, :tsv_raw],
    do: "text/tab-separated-values; charset=UTF-8"

  def content_type(:row_binary_with_names_and_types), do: "application/octet-stream"
  def content_type(_json), do: "application/json; charset=UTF-8"

  @doc """
  The ClickHouse type a result column answers as.
  """
  @spec type_name(term(), boolean()) :: String.t()
  def type_name(_dtype, true), do: "Nullable(String)"
  def type_name(@map_dtype, false), do: "Map(String, String)"
  def type_name({:list, _element} = dtype, false), do: base_type(dtype)
  def type_name(dtype, false), do: "Nullable(" <> base_type(dtype) <> ")"

  defp base_type({:s, bits}), do: "Int#{bits}"
  defp base_type({:u, bits}), do: "UInt#{bits}"
  defp base_type({:f, bits}), do: "Float#{bits}"
  defp base_type(:boolean), do: "Bool"
  defp base_type(:string), do: "String"
  defp base_type({:naive_datetime, :nanosecond}), do: "DateTime64(9)"
  defp base_type({:naive_datetime, _precision}), do: "DateTime64(6)"
  defp base_type({:datetime, :nanosecond, _zone}), do: "DateTime64(9, 'UTC')"
  defp base_type({:datetime, _precision, _zone}), do: "DateTime64(6, 'UTC')"
  defp base_type(:date), do: "Date32"
  defp base_type({:decimal, precision, scale}), do: "Decimal(#{precision}, #{scale})"
  defp base_type({:list, {:list, _element} = nested}), do: "Array(#{base_type(nested)})"
  defp base_type({:list, element}), do: "Array(Nullable(#{base_type(element)}))"
  defp base_type(_other), do: "String"

  @doc """
  `rows` of `columns` in `format`.

  `elapsed_ms:` is reported in the `statistics` of the `JSON` formats.
  `date_time: :iso` writes a timestamp as ISO 8601 with a `Z`, in the text
  formats; RowBinary carries a timestamp as a number either way.
  """
  @spec encode(t(), [column()], [map()], elapsed_ms: non_neg_integer(), date_time: :simple | :iso) ::
          iodata()
  def encode(format, columns, rows, opts \\ []) do
    style = Keyword.get(opts, :date_time, :simple)

    body(format, columns, rows, style, Keyword.get(opts, :elapsed_ms, 0))
  end

  defp body(:tsv, columns, rows, style, _elapsed_ms),
    do: tsv_rows(columns, rows, style, &escape/1)

  defp body(:tsv_raw, columns, rows, style, _elapsed_ms),
    do: tsv_rows(columns, rows, style, &Function.identity/1)

  defp body(:tsv_names, columns, rows, style, _elapsed_ms),
    do: [
      tsv_line(Enum.map(columns, &escape(elem(&1, 0)))),
      tsv_rows(columns, rows, style, &escape/1)
    ]

  defp body(:tsv_names_types, columns, rows, style, _elapsed_ms) do
    [
      tsv_line(Enum.map(columns, &escape(elem(&1, 0)))),
      tsv_line(type_names(columns)),
      tsv_rows(columns, rows, style, &escape/1)
    ]
  end

  defp body(:json_each_row, columns, rows, style, _elapsed_ms),
    do: Enum.map(rows, &[json_object(columns, &1, style), "\n"])

  defp body(:json_compact_each_row, columns, rows, style, _elapsed_ms),
    do: Enum.map(rows, &[json_array(columns, &1, style), "\n"])

  defp body(:json_compact_each_row_names, columns, rows, style, elapsed_ms),
    do: [json_names(columns), body(:json_compact_each_row, columns, rows, style, elapsed_ms)]

  defp body(:json_compact_each_row_names_types, columns, rows, style, elapsed_ms) do
    [
      json_names(columns),
      JSON.encode_to_iodata!(type_names(columns)),
      "\n",
      body(:json_compact_each_row, columns, rows, style, elapsed_ms)
    ]
  end

  defp body(:row_binary_with_names_and_types, columns, rows, _style, _elapsed_ms) do
    [
      leb128(length(columns)),
      Enum.map(columns, fn {name, _dtype, _json?} -> binary_string(name) end),
      Enum.map(type_names(columns), &binary_string/1),
      Enum.map(rows, fn row ->
        Enum.map(columns, fn {name, dtype, json?} -> binary(dtype, json?, row[name]) end)
      end)
    ]
  end

  defp body(format, columns, rows, style, elapsed_ms) when format in [:json, :json_compact] do
    meta =
      Enum.map_intersperse(columns, ",", fn {name, dtype, json?} ->
        [
          ~s({"name":),
          JSON.encode!(name),
          ~s(,"type":),
          JSON.encode!(type_name(dtype, json?)),
          "}"
        ]
      end)

    data =
      Enum.map_intersperse(rows, ",", fn row ->
        if format == :json,
          do: json_object(columns, row, style),
          else: json_array(columns, row, style)
      end)

    [
      ~s({"meta":[),
      meta,
      ~s(],"data":[),
      data,
      ~s(],"rows":),
      Integer.to_string(length(rows)),
      ~s(,"statistics":{"elapsed":),
      Float.to_string(elapsed_ms / 1000),
      ~s(,"rows_read":0,"bytes_read":0}}\n)
    ]
  end

  defp type_names(columns),
    do: Enum.map(columns, fn {_name, dtype, json?} -> type_name(dtype, json?) end)

  defp json_names(columns),
    do: [
      JSON.encode_to_iodata!(Enum.map(columns, fn {name, _dtype, _json?} -> utf8(name) end)),
      "\n"
    ]

  defp styled(%NaiveDateTime{} = value, :iso), do: NaiveDateTime.to_iso8601(value) <> "Z"

  defp styled(%DateTime{} = value, :iso),
    do: value |> DateTime.to_naive() |> styled(:iso)

  defp styled(value, _style), do: value

  defp tsv_rows(columns, rows, style, escape) do
    Enum.map(rows, fn row ->
      tsv_line(
        Enum.map(columns, fn {name, dtype, json?} ->
          tsv(dtype, json?, styled(row[name], style), escape)
        end)
      )
    end)
  end

  defp tsv_line(values), do: [Enum.intersperse(values, "\t"), "\n"]

  defp tsv(@map_dtype, false, nil, _escape), do: "{}"
  defp tsv({:list, _element}, false, nil, _escape), do: "[]"

  defp tsv({:list, _element}, false, value, _escape) when is_list(value),
    do: array_text(value)

  defp tsv(_dtype, _json?, nil, _escape), do: "\\N"
  defp tsv(_dtype, true, value, escape), do: escape.(json_text(value))
  defp tsv(@map_dtype, false, value, _escape) when is_map(value), do: map_text(value)
  defp tsv(_dtype, false, value, escape) when is_binary(value), do: escape.(value)

  defp tsv(_dtype, false, value, escape)
       when is_list(value) or (is_map(value) and not is_struct(value)),
       do: escape.(json_text(value))

  defp tsv(_dtype, false, value, escape), do: escape.(text(value))

  defp array_text(values) do
    elements =
      Enum.map_intersperse(values, ",", fn
        nil -> "NULL"
        nested when is_list(nested) -> array_text(nested)
        text when is_binary(text) -> quoted(text)
        other -> escape(text(other))
      end)

    ["[", elements, "]"]
  end

  defp map_text(map) do
    entries =
      Enum.map_intersperse(map, ",", fn {key, value} ->
        [quoted(key), ":", if(is_nil(value), do: "NULL", else: quoted(value))]
      end)

    ["{", entries, "}"]
  end

  defp quoted(text), do: ["'", escape(to_string(text)), "'"]

  defp escape(text) do
    if String.contains?(text, ["\\", "\t", "\n", "\r", "\0", "'"]) do
      for <<byte <- text>>, into: "", do: escape_byte(byte)
    else
      text
    end
  end

  defp escape_byte(?\\), do: "\\\\"
  defp escape_byte(?\t), do: "\\t"
  defp escape_byte(?\n), do: "\\n"
  defp escape_byte(?\r), do: "\\r"
  defp escape_byte(0), do: "\\0"
  defp escape_byte(?'), do: "\\'"
  defp escape_byte(byte), do: <<byte>>

  defp json_object(columns, row, style) do
    fields =
      Enum.map_intersperse(columns, ",", fn {name, dtype, json?} ->
        [json_string(name), ":", json(dtype, json?, styled(row[name], style))]
      end)

    ["{", fields, "}"]
  end

  defp json_array(columns, row, style) do
    values =
      Enum.map_intersperse(columns, ",", fn {name, dtype, json?} ->
        json(dtype, json?, styled(row[name], style))
      end)

    ["[", values, "]"]
  end

  defp json(@map_dtype, false, nil), do: "{}"
  defp json({:list, _element}, false, nil), do: "[]"

  defp json({:list, element}, false, value) when is_list(value),
    do: ["[", Enum.map_intersperse(value, ",", &json(element, false, &1)), "]"]

  defp json(_dtype, _json?, nil), do: "null"
  defp json(_dtype, true, value), do: json_string(json_text(value))
  defp json({kind, 64}, false, value) when kind in [:s, :u], do: [?", text(value), ?"]
  defp json(_dtype, false, value) when is_boolean(value), do: to_string(value)
  defp json(_dtype, false, value) when is_integer(value), do: Integer.to_string(value)
  defp json(_dtype, false, value) when value in [:nan, :infinity, :neg_infinity], do: "null"
  defp json(_dtype, false, value) when is_float(value), do: JSON.encode!(value)
  defp json(_dtype, false, %Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp json(@map_dtype, false, value) when is_map(value), do: JSON.encode!(utf8(value))
  defp json(_dtype, false, value) when is_binary(value), do: json_string(value)

  defp json(_dtype, false, value) when is_list(value) or (is_map(value) and not is_struct(value)),
    do: json_string(json_text(value))

  defp json(_dtype, false, value), do: json_string(text(value))

  defp json_string(text), do: JSON.encode!(utf8(text))

  defp utf8(value) when is_binary(value),
    do: if(String.valid?(value), do: value, else: String.replace_invalid(value))

  defp utf8(value) when is_list(value), do: Enum.map(value, &utf8/1)

  defp utf8(value) when is_map(value) and not is_struct(value),
    do: Map.new(value, fn {key, entry} -> {utf8(key), utf8(entry)} end)

  defp utf8(value), do: value

  defp binary(@map_dtype, false, nil), do: <<0>>
  defp binary({:list, _element}, false, nil), do: <<0>>

  defp binary({:list, element}, false, value) when is_list(value),
    do: [leb128(length(value)), Enum.map(value, &binary(element, false, &1))]

  defp binary(@map_dtype, false, value) when is_map(value) do
    [
      leb128(map_size(value)),
      Enum.map(value, fn {key, entry} -> [binary_string(key), binary_string(entry || "")] end)
    ]
  end

  defp binary(_dtype, _json?, nil), do: <<1>>
  defp binary(_dtype, true, value), do: [0, binary_string(json_text(value))]
  defp binary(dtype, false, value), do: [0, binary_value(dtype, value)]

  defp binary_value({:s, bits}, value) when is_integer(value),
    do: <<value::little-signed-size(bits)>>

  defp binary_value({:u, bits}, value) when is_integer(value),
    do: <<value::little-unsigned-size(bits)>>

  defp binary_value({:f, bits}, value), do: binary_float(bits, value)
  defp binary_value(:boolean, value), do: if(value, do: <<1>>, else: <<0>>)

  defp binary_value({:naive_datetime, :nanosecond}, %NaiveDateTime{} = value),
    do: <<NaiveDateTime.diff(value, @unix_epoch, :nanosecond)::little-signed-64>>

  defp binary_value({:naive_datetime, _precision}, %NaiveDateTime{} = value),
    do: <<NaiveDateTime.diff(value, @unix_epoch, :microsecond)::little-signed-64>>

  defp binary_value({:datetime, precision, _zone}, %DateTime{} = value),
    do: binary_value({:naive_datetime, precision}, DateTime.to_naive(value))

  defp binary_value(:date, %Date{} = value),
    do: <<Date.diff(value, ~D[1970-01-01])::little-signed-32>>

  defp binary_value({:decimal, precision, scale}, %Decimal{} = value) do
    scaled =
      value |> Decimal.mult(Decimal.new(1, 1, scale)) |> Decimal.round(0) |> Decimal.to_integer()

    <<scaled::little-signed-size(decimal_bits(precision))>>
  end

  defp binary_value(_dtype, value) when is_binary(value), do: binary_string(value)

  defp binary_value(_dtype, value)
       when is_list(value) or (is_map(value) and not is_struct(value)),
       do: binary_string(json_text(value))

  defp binary_value(_dtype, value), do: binary_string(text(value))

  defp binary_float(bits, :nan), do: nan(bits)
  defp binary_float(32, :infinity), do: <<0, 0, 128, 127>>
  defp binary_float(32, :neg_infinity), do: <<0, 0, 128, 255>>
  defp binary_float(64, :infinity), do: <<0, 0, 0, 0, 0, 0, 240, 127>>
  defp binary_float(64, :neg_infinity), do: <<0, 0, 0, 0, 0, 0, 240, 255>>
  defp binary_float(bits, value), do: <<value::little-float-size(bits)>>

  defp nan(32), do: <<0, 0, 192, 127>>
  defp nan(64), do: <<0, 0, 0, 0, 0, 0, 248, 127>>

  defp decimal_bits(precision) when precision <= 9, do: 32
  defp decimal_bits(precision) when precision <= 18, do: 64
  defp decimal_bits(precision) when precision <= 38, do: 128
  defp decimal_bits(_precision), do: 256

  defp binary_string(text), do: [leb128(byte_size(text)), text]

  defp leb128(n) when n < 128, do: <<n>>
  defp leb128(n), do: [<<1::1, band(n, 127)::7>>, leb128(bsr(n, 7))]

  defp json_text(value) when is_binary(value), do: value

  defp json_text(value) do
    JSON.encode!(utf8(value))
  rescue
    Protocol.UndefinedError -> inspect(value)
  end

  defp text(true), do: "true"
  defp text(false), do: "false"
  defp text(value) when is_integer(value), do: Integer.to_string(value)
  defp text(:nan), do: "nan"
  defp text(:infinity), do: "inf"
  defp text(:neg_infinity), do: "-inf"
  defp text(value) when is_float(value), do: JSON.encode!(value)
  defp text(%NaiveDateTime{} = value), do: NaiveDateTime.to_string(value)
  defp text(%DateTime{} = value), do: value |> DateTime.to_naive() |> NaiveDateTime.to_string()
  defp text(%Date{} = value), do: Date.to_string(value)
  defp text(%Time{} = value), do: Time.to_string(value)
  defp text(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp text(value), do: inspect(value)
end
