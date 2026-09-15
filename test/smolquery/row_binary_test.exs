defmodule Smolquery.RowBinaryTest do
  @moduledoc """
  The fixtures in `test/support/fixtures/rowbinary` were written by ClickHouse
  itself (`clickhouse local` 26.9), from the SQL files beside them:

      clickhouse local --session_timezone=UTC --queries-file typed.sql \\
        --format RowBinaryWithNamesAndTypes > typed.bin
      clickhouse local --session_timezone=UTC --queries-file canon.sql \\
        --format RowBinary > plain.bin
      clickhouse local --session_timezone=UTC --queries-file canon.sql \\
        --format RowBinaryWithNames > names.bin

  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Smolquery.Engine
  alias Smolquery.RowBinary
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @fixtures Path.expand("../support/fixtures/rowbinary", __DIR__)

  @typed_schema Schema.new!([
                  {"id", :int64, nullable: false},
                  {"small", :int64},
                  {"big", :int64},
                  {"ratio", :float64},
                  {"score", :float64},
                  {"name", :string},
                  {"code", :string},
                  {"note", :string},
                  {"tag", :string},
                  {"ok", :bool},
                  {"at", :timestamp},
                  {"at_ns", :timestamp},
                  {"day", :date},
                  {"old_day", :date},
                  {"amount", {:numeric, 18, 2}},
                  {"wide", {:numeric, 38, 6}},
                  {"attrs", {:map, :string, :string}},
                  {"payload", :variant}
                ])

  @canonical_schema Schema.new!([
                      {"id", :int64, nullable: false},
                      {"name", :string},
                      {"score", :float64},
                      {"ok", :bool},
                      {"at", :timestamp},
                      {"day", :date},
                      {"amount", {:numeric, 18, 2}},
                      {"attrs", {:map, :string, :string}},
                      {"payload", :variant}
                    ])

  @canonical_rows [
    %{
      "id" => 1,
      "name" => "a",
      "score" => 1.5,
      "ok" => true,
      "at" => "2026-09-14 10:00:00.000001",
      "day" => "2026-09-14",
      "amount" => "12.50",
      "attrs" => %{"k" => "v"},
      "payload" => %{"x" => 1}
    },
    %{
      "id" => 2,
      "name" => nil,
      "score" => nil,
      "ok" => nil,
      "at" => nil,
      "day" => nil,
      "amount" => nil,
      "attrs" => %{},
      "payload" => nil
    }
  ]

  defp fixture(name), do: File.read!(Path.join(@fixtures, name))

  defp lines(%{ndjson: ndjson}) do
    ndjson
    |> IO.iodata_to_binary()
    |> String.split("\n", trim: true)
    |> Enum.map(&JSON.decode!/1)
  end

  describe "decode/3 on bodies ClickHouse wrote" do
    test "RowBinaryWithNamesAndTypes: every supported type, as the flush reads it" do
      assert {:ok, decoded} =
               RowBinary.decode(@typed_schema, fixture("typed.bin"), :with_names_and_types)

      assert decoded.row_count == 2
      assert decoded.errors == []
      assert decoded.ndjson |> IO.iodata_to_binary() |> String.ends_with?("}\n")

      assert lines(decoded) == [
               %{
                 "id" => 1,
                 "small" => -5,
                 "big" => 9_223_372_036_854_775_807,
                 "ratio" => 0.5,
                 "score" => "NaN",
                 "name" => ~s(héllo "world"\nline2),
                 "code" => "lc",
                 "note" => nil,
                 "tag" => "ab",
                 "ok" => true,
                 "at" => "2026-09-14 10:00:00.000000",
                 "at_ns" => "2026-09-14 10:00:00.123456",
                 "day" => "2026-09-14",
                 "old_day" => "1900-01-01",
                 "amount" => "-123.45",
                 "wide" => "1234567890.123456",
                 "attrs" => %{"host" => "a", "zone" => "b"},
                 "payload" => %{"k" => [1, 2]}
               },
               %{
                 "id" => 2,
                 "small" => 127,
                 "big" => 0,
                 "ratio" => -1.25,
                 "score" => "-Infinity",
                 "name" => "",
                 "code" => "lc",
                 "note" => "note",
                 "tag" => "abcd",
                 "ok" => false,
                 "at" => "1970-01-01 00:00:00.000000",
                 "at_ns" => "1969-12-31 23:59:59.999999",
                 "day" => "1970-01-01",
                 "old_day" => "2299-12-31",
                 "amount" => "0.00",
                 "wide" => "-0.000001",
                 "attrs" => %{},
                 "payload" => 3
               }
             ]
    end

    test "RowBinary reads the schema's columns in order, at the types the schema implies" do
      assert {:ok, decoded} =
               RowBinary.decode(@canonical_schema, fixture("plain.bin"), :row_binary)

      assert decoded.errors == []
      assert lines(decoded) == @canonical_rows
    end

    test "RowBinaryWithNames binds names to columns and reads the types the schema implies" do
      assert {:ok, decoded} =
               RowBinary.decode(@canonical_schema, fixture("names.bin"), :with_names)

      assert decoded.errors == []
      assert lines(decoded) == @canonical_rows
    end

    test "a header may name the columns in any order and leave out nullable ones" do
      schema =
        Schema.new!([{"note", :string}, {"id", :int64, nullable: false}, {"extra", :bool}])

      body = header(["id", "note"], ["Int64", "Nullable(String)"]) <> <<7::little-64, 0, 1, "x">>

      assert {:ok, decoded} = RowBinary.decode(schema, body, :with_names_and_types)
      assert lines(decoded) == [%{"id" => 7, "note" => "x"}]
    end

    @tag :tmp_dir
    test "the segment writer takes the NDJSON and stores every value exactly", %{tmp_dir: dir} do
      engine = Module.concat(__MODULE__, Engine)
      start_supervised!({Engine, name: engine, extensions: []})

      {:ok, decoded} =
        RowBinary.decode(@typed_schema, fixture("typed.bin"), :with_names_and_types)

      path = Path.join(dir, "typed.ndjson")
      File.write!(path, decoded.ndjson)

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, @typed_schema,
          store: Store.Local.new(dir: dir),
          engine: engine
        )

      {:ok, result} =
        Engine.query(
          engine,
          "SELECT id, small, big, ratio, score::VARCHAR, name, code, note, tag, ok, " <>
            ~s|"at"::VARCHAR, at_ns::VARCHAR, day, old_day, amount, wide, attrs, | <>
            "payload::VARCHAR FROM read_parquet($1) ORDER BY id",
          [segment.path]
        )

      assert result.rows == [
               [
                 1,
                 -5,
                 9_223_372_036_854_775_807,
                 0.5,
                 "nan",
                 ~s(héllo "world"\nline2),
                 "lc",
                 nil,
                 "ab",
                 true,
                 "2026-09-14 10:00:00",
                 "2026-09-14 10:00:00.123456",
                 ~D[2026-09-14],
                 ~D[1900-01-01],
                 Decimal.new("-123.45"),
                 Decimal.new("1234567890.123456"),
                 %{"host" => "a", "zone" => "b"},
                 ~s({"k":[1,2]})
               ],
               [
                 2,
                 127,
                 0,
                 -1.25,
                 "-inf",
                 "",
                 "lc",
                 "note",
                 "abcd",
                 false,
                 "1970-01-01 00:00:00",
                 "1969-12-31 23:59:59.999999",
                 ~D[1970-01-01],
                 ~D[2299-12-31],
                 Decimal.new("0.00"),
                 Decimal.new("-0.000001"),
                 %{},
                 "3"
               ]
             ]
    end
  end

  describe "Float32 specials" do
    test "NaN and the infinities decode as DuckDB's strings for them, quiet NaN included" do
      schema = Schema.new!([{"f", :float64}])

      body =
        header(["f"], ["Float32"]) <>
          <<0x7FC00000::little-32, 0xFFC00000::little-32, 0x7F800000::little-32,
            0xFF800000::little-32, 0x7F800001::little-32>>

      assert {:ok, decoded} = RowBinary.decode(schema, body, :with_names_and_types)

      assert Enum.map(lines(decoded), & &1["f"]) ==
               ["NaN", "NaN", "Infinity", "-Infinity", "NaN"]
    end
  end

  describe "decode/3 round trip" do
    test "random rows encoded as RowBinary decode to the values that were encoded" do
      rows = for _row <- 1..500, do: random_row()
      body = IO.iodata_to_binary(Enum.map(rows, &encode_canonical/1))

      assert {:ok, decoded} = RowBinary.decode(@canonical_schema, body, :row_binary)
      assert decoded.errors == []
      assert decoded.row_count == 500
      assert lines(decoded) == Enum.map(rows, &expected/1)
    end
  end

  describe "decode/3 refusing rows" do
    test "a value its column cannot hold refuses its row, and the rows after it still decode" do
      schema =
        Schema.new!([
          {"id", :int64, nullable: false},
          {"big", :int64},
          {"name", :string},
          {"payload", :variant},
          {"amount", {:numeric, 4, 2}},
          {"day", :date},
          {"at", :timestamp},
          {"attrs", {:map, :string, :string}}
        ])

      body =
        IO.iodata_to_binary([
          header(
            ["id", "big", "name", "payload", "amount", "day", "at", "attrs"],
            [
              "Nullable(Int64)",
              "UInt64",
              "String",
              "String",
              "Decimal(9, 2)",
              "Date32",
              "DateTime64(0)",
              "Map(String, String)"
            ]
          ),
          refusal_row(0, id: 1),
          refusal_row(1, id: nil, big: 18_446_744_073_709_551_615),
          refusal_row(2, name: <<255>>, payload: "not json", amount: 100_000, attrs: <<255>>),
          refusal_row(3, day: 3_000_000, at: 9_223_372_036_854_775_807),
          refusal_row(4, id: 5)
        ])

      assert {:ok, decoded} = RowBinary.decode(schema, body, :with_names_and_types)

      assert decoded.row_count == 2
      assert Enum.map(lines(decoded), & &1["id"]) == [1, 5]

      assert decoded.errors == [
               %{
                 index: 1,
                 errors: [
                   %{message: "column id must not be null"},
                   %{message: "column big (INT64) cannot accept 18446744073709551615"}
                 ]
               },
               %{
                 index: 2,
                 errors: [
                   %{message: "column name (STRING) cannot accept <<255>>"},
                   %{message: ~s|column payload (VARIANT) cannot accept "not json"|},
                   %{message: "column amount (NUMERIC(4,2)) cannot accept 1000.00"},
                   %{message: "column attrs (MAP(STRING, STRING)) cannot accept <<255>>"}
                 ]
               },
               %{
                 index: 3,
                 errors: [
                   %{message: "column day (DATE) cannot accept 3000000 days from 1970-01-01"},
                   %{message: "column at (TIMESTAMP) cannot accept 9223372036854775807"}
                 ]
               }
             ]
    end
  end

  describe "decode/3 refusing the request" do
    test "an empty body is zero rows in every format" do
      for format <- [:row_binary, :with_names, :with_names_and_types] do
        assert RowBinary.decode(@canonical_schema, <<>>, format) ==
                 {:ok, %{ndjson: [], row_count: 0, errors: []}}
      end
    end

    test "a body that ends mid-value names the row and column" do
      body = fixture("typed.bin")
      short = binary_part(body, 0, byte_size(body) - 1)

      assert RowBinary.decode(@typed_schema, short, :with_names_and_types) ==
               {:error, {:invalid_rowbinary, "row 1, column payload: the body ends mid-value"}}
    end

    test "a marker byte RowBinary never writes is a misread body" do
      <<id::binary-size(8), _marker, rest::binary>> = fixture("plain.bin")

      assert RowBinary.decode(@canonical_schema, <<id::binary, 7, rest::binary>>, :row_binary) ==
               {:error,
                {:invalid_rowbinary, "row 0, column name: a Nullable marker is 7, not 0 or 1"}}
    end

    test "a header's unknown, materialized, repeated and missing columns are all named" do
      schema =
        Schema.new!([
          {"id", :int64, nullable: false},
          {"ts_int", :int64},
          {"ts", :timestamp, materialized: "epoch_ms(ts_int)"}
        ])

      body = header(["ts", "nope", "ts_int", "ts_int"], ["Int64", "Int64", "Int64", "Int64"])

      assert RowBinary.decode(schema, body, :with_names_and_types) ==
               {:error,
                {:invalid_rowbinary,
                 "column ts_int appears more than once; " <>
                   "column ts is materialized; it takes no value; " <>
                   ~s(unknown column: "nope"; ) <>
                   "column id must not be null, and the header does not name it"}}
    end

    test "a header's types must be ones smolquery stores, in columns that take them" do
      schema =
        Schema.new!([
          {"id", :int64},
          {"tags", :string},
          {"amount", {:numeric, 18, 2}},
          {"attrs", {:map, :string, :string}}
        ])

      body =
        header(
          ["id", "tags", "amount", "attrs"],
          ["String", "Array(String)", "Decimal(18, 3)", "Map(Nullable(String), String)"]
        )

      assert RowBinary.decode(schema, body, :with_names_and_types) ==
               {:error,
                {:invalid_rowbinary,
                 "column id is INT64; ClickHouse type String cannot be written to it; " <>
                   "column tags has ClickHouse type Array(String), which smolquery cannot store; " <>
                   "column amount is NUMERIC(18,2); ClickHouse type Decimal(18, 3) cannot be written to it; " <>
                   "column attrs is MAP(STRING, STRING); " <>
                   "ClickHouse type Map(Nullable(String), String) cannot be written to it"}}
    end

    test "a header that ends early, or names no columns but carries bytes, is refused" do
      assert RowBinary.decode(@canonical_schema, <<2, 2, "id">>, :with_names) ==
               {:error, {:invalid_rowbinary, "header: the body ends mid-value"}}

      schema = Schema.new!([{"id", :int64}])

      assert RowBinary.decode(schema, <<0, 1, 2>>, :with_names_and_types) ==
               {:error, {:invalid_rowbinary, "the header names no columns, but bytes follow it"}}
    end
  end

  describe "parse_type/1" do
    test "reads the type names a ClickHouse header spells" do
      assert RowBinary.parse_type("Int64") == {:ok, {:int, 64, :signed}}
      assert RowBinary.parse_type("UInt8") == {:ok, {:int, 8, :unsigned}}

      assert RowBinary.parse_type("LowCardinality(Nullable(String))") ==
               {:ok, {:nullable, :string}}

      assert RowBinary.parse_type("DateTime('UTC')") == {:ok, :datetime}
      assert RowBinary.parse_type(~S"DateTime('It\'s/Zone')") == {:ok, :datetime}
      assert RowBinary.parse_type("DateTime64(9, 'UTC')") == {:ok, {:datetime64, 9}}

      assert RowBinary.parse_type("Nullable(DateTime64(3))") ==
               {:ok, {:nullable, {:datetime64, 3}}}

      assert RowBinary.parse_type("Decimal(18, 2)") == {:ok, {:decimal, 18, 2}}
      assert RowBinary.parse_type("Decimal128(4)") == {:ok, {:decimal, 38, 4}}
      assert RowBinary.parse_type("FixedString(16)") == {:ok, {:fixed_string, 16}}

      assert RowBinary.parse_type("Map(LowCardinality(String), Nullable(String))") ==
               {:ok, {:map, :string, {:nullable, :string}}}
    end

    test "refuses types it cannot read and text that does not parse" do
      for text <- [
            "Array(String)",
            "UUID",
            "Int128",
            "Decimal(76, 2)",
            "Decimal256(2)",
            "DateTime64(10)",
            "Enum8('a' = 1)",
            "Int64)",
            "Nullable(",
            "Nullable()",
            ""
          ] do
        assert RowBinary.parse_type(text) == :error, "expected #{inspect(text)} to be refused"
      end
    end
  end

  defp leb(n) when n < 128, do: <<n>>
  defp leb(n), do: <<1::1, n &&& 127::7, leb(n >>> 7)::binary>>

  defp str(bytes), do: [leb(byte_size(bytes)), bytes]

  defp header(names, types) do
    IO.iodata_to_binary([leb(length(names)), Enum.map(names, &str/1), Enum.map(types, &str/1)])
  end

  defp refusal_row(_index, overrides) do
    values =
      Keyword.merge(
        [id: 9, big: 1, name: "n", payload: "{}", amount: 1, day: 0, at: 0, attrs: "v"],
        overrides
      )

    [
      nullable(values[:id], &<<&1::little-signed-64>>),
      <<values[:big]::little-unsigned-64>>,
      str(values[:name]),
      str(values[:payload]),
      <<values[:amount]::little-signed-32>>,
      <<values[:day]::little-signed-32>>,
      <<values[:at]::little-signed-64>>,
      [leb(1), str("k"), str(values[:attrs])]
    ]
  end

  defp nullable(nil, _encode), do: <<1>>
  defp nullable(value, encode), do: [<<0>>, encode.(value)]

  @min_micros -62_135_596_800_000_000
  @max_micros 253_402_300_799_999_999

  defp random_row do
    %{
      id:
        Enum.random([
          0,
          -1,
          1,
          9_223_372_036_854_775_807,
          -9_223_372_036_854_775_808,
          random_int(62)
        ]),
      name:
        maybe(fn ->
          Enum.random(["", "plain", ~s(quote " and \\ slash), "new\nline", "ünï 🦆", " "])
        end),
      score:
        maybe(fn ->
          Enum.random([:nan, :infinity, :neg_infinity, -0.0, 1.0e300, :rand.normal() * 1.0e6])
        end),
      ok: maybe(fn -> Enum.random([true, false]) end),
      at: maybe(fn -> Enum.random([@min_micros, @max_micros, 0, -1, random_int(52)]) end),
      day: maybe(fn -> Enum.random([-719_162, 2_932_896, 0, -1, random_int(15)]) end),
      amount:
        maybe(fn -> Enum.random([0, 1, -1, 999_999_999_999_999_999, -99, random_int(55)]) end),
      attrs:
        Map.new(1..Enum.random(0..3)//1, fn i -> {"k#{i}", Enum.random(["", "v", "ü\n"])} end),
      payload: maybe(fn -> Enum.random([%{"a" => [1, nil, "x"]}, [], "s", 12, 1.5, true]) end)
    }
  end

  defp maybe(generate), do: if(:rand.uniform(4) == 1, do: nil, else: generate.())

  defp random_int(bits), do: :rand.uniform(1 <<< bits) - (1 <<< (bits - 1))

  defp encode_canonical(row) do
    [
      <<row.id::little-signed-64>>,
      nullable(row.name, &str/1),
      nullable(row.score, &float_bytes/1),
      nullable(row.ok, &if(&1, do: <<1>>, else: <<0>>)),
      nullable(row.at, &<<&1::little-signed-64>>),
      nullable(row.day, &<<&1::little-signed-32>>),
      nullable(row.amount, &<<&1::little-signed-64>>),
      [leb(map_size(row.attrs)), Enum.map(row.attrs, fn {k, v} -> [str(k), str(v)] end)],
      nullable(row.payload, &str(JSON.encode!(&1)))
    ]
  end

  defp float_bytes(:nan), do: <<0x7FF8000000000000::little-64>>
  defp float_bytes(:infinity), do: <<0x7FF0000000000000::little-64>>
  defp float_bytes(:neg_infinity), do: <<0xFFF0000000000000::little-64>>
  defp float_bytes(float), do: <<float::little-float-64>>

  defp expected(row) do
    %{
      "id" => row.id,
      "name" => row.name,
      "score" => expected_float(row.score),
      "ok" => row.ok,
      "at" => row.at && row.at |> DateTime.from_unix!(:microsecond) |> NaiveDateTime.to_string(),
      "day" => row.day && ~D[1970-01-01] |> Date.add(row.day) |> Date.to_string(),
      "amount" => row.amount && expected_decimal(row.amount),
      "attrs" => row.attrs,
      "payload" => row.payload
    }
  end

  defp expected_float(:nan), do: "NaN"
  defp expected_float(:infinity), do: "Infinity"
  defp expected_float(:neg_infinity), do: "-Infinity"
  defp expected_float(other), do: other

  defp expected_decimal(unscaled) do
    sign = if unscaled < 0, do: -1, else: 1

    sign |> Decimal.new(abs(unscaled), -2) |> Decimal.to_string(:normal)
  end
end
