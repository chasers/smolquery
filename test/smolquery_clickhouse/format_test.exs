defmodule SmolqueryClickHouse.FormatTest do
  use ExUnit.Case, async: true

  alias SmolqueryClickHouse.Format

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}

  @columns [
    {"id", {:s, 64}, false},
    {"msg", :string, false},
    {"ts", {:naive_datetime, :microsecond}, false},
    {"attrs", @map_dtype, false},
    {"doc", :string, true}
  ]

  @rows [
    %{
      "id" => 1,
      "msg" => "tab\there, it's",
      "ts" => ~N[2026-09-15 12:00:00.123456],
      "attrs" => %{"host" => "a"},
      "doc" => %{"n" => 1}
    },
    %{"id" => nil, "msg" => nil, "ts" => nil, "attrs" => %{}, "doc" => nil}
  ]

  defp encode(format, columns \\ @columns, rows \\ @rows),
    do: format |> Format.encode(columns, rows, elapsed_ms: 12) |> IO.iodata_to_binary()

  describe "fetch/1" do
    test "reads ClickHouse's names and their aliases, in any case" do
      assert Format.fetch("TabSeparated") == {:ok, :tsv}
      assert Format.fetch("TSVWithNamesAndTypes") == {:ok, :tsv_names_types}
      assert Format.fetch("jsoneachrow") == {:ok, :json_each_row}
      assert Format.fetch("Native") == :error
    end
  end

  test "name/1 and content_type/1 answer as ClickHouse does" do
    assert Format.name(:json_compact) == "JSONCompact"
    assert Format.content_type(:tsv) =~ "tab-separated-values"
    assert Format.content_type(:json_each_row) =~ "application/json"
  end

  test "type_name/2 wraps every type but a map in Nullable" do
    assert Format.type_name({:s, 64}, false) == "Nullable(Int64)"
    assert Format.type_name({:decimal, 18, 2}, false) == "Nullable(Decimal(18, 2))"
    assert Format.type_name({:naive_datetime, :nanosecond}, false) == "Nullable(DateTime64(9))"
    assert Format.type_name(@map_dtype, false) == "Map(String, String)"
    assert Format.type_name({:s, 64}, true) == "Nullable(String)"
  end

  describe "encode/4" do
    test "TabSeparated escapes text and writes NULL as \\N" do
      assert encode(:tsv) ==
               "1\ttab\\there, it\\'s\t2026-09-15 12:00:00.123456\t{'host':'a'}\t{\"n\":1}\n" <>
                 "\\N\t\\N\t\\N\t{}\t\\N\n"
    end

    test "TabSeparatedWithNamesAndTypes leads with names, then types" do
      [names, types | _rows] = String.split(encode(:tsv_names_types), "\n")

      assert names == "id\tmsg\tts\tattrs\tdoc"

      assert types ==
               "Nullable(Int64)\tNullable(String)\tNullable(DateTime64(6))\tMap(String, String)\tNullable(String)"
    end

    test "JSONEachRow quotes 64-bit integers and keeps maps as objects" do
      assert [first, second, ""] = String.split(encode(:json_each_row), "\n")

      assert JSON.decode!(first) == %{
               "id" => "1",
               "msg" => "tab\there, it's",
               "ts" => "2026-09-15 12:00:00.123456",
               "attrs" => %{"host" => "a"},
               "doc" => ~s({"n":1})
             }

      assert %{"id" => nil, "attrs" => %{}} = JSON.decode!(second)
    end

    test "JSON carries meta, data, rows and statistics" do
      assert %{
               "meta" => [%{"name" => "id", "type" => "Nullable(Int64)"} | _],
               "data" => [%{"id" => "1"}, %{"id" => nil}],
               "rows" => 2,
               "statistics" => %{"elapsed" => 0.012}
             } = JSON.decode!(encode(:json))
    end

    test "JSONCompact writes each row as an array" do
      assert %{"data" => [["1" | _], [nil | _]]} = JSON.decode!(encode(:json_compact))
    end

    test "RowBinaryWithNamesAndTypes writes a header, then Nullable-marked values" do
      columns = [
        {"n", {:s, 64}, false},
        {"s", :string, false},
        {"d", :date, false},
        {"m", @map_dtype, false}
      ]

      rows = [
        %{"n" => 2, "s" => "hi", "d" => ~D[1970-01-03], "m" => %{"k" => "v"}},
        %{"n" => nil, "s" => nil, "d" => nil, "m" => %{}}
      ]

      assert encode(:row_binary_with_names_and_types, columns, rows) ==
               IO.iodata_to_binary([
                 4,
                 [1, "n", 1, "s", 1, "d", 1, "m"],
                 [15, "Nullable(Int64)", 16, "Nullable(String)", 16, "Nullable(Date32)"],
                 [19, "Map(String, String)"],
                 [0, <<2::little-signed-64>>, 0, 2, "hi", 0, <<2::little-signed-32>>],
                 [1, 1, "k", 1, "v"],
                 [1, 1, 1, 0]
               ])
    end

    test "RowBinaryWithNamesAndTypes scales decimals and counts timestamps from the epoch" do
      columns = [{"d", {:decimal, 10, 2}, false}, {"t", {:naive_datetime, :microsecond}, false}]
      rows = [%{"d" => Decimal.new("1.50"), "t" => ~N[1970-01-01 00:00:01.000002]}]

      body =
        :row_binary_with_names_and_types
        |> Format.encode(columns, rows)
        |> IO.iodata_to_binary()

      assert binary_part(body, byte_size(body) - 18, 18) ==
               <<0, 150::little-signed-64, 0, 1_000_002::little-signed-64>>
    end

    test "a NULL map answers as the empty map, since its type is not Nullable" do
      columns = [{"m", @map_dtype, false}, {"n", {:s, 64}, false}]
      rows = [%{"m" => nil, "n" => 7}]

      assert encode(:tsv, columns, rows) == "{}\t7\n"
      assert JSON.decode!(encode(:json_each_row, columns, rows)) == %{"m" => %{}, "n" => "7"}

      body = encode(:row_binary_with_names_and_types, columns, rows)
      assert binary_part(body, byte_size(body) - 10, 10) == <<0, 0, 7::little-signed-64>>
    end

    test "a string that is not UTF-8 is replaced in JSON and carried as it is elsewhere" do
      columns = [{"s", :string, false}, {"m", @map_dtype, false}]
      rows = [%{"s" => <<255, ?a>>, "m" => %{"k" => <<255>>}}]

      assert JSON.decode!(encode(:json_each_row, columns, rows)) ==
               %{"s" => "�a", "m" => %{"k" => "�"}}

      assert encode(:tsv, columns, rows) == <<255, ?a, ?\t, "{'k':'", 255, "'}\n">>
    end

    test "non-finite floats, decimals and booleans follow ClickHouse's defaults" do
      columns = [{"f", {:f, 64}, false}, {"d", {:decimal, 10, 2}, false}, {"b", :boolean, false}]
      rows = [%{"f" => :nan, "d" => Decimal.new("1.50"), "b" => true}]

      assert encode(:tsv, columns, rows) == "nan\t1.50\ttrue\n"

      assert JSON.decode!(encode(:json_each_row, columns, rows)) == %{
               "f" => nil,
               "d" => 1.5,
               "b" => true
             }
    end
  end

  describe "the JSONCompactEachRow family (T-493)" do
    test "JSONCompactEachRowWithNamesAndTypes is names, types, then a row per line" do
      assert encode(:json_compact_each_row_names_types) ==
               ~s|["id","msg","ts","attrs","doc"]\n| <>
                 ~s|["Nullable(Int64)","Nullable(String)","Nullable(DateTime64(6))","Map(String, String)","Nullable(String)"]\n| <>
                 ~s|["1","tab\\there, it's","2026-09-15 12:00:00.123456",{"host":"a"},"{\\"n\\":1}"]\n| <>
                 ~s|[null,null,null,{},null]\n|
    end

    test "with no rows the two header lines still answer" do
      assert encode(:json_compact_each_row_names_types, @columns, [])
             |> String.split("\n")
             |> length() == 3
    end

    test "the shorter two leave the types, or both lines, out" do
      assert [names, _row, _null, ""] = encode(:json_compact_each_row_names) |> String.split("\n")
      assert names == ~s|["id","msg","ts","attrs","doc"]|

      assert [row, _null, ""] = encode(:json_compact_each_row) |> String.split("\n")
      assert String.starts_with?(row, ~s|["1",|)
    end

    test "their names are fetched as ClickHouse spells them" do
      assert Format.fetch("JSONCompactEachRowWithNamesAndTypes") ==
               {:ok, :json_compact_each_row_names_types}

      assert Format.name(:json_compact_each_row_names_types) ==
               "JSONCompactEachRowWithNamesAndTypes"
    end
  end

  test "TabSeparatedRaw writes a value as it is, and NULL as \\N" do
    assert encode(:tsv_raw) ==
             "1\ttab\there, it's\t2026-09-15 12:00:00.123456\t{'host':'a'}\t{\"n\":1}\n" <>
               "\\N\t\\N\t\\N\t{}\t\\N\n"

    assert Format.fetch("TSVRaw") == {:ok, :tsv_raw}
  end

  describe "date_time: :iso" do
    defp iso(format) do
      columns = [{"ts", {:naive_datetime, :microsecond}, false}]
      rows = [%{"ts" => ~N[2026-09-15 12:00:00.123456]}, %{"ts" => nil}]

      format |> Format.encode(columns, rows, date_time: :iso) |> IO.iodata_to_binary()
    end

    test "a timestamp is ISO 8601 with a Z in the JSON and tab-separated formats" do
      assert iso(:json_each_row) == ~s|{"ts":"2026-09-15T12:00:00.123456Z"}\n{"ts":null}\n|
      assert iso(:json_compact_each_row) == ~s|["2026-09-15T12:00:00.123456Z"]\n[null]\n|
      assert iso(:tsv) == "2026-09-15T12:00:00.123456Z\n\\N\n"
    end

    test "RowBinary carries the same number either way" do
      assert iso(:row_binary_with_names_and_types) ==
               :row_binary_with_names_and_types
               |> Format.encode(
                 [{"ts", {:naive_datetime, :microsecond}, false}],
                 [%{"ts" => ~N[2026-09-15 12:00:00.123456]}, %{"ts" => nil}]
               )
               |> IO.iodata_to_binary()
    end
  end

  describe "an array (T-496)" do
    @array_columns [{"keys", {:list, :string}, false}, {"ns", {:list, {:s, 64}}, false}]
    @array_rows [%{"keys" => ["a", "it's"], "ns" => [1, nil]}, %{"keys" => nil, "ns" => []}]

    test "is typed Array(Nullable(T)), and a list of lists Array(Array(...))" do
      assert Format.type_name({:list, :string}, false) == "Array(Nullable(String))"

      assert Format.type_name({:list, {:list, {:f, 64}}}, false) ==
               "Array(Array(Nullable(Float64)))"
    end

    test "is a JSON array, with a NULL one empty" do
      assert encode(:json_each_row, @array_columns, @array_rows) ==
               ~s|{"keys":["a","it's"],"ns":["1",null]}\n{"keys":[],"ns":[]}\n|
    end

    test "is bracketed and quoted in a tab-separated row" do
      assert encode(:tsv, @array_columns, @array_rows) == "['a','it\\'s']\t[1,NULL]\n[]\t[]\n"
    end

    test "is a length and its nullable elements in RowBinary" do
      body =
        encode(:row_binary_with_names_and_types, [{"keys", {:list, :string}, false}], [
          %{"keys" => ["a", nil]}
        ])

      assert String.ends_with?(body, <<2, 0, 1, ?a, 1>>)
    end
  end

  describe "a list of structs is not an array (review of T-496)" do
    @entries {:list, {:struct, [{"key", {:s, 32}}, {"value", {:s, 32}}]}}
    @struct_columns [{"im", @entries, false}]
    @struct_rows [%{"im" => [%{"key" => 1, "value" => 2}]}, %{"im" => nil}]

    test "answers as a String holding JSON, as it did before arrays" do
      assert Format.type_name(@entries, false) == "Nullable(String)"

      assert encode(:json_each_row, @struct_columns, @struct_rows) ==
               ~s|{"im":"[{\\"key\\":1,\\"value\\":2}]"}\n{"im":null}\n|

      assert encode(:tsv, @struct_columns, @struct_rows) == ~s|[{"key":1,"value":2}]\n\\N\n|
    end

    test "inside an array it is a nullable String, in the type and in RowBinary alike" do
      dtype = {:list, @entries}

      assert Format.type_name(dtype, false) == "Array(Nullable(String))"

      body =
        encode(:row_binary_with_names_and_types, [{"a", dtype, false}], [
          %{"a" => [[%{"key" => 1, "value" => 2}]]}
        ])

      json = ~s|[{"key":1,"value":2}]|

      assert String.ends_with?(body, <<1, 0, byte_size(json)>> <> json)
    end
  end

  describe "CSV" do
    test "quotes text, timestamps, maps and JSON; leaves a number bare; NULL is \\N" do
      assert encode(:csv) ==
               ~s|1,"tab\there, it's","2026-09-15 12:00:00.123456","{'host':'a'}","{""n"":1}"\n| <>
                 ~s|\\N,\\N,\\N,"{}",\\N\n|
    end

    test "doubles a quote inside a string, and writes a boolean, a float and a decimal bare" do
      columns = [
        {"s", :string, false},
        {"b", :boolean, false},
        {"f", {:f, 64}, false},
        {"d", {:decimal, 10, 2}, false},
        {"a", {:list, :string}, false}
      ]

      rows = [
        %{
          "s" => ~s|say "hi", ok|,
          "b" => true,
          "f" => 1.5,
          "d" => Decimal.new("12.50"),
          "a" => ["x", "y"]
        }
      ]

      assert encode(:csv, columns, rows) == ~s|"say ""hi"", ok",true,1.5,12.50,"['x','y']"\n|
    end

    test "the headed forms write names, then types, quoted" do
      columns = [{"id", {:s, 64}, false}]

      assert encode(:csv_names, columns, [%{"id" => 1}]) == ~s|"id"\n1\n|
      assert encode(:csv_names_types, columns, [%{"id" => 1}]) == ~s|"id"\n"Nullable(Int64)"\n1\n|
    end

    test "is fetched by ClickHouse's names, with a header flag in its content type" do
      assert Format.fetch("CSVWithNames") == {:ok, :csv_names}
      assert Format.name(:csv) == "CSV"
      assert Format.content_type(:csv) == "text/csv; charset=UTF-8; header=absent"
      assert Format.content_type(:csv_names) == "text/csv; charset=UTF-8; header=present"
    end

    test "date_time: :iso applies" do
      columns = [{"ts", {:naive_datetime, :microsecond}, false}]
      rows = [%{"ts" => ~N[2026-09-15 12:00:00.123456]}]

      assert :csv |> Format.encode(columns, rows, date_time: :iso) |> IO.iodata_to_binary() ==
               ~s|"2026-09-15T12:00:00.123456Z"\n|
    end
  end

  describe "a DateTime64(9) given as integer nanoseconds" do
    @ns_columns [{"ts", {:naive_datetime, :nanosecond}, false}]
    @ns_rows [%{"ts" => 1_789_869_796_123_456_789}, %{"ts" => -1}, %{"ts" => nil}]

    test "is written with all nine digits, in both styles, before the epoch too" do
      assert encode(:json_each_row, @ns_columns, @ns_rows) ==
               ~s|{"ts":"2026-09-20 02:03:16.123456789"}\n{"ts":"1969-12-31 23:59:59.999999999"}\n{"ts":null}\n|

      assert :tsv
             |> Format.encode(@ns_columns, @ns_rows, date_time: :iso)
             |> IO.iodata_to_binary() ==
               "2026-09-20T02:03:16.123456789Z\n1969-12-31T23:59:59.999999999Z\n\\N\n"

      assert encode(:csv, @ns_columns, [hd(@ns_rows)]) == ~s|"2026-09-20 02:03:16.123456789"\n|
    end

    test "is the same number in RowBinary" do
      body = encode(:row_binary_with_names_and_types, @ns_columns, [hd(@ns_rows)])

      assert String.ends_with?(body, <<0, 1_789_869_796_123_456_789::little-signed-64>>)
    end
  end

  describe "non_null: a column that can never be NULL answers its plain type (T-510)" do
    @typed [
      {"n", {:s, 64}, false},
      {"t", {:naive_datetime, :microsecond}, false},
      {"j", :string, true}
    ]
    @typed_rows [%{"n" => 2, "t" => ~N[1970-01-01 00:00:01.000000], "j" => %{"a" => 1}}]

    defp non_null(format, names),
      do: format |> Format.encode(@typed, @typed_rows, non_null: names) |> IO.iodata_to_binary()

    test "in meta and in the types line, and only for the columns named" do
      assert %{"meta" => meta} = :json |> non_null(["t", "j"]) |> JSON.decode!()

      assert Enum.map(meta, & &1["type"]) == [
               "Nullable(Int64)",
               "DateTime64(6)",
               "Nullable(String)"
             ]

      assert [_names, types, _row, ""] =
               :json_compact_each_row_names_types |> non_null(["n", "t"]) |> String.split("\n")

      assert JSON.decode!(types) == ["Int64", "DateTime64(6)", "Nullable(String)"]

      assert [_names, "Int64\tDateTime64(6)\tNullable(String)", _row, ""] =
               :tsv_names_types |> non_null(["n", "t"]) |> String.split("\n")
    end

    test "RowBinary writes such a column without the null marker" do
      assert non_null(:row_binary_with_names_and_types, ["n", "t"]) ==
               IO.iodata_to_binary([
                 3,
                 [1, "n", 1, "t", 1, "j"],
                 [5, "Int64", 13, "DateTime64(6)", 16, "Nullable(String)"],
                 [<<2::little-signed-64>>, <<1_000_000::little-signed-64>>, 0, 7, ~s({"a":1})]
               ])
    end
  end
end
