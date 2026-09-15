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
    do: format |> Format.encode(columns, rows, 12) |> IO.iodata_to_binary()

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
end
