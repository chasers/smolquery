defmodule SmolqueryPg.TypesTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Types

  test "describes the smolquery column types as Postgres types" do
    assert Types.describe({:s, 64}, false) == {20, 8, -1}
    assert Types.describe({:s, 32}, false) == {20, 8, -1}
    assert Types.describe({:f, 64}, false) == {701, 8, -1}
    assert Types.describe(:string, false) == {25, -1, -1}
    assert Types.describe(:boolean, false) == {16, 1, -1}
    assert Types.describe({:naive_datetime, :microsecond}, false) == {1114, 8, -1}
    assert Types.describe({:datetime, :microsecond, "Etc/UTC"}, false) == {1184, 8, -1}
    assert Types.describe(:date, false) == {1082, 4, -1}
    assert Types.describe({:decimal, 38, 2}, false) == {1700, -1, 38 * 65_536 + 2 + 4}

    assert Types.describe({:list, {:struct, [{"key", :string}, {"value", :string}]}}, false) ==
             {3802, -1, -1}

    assert Types.describe({:list, {:s, 64}}, false) == {3802, -1, -1}
    assert Types.describe(:string, true) == {3802, -1, -1}
  end

  test "encodes values in the text forms Postgres reads" do
    assert Types.encode_text(:boolean, false, true) == "t"
    assert Types.encode_text(:boolean, false, false) == "f"
    assert Types.encode_text({:s, 64}, false, -42) == "-42"
    assert Types.encode_text({:f, 64}, false, 1.5) == "1.5"
    assert Types.encode_text({:f, 64}, false, :nan) == "NaN"
    assert Types.encode_text({:f, 64}, false, :infinity) == "Infinity"
    assert Types.encode_text(:string, false, "text") == "text"

    assert Types.encode_text({:naive_datetime, :microsecond}, false, ~N[2026-08-01 10:00:00.5]) ==
             "2026-08-01 10:00:00.5"

    assert IO.iodata_to_binary(
             Types.encode_text(
               {:datetime, :microsecond, "Etc/UTC"},
               false,
               ~U[2026-08-01 10:00:00Z]
             )
           ) == "2026-08-01 10:00:00+00"

    assert Types.encode_text(:date, false, ~D[2026-08-01]) == "2026-08-01"
    assert Types.encode_text({:decimal, 38, 2}, false, Decimal.new("12.50")) == "12.50"
    assert IO.iodata_to_binary(Types.encode_text(:binary, false, <<1, 255>>)) == "\\x01ff"
    assert Types.encode_text(:string, true, %{"a" => [1, 2]}) == ~s|{"a":[1,2]}|
    assert Types.encode_text({:list, {:s, 64}}, false, [1, 2]) == "[1,2]"
    assert Types.encode_text(:boolean, false, nil) == nil
    assert Types.encode_text(:string, true, nil) == nil
  end

  test "a stored map arrives as a JSON object" do
    map_dtype = {:list, {:struct, [{"key", :string}, {"value", :string}]}}

    assert Types.encode_text(map_dtype, false, %{"host" => "a"}) == ~s|{"host":"a"}|
  end
end

defmodule SmolqueryPg.TypesBinaryTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Types

  test "encodes the scalar types in their Postgres binary forms" do
    assert IO.iodata_to_binary(Types.encode(:boolean, false, true, 1)) == <<1>>
    assert IO.iodata_to_binary(Types.encode({:s, 64}, false, -7, 1)) == <<-7::64-signed>>
    assert IO.iodata_to_binary(Types.encode({:f, 64}, false, 2.5, 1)) == <<2.5::float-64>>

    assert IO.iodata_to_binary(Types.encode({:f, 64}, false, :nan, 1)) ==
             <<0x7FF8000000000000::64>>

    assert IO.iodata_to_binary(Types.encode(:string, false, "x", 1)) == "x"
    assert IO.iodata_to_binary(Types.encode(:date, false, ~D[2000-01-02], 1)) == <<1::32-signed>>

    assert IO.iodata_to_binary(
             Types.encode({:naive_datetime, :microsecond}, false, ~N[2000-01-01 00:00:01], 1)
           ) == <<1_000_000::64-signed>>

    assert IO.iodata_to_binary(Types.encode(:string, true, %{"a" => 1}, 1)) == <<1, ~s|{"a":1}|>>
    assert Types.encode(:string, false, nil, 1) == nil
  end

  test "a numeric round-trips through the binary form" do
    for text <- ["123456.78", "0", "-1.5", "10000", "0.0001", "12.50"] do
      decimal = Decimal.new(text)
      encoded = IO.iodata_to_binary(Types.encode({:decimal, 38, 2}, false, decimal, 1))

      assert {:ok, {:numeric, decoded}} = Types.decode_param(1700, 1, encoded)
      assert Decimal.equal?(Decimal.new(decoded), decimal), "#{text} came back as #{decoded}"
    end
  end

  test "the binary timestamp infinities decode to literals DuckDB accepts" do
    assert {:ok, {:timestamp, :infinity}} =
             Types.decode_param(1114, 1, <<0x7FFFFFFFFFFFFFFF::64-signed>>)

    assert {:ok, {:timestamp, :neg_infinity}} =
             Types.decode_param(1184, 1, <<-0x8000000000000000::64-signed>>)

    assert {:error, {:timestamp_out_of_range, _us}} =
             Types.decode_param(1114, 1, <<0x7FFFFFFFFFFFFFFE::64-signed>>)

    assert {:ok, [%NaiveDateTime{year: 294_247}, %NaiveDateTime{year: -290_308}]} =
             SmolqueryPg.Params.values([
               {1114, 1, <<0x7FFFFFFFFFFFFFFF::64-signed>>},
               {1114, 1, <<-0x8000000000000000::64-signed>>}
             ])
  end

  test "describes DuckDB type names as Postgres types" do
    assert Types.describe({:duckdb, "BIGINT"}, false) == {20, 8, -1}
    assert Types.describe({:duckdb, "VARCHAR"}, false) == {25, -1, -1}
    assert Types.describe({:duckdb, "DECIMAL(38,2)"}, false) == {1700, -1, 38 * 65_536 + 2 + 4}
    assert Types.describe({:duckdb, "TIMESTAMP"}, false) == {1114, 8, -1}
    assert Types.describe({:duckdb, "MAP(VARCHAR, VARCHAR)"}, false) == {3802, -1, -1}
    assert Types.describe({:duckdb, "INTEGER[]"}, false) == {3802, -1, -1}
    assert Types.describe({:duckdb, "VARIANT"}, false) == {3802, -1, -1}
  end
end
