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
