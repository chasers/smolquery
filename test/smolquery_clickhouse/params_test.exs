defmodule SmolqueryClickHouse.ParamsTest do
  use ExUnit.Case, async: true

  alias SmolqueryClickHouse.Params

  test "fills the statement HyperDX sends for its results table" do
    sql =
      "SELECT Timestamp,Body FROM {HYPERDX_PARAM_1:Identifier}.{HYPERDX_PARAM_2:Identifier} " <>
        "WHERE (Timestamp >= fromUnixTimestamp64Milli({HYPERDX_PARAM_3:Int64}) AND " <>
        "Timestamp <= fromUnixTimestamp64Milli({HYPERDX_PARAM_4:Int64})) " <>
        "ORDER BY Timestamp DESC LIMIT {HYPERDX_PARAM_5:Int32} OFFSET {HYPERDX_PARAM_6:Int32}"

    params = %{
      "param_HYPERDX_PARAM_1" => "default",
      "param_HYPERDX_PARAM_2" => "otel_logs",
      "param_HYPERDX_PARAM_3" => "1789000000000",
      "param_HYPERDX_PARAM_4" => "1789000900000",
      "param_HYPERDX_PARAM_5" => "200",
      "param_HYPERDX_PARAM_6" => "0"
    }

    assert Params.substitute(sql, params) ==
             {:ok,
              ~s|SELECT Timestamp,Body FROM "default"."otel_logs" | <>
                "WHERE (Timestamp >= fromUnixTimestamp64Milli(1789000000000) AND " <>
                "Timestamp <= fromUnixTimestamp64Milli(1789000900000)) " <>
                "ORDER BY Timestamp DESC LIMIT 200 OFFSET 0"}
  end

  test "one parameter fills every placeholder that names it" do
    assert Params.substitute("SELECT {a:Int32} + {a:Int32}, { a : Int32 }", %{"param_a" => "2"}) ==
             {:ok, "SELECT 2 + 2, 2"}
  end

  test "a String is a literal, read from ClickHouse's escaped form" do
    assert Params.substitute("SELECT {s:String}", %{"param_s" => ~S|it's a\tb|}) ==
             {:ok, "SELECT 'it''s a\tb'"}
  end

  test "an Identifier is quoted, whatever it holds" do
    assert Params.substitute("SELECT {c:Identifier}", %{"param_c" => ~s|a"; DROP|}) ==
             {:ok, ~s|SELECT "a""; DROP"|}
  end

  test "each type writes its own literal" do
    params = %{
      "param_f" => "1.5e3",
      "param_b" => "true",
      "param_d" => "2026-09-19",
      "param_t" => "2026-09-19 10:11:12.123",
      "param_n" => "\\N",
      "param_l" => "x"
    }

    sql =
      "SELECT {f:Float64}, {b:Bool}, {d:Date}, {t:DateTime64(3)}, {n:Nullable(Int64)}, {l:LowCardinality(String)}"

    assert Params.substitute(sql, params) ==
             {:ok,
              "SELECT 1.5e3, TRUE, CAST('2026-09-19' AS DATE), " <>
                "CAST('2026-09-19 10:11:12.123' AS TIMESTAMP), NULL, 'x'"}
  end

  test "a placeholder in a literal, a quoted name or a comment stays as written" do
    sql = ~S|SELECT '{a:Int32}', "{a:Int32}", `{a:Int32}` -- {a:Int32}|

    assert Params.substitute(sql, %{}) == {:ok, sql}
  end

  test "a struct literal is not a placeholder" do
    sql = "SELECT {a: 1}, {'b': 2}, {c: d}"

    assert Params.substitute(sql, %{}) == {:ok, sql}
  end

  test "a placeholder the request does not fill is code 456" do
    assert {:error, {400, 456, "UNKNOWN_QUERY_PARAMETER", message, nil}} =
             Params.substitute("SELECT {a:Int32}", %{"a" => "1"})

    assert message =~ "param_a"
  end

  test "a value its type does not take is code 36, and is never written into the statement" do
    for {type, value} <- [
          {"Int32", "1; DROP TABLE t"},
          {"Int64", "1.5"},
          {"Float64", "nan()"},
          {"Bool", "maybe"}
        ] do
      assert {:error, {400, 36, "BAD_ARGUMENTS", _message, nil}} =
               Params.substitute("SELECT {a:#{type}}", %{"param_a" => value})
    end
  end

  test "a type with no literal here is code 36" do
    assert {:error, {400, 36, "BAD_ARGUMENTS", message, nil}} =
             Params.substitute("SELECT {a:Array(String)}", %{"param_a" => "['x']"})

    assert message =~ "Array(String)"
  end
end
