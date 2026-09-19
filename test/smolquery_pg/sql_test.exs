defmodule SmolqueryPg.SqlTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Sql

  test "tokens concatenate back to the input and classify each region" do
    sql = ~s|SELECT 'a;''b' AS "c;d" -- e;\n/* f; /* g */ */ $$h;$$ $1|

    tokens = Sql.tokens(sql)

    assert Enum.map_join(tokens, "", &elem(&1, 1)) == sql

    assert Enum.map(tokens, &elem(&1, 0)) ==
             [:code, :string, :code, :quoted, :code, :comment, :comment, :code, :dollar, :code]
  end

  test "map_code touches code only" do
    sql = ~s|SELECT $1, '$1', "$1" /* $1 */|

    assert Sql.map_code(sql, &String.replace(&1, "$1", "X")) == ~s|SELECT X, '$1', "$1" /* $1 */|
  end

  describe "dialect: :clickhouse" do
    test "a backquoted identifier is a token, and a backslash escapes inside quotes" do
      sql = ~S|SELECT `a``b;`, 'it\'s; -- here', "c\"d" FROM t -- `e|

      tokens = Sql.tokens(sql, dialect: :clickhouse)

      assert Enum.map_join(tokens, "", &elem(&1, 1)) == sql

      assert tokens == [
               {:code, "SELECT "},
               {:backquoted, "`a``b;`"},
               {:code, ", "},
               {:string, ~S|'it\'s; -- here'|},
               {:code, ", "},
               {:quoted, ~S|"c\"d"|},
               {:code, " FROM t "},
               {:comment, "-- `e"}
             ]
    end

    test "without the option a backtick is code and a backslash is a character" do
      assert Sql.tokens(~S|SELECT `a`, 'b\'|) == [{:code, "SELECT `a`, "}, {:string, ~S|'b\'|}]
    end

    test "map_code takes the option" do
      assert Sql.map_code("SELECT `x`, x", &String.replace(&1, "x", "y"), dialect: :clickhouse) ==
               "SELECT `x`, y"
    end
  end
end
