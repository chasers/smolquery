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
end
