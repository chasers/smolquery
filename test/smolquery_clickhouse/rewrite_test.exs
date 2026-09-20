defmodule SmolqueryClickHouse.RewriteTest do
  use ExUnit.Case, async: true

  alias SmolqueryClickHouse.Rewrite

  describe "an alias inside GROUP BY and ORDER BY" do
    test "HyperDX's histogram keeps the alias in its SELECT only" do
      bucket =
        ~s|toStartOfInterval(toDateTime(Timestamp), INTERVAL 1 minute) AS "__hdx_time_bucket"|

      sql =
        ~s[SELECT count(),SeverityText,#{bucket} FROM "default"."otel_logs" WHERE (Timestamp >= 1) ] <>
          "GROUP BY SeverityText,#{bucket} ORDER BY #{bucket} LIMIT 100000"

      bare = "toStartOfInterval(toDateTime(Timestamp), INTERVAL 1 minute)"

      assert Rewrite.call(sql) ==
               ~s[SELECT count(),SeverityText,#{bucket} FROM "default"."otel_logs" WHERE (Timestamp >= 1) ] <>
                 "GROUP BY SeverityText,#{bare} ORDER BY #{bare} LIMIT 100000"
    end

    test "a bare alias, a direction after it, and lowercase keywords" do
      assert Rewrite.call("select a as x from t group by a as x order by a as x desc, b") ==
               "select a as x from t group by a order by a desc, b"
    end

    test "an AS below the clause's depth is not the clause's" do
      sql = "SELECT 1 FROM t GROUP BY CAST(x AS VARCHAR), y ORDER BY CAST(y AS INTEGER) AS z"

      assert Rewrite.call(sql) ==
               "SELECT 1 FROM t GROUP BY CAST(x AS VARCHAR), y ORDER BY CAST(y AS INTEGER)"
    end

    test "the clause ends at HAVING, LIMIT and the subquery's parenthesis" do
      sql =
        "SELECT * FROM (SELECT a AS x FROM t GROUP BY a AS x HAVING count() > 1) AS s " <>
          "JOIN u AS v ON true ORDER BY s.x LIMIT 1"

      assert Rewrite.call(sql) ==
               "SELECT * FROM (SELECT a AS x FROM t GROUP BY a HAVING count() > 1) AS s " <>
                 "JOIN u AS v ON true ORDER BY s.x LIMIT 1"
    end

    test "a window's ORDER BY does not reach the statement's SELECT list" do
      sql = "SELECT sum(x) OVER (PARTITION BY a ORDER BY b) AS running, c AS d FROM t"

      assert Rewrite.call(sql) == sql
    end

    test "the words inside a literal, a quoted name and a comment are not clauses" do
      sql = ~s|SELECT 'GROUP BY a AS x', "ORDER BY a AS x" FROM t -- GROUP BY a AS x|

      assert Rewrite.call(sql) == sql
    end
  end

  describe "CAST(x, 'Type')" do
    test "becomes CAST(x AS type), with the engine's name for the type" do
      assert Rewrite.call(~s|SELECT 1 FROM t WHERE (("Duration" = CAST('250', 'Float64')))|) ==
               ~s|SELECT 1 FROM t WHERE (("Duration" = CAST('250' AS DOUBLE)))|

      assert Rewrite.call("SELECT cast(a + f(b, c) , 'Nullable(UInt32)' )") ==
               "SELECT cast(a + f(b, c) AS UINTEGER)"
    end

    test "the standard form, an unknown type and another function's literal are left alone" do
      for sql <- [
            "SELECT CAST(x AS UInt32)",
            "SELECT CAST(x, 'Array(String)')",
            "SELECT concat(x, 'Float64')",
            "SELECT CAST(x, 'Float64', y)"
          ] do
        assert Rewrite.call(sql) == sql
      end
    end
  end

  test "a type's text never reaches the statement's code unchecked" do
    sql = "SELECT CAST(x, 'Decimal(1)) FROM secrets --') FROM t"

    assert Rewrite.call(sql) == sql
    assert Rewrite.engine_type("Decimal(1)) FROM secrets --") == :error
    assert Rewrite.engine_type("Decimal( 38 , 2 )") == {:ok, "DECIMAL( 38 , 2 )"}
  end

  test "engine_type/1 reads wrapped and parameterized types" do
    assert Rewrite.engine_type("LowCardinality(String)") == {:ok, "VARCHAR"}
    assert Rewrite.engine_type("DateTime64(9)") == {:ok, "TIMESTAMP"}
    assert Rewrite.engine_type("Decimal(38, 2)") == {:ok, "DECIMAL(38, 2)"}
    assert Rewrite.engine_type("Tuple(String)") == :error
  end
end
