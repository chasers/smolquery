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
            "SELECT CAST(x, 'Tuple(String, Int64)')",
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

  describe "a parametric aggregate" do
    test "moves its parameters behind its arguments" do
      assert Rewrite.call("SELECT groupUniqArray(20)(param0) AS param0 FROM sampledData") ==
               "SELECT groupUniqArray(param0, 20) AS param0 FROM sampledData"

      assert Rewrite.call("SELECT groupUniqArrayArray(1000)(keys) as keysArr FROM sampledKeys") ==
               "SELECT groupUniqArrayArray(keys, 1000) as keysArr FROM sampledKeys"
    end

    test "quantile is the engine's interpolating one, and -If keeps its condition" do
      assert Rewrite.call("SELECT quantile(0.95)(toFloat64OrDefault(toString(d))) FROM t") ==
               "SELECT quantile_cont(toFloat64OrDefault(toString(d)), 0.95) FROM t"

      assert Rewrite.call("SELECT quantileIf(0.5)(d, (s = 'a') AND d IS NOT NULL) FROM t") ==
               "SELECT quantileIf(d, (s = 'a') AND d IS NOT NULL, 0.5) FROM t"
    end

    test "rewrites what its arguments hold, too" do
      assert Rewrite.call("SELECT groupUniqArray(5)(CAST(x, 'String')) FROM t") ==
               "SELECT groupUniqArray(CAST(x AS VARCHAR), 5) FROM t"
    end

    test "an ordinary call by the same name, and another function, are left alone" do
      for sql <- [
            "SELECT quantile(x, 0.5) FROM t",
            "SELECT groupUniqArray(x) FROM t",
            "SELECT f(1)(2)"
          ] do
        assert Rewrite.call(sql) == sql
      end
    end
  end

  describe "LIKE with a backslash in its pattern" do
    test "gets the escape character ClickHouse assumes" do
      assert Rewrite.call(~S|SELECT 1 WHERE (lower(Body) LIKE lower('%user\_id%'))|) ==
               ~S|SELECT 1 WHERE (lower(Body) LIKE lower('%user\_id%') ESCAPE '\')|

      assert Rewrite.call(~S|SELECT 1 WHERE a NOT ILIKE '%50\%%' AND b = 1|) ==
               ~S|SELECT 1 WHERE a NOT ILIKE '%50\%%' ESCAPE '\' AND b = 1|
    end

    test "a pattern with no backslash, and one that is not a literal, are left alone" do
      for sql <- [
            "SELECT 1 WHERE a LIKE '%b%'",
            "SELECT 1 WHERE a LIKE lower(b)",
            "SELECT 1 WHERE a LIKE b"
          ] do
        assert Rewrite.call(sql) == sql
      end
    end
  end

  describe "words the engine keeps for itself" do
    test "a bare default database is quoted; a column or a quoted one is not" do
      assert Rewrite.call("SELECT * FROM default.otel_logs") ==
               ~s|SELECT * FROM "default".otel_logs|

      assert Rewrite.call(~s|SELECT t.default, "default".x FROM t|) ==
               ~s|SELECT t.default, "default".x FROM t|

      assert Rewrite.call("SELECT 1 AS x DEFAULT") == "SELECT 1 AS x DEFAULT"
    end

    test "isNull, isNotNull and any are calls the parser can read" do
      assert Rewrite.call(
               "SELECT any(x), isNull(y), isNotNull(z) FROM t WHERE x = ANY (SELECT 1)"
             ) ==
               "SELECT any_value(x), clickhouse_isNull(y), clickhouse_isNotNull(z) FROM t WHERE x = ANY (SELECT 1)"
    end
  end

  test "now() is the clock with no zone, which a timestamp column compares with (T-542)" do
    assert Rewrite.call("SELECT count() FROM t WHERE ts >= now() - INTERVAL 15 MINUTE") ==
             "SELECT count() FROM t WHERE ts >= now64() - INTERVAL 15 MINUTE"

    assert Rewrite.call("SELECT NOW(), 'now()', now FROM t") ==
             "SELECT now64(), 'now()', now FROM t"
  end

  test "a quantified comparison keeps its ANY, with or without a space (review of T-496)" do
    for sql <- [
          "SELECT 1 WHERE x = ANY(SELECT y FROM u)",
          "SELECT 1 WHERE x = any (SELECT y FROM u)",
          "SELECT 1 WHERE x <> ANY([1, 2]) AND y >= ANY(SELECT 1)",
          "SELECT 1 WHERE x IN (1) AND z < ANY(SELECT 2)"
        ] do
      assert Rewrite.call(sql) == sql
    end

    assert Rewrite.call("SELECT any(x) FROM t WHERE y = ANY(SELECT 1)") ==
             "SELECT any_value(x) FROM t WHERE y = ANY(SELECT 1)"
  end

  describe "WITH (expr) AS alias" do
    test "HyperDX's histogram reads a source's aliases it does not select" do
      sql =
        ~s|WITH (ServiceName) AS "service",(SeverityText) AS "level",(ResourceAttributes['k8s.pod.name']) AS "pod" | <>
          ~s|SELECT count(),SeverityText FROM "default"."otel_logs" | <>
          "WHERE (Timestamp >= 1) AND (((service ILIKE '%api%') AND (pod ILIKE '%api-0%'))) GROUP BY SeverityText"

      assert Rewrite.call(sql) ==
               ~s|SELECT count(),SeverityText FROM "default"."otel_logs" | <>
                 "WHERE (Timestamp >= 1) AND ((((ServiceName) ILIKE '%api%') AND " <>
                 "((ResourceAttributes['k8s.pod.name']) ILIKE '%api-0%'))) GROUP BY SeverityText"
    end

    test "an alias is not replaced where a SELECT defines it, after a dot, or as a call" do
      sql = "WITH (a + 1) AS x SELECT a as x, t.x, x(1), x FROM t ORDER BY x"

      assert Rewrite.call(sql) == "SELECT a as x, t.x, x(1), (a + 1) FROM t ORDER BY (a + 1)"
    end

    test "a common table expression beside it keeps the WITH" do
      sql = "WITH (a) AS x, s AS (SELECT a, b FROM t WHERE b IN (1, 2)) SELECT x FROM s"

      assert Rewrite.call(sql) ==
               "WITH s AS (SELECT a, b FROM t WHERE b IN (1, 2)) SELECT (a) FROM s"
    end

    test "a subquery that defines the name for itself keeps it, inside and out (review of T-496)" do
      sql =
        "WITH (ServiceName) AS service SELECT service, n FROM " <>
          "(SELECT ServiceName AS service, count() AS n FROM t GROUP BY service) ORDER BY service"

      assert Rewrite.call(sql) ==
               "SELECT service, n FROM " <>
                 "(SELECT ServiceName AS service, count() AS n FROM t GROUP BY service) ORDER BY service"
    end

    test "a name inside a subquery is the subquery's, not the alias (review of T-496)" do
      assert Rewrite.call("WITH (a) AS x SELECT x FROM t WHERE k IN (SELECT x FROM u)") ==
               "SELECT (a) FROM t WHERE k IN (SELECT x FROM u)"
    end

    test "a later alias may use an earlier one (review of T-496)" do
      assert Rewrite.call("WITH (a + 1) AS b, (b * 2) AS c SELECT c FROM t") ==
               "SELECT ((a + 1) * 2) FROM t"
    end

    test "an alias defined without AS is a definition, not a call (review of T-496)" do
      assert Rewrite.call(
               "WITH (ServiceName) AS service SELECT ServiceName service, count() n FROM t WHERE service = 'a'"
             ) ==
               "SELECT ServiceName service, count() n FROM t WHERE (ServiceName) = 'a'"
    end

    test "an expression in an alias is rewritten like any other" do
      assert Rewrite.call("WITH (CAST(d, 'Float64')) AS n SELECT n FROM t") ==
               "SELECT (CAST(d AS DOUBLE)) FROM t"
    end

    test "a statement with only table expressions, or none, is left alone" do
      for sql <- [
            "WITH s AS (SELECT 1 AS a) SELECT a FROM s",
            "WITH RECURSIVE r(n) AS (SELECT 1) SELECT n FROM r",
            "SELECT 1 AS x"
          ] do
        assert Rewrite.call(sql) == sql
      end
    end
  end

  describe "what HyperDX writes on a row click" do
    test "JSONExtract with a type and no path is a cast of the JSON" do
      assert Rewrite.call(
               ~S|SELECT 1 WHERE LogAttributes=JSONExtract('{"http.status":"500"}', 'Map(String, String)')|
             ) ==
               ~S|SELECT 1 WHERE LogAttributes=CAST(CAST('{"http.status":"500"}' AS JSON) AS MAP(VARCHAR, VARCHAR))|

      assert Rewrite.call(~S|SELECT JSONExtract(f(a, b), 'Array(Nullable(String))')|) ==
               "SELECT CAST(CAST(f(a, b) AS JSON) AS VARCHAR[])"
    end

    test "a path form, and a type with no name here, are left for the engine" do
      for sql <- [
            "SELECT JSONExtract(j, 'a', 'String')",
            "SELECT JSONExtract(j, 'Tuple(String)')",
            "SELECT JSONExtract(j)"
          ] do
        assert Rewrite.call(sql) == sql
      end
    end

    test "MD5 is the macro that answers bytes, as ClickHouse's does" do
      assert Rewrite.call("SELECT 1 WHERE lower(hex(MD5(leftUTF8(Body, 1000))))='abc'") ==
               "SELECT 1 WHERE lower(hex(clickhouse_MD5(leftUTF8(Body, 1000))))='abc'"
    end

    test "engine_type/1 reads maps and arrays, nested" do
      assert Rewrite.engine_type("Map(LowCardinality(String), String)") ==
               {:ok, "MAP(VARCHAR, VARCHAR)"}

      assert Rewrite.engine_type("Array(Map(String, Nullable(Int64)))") ==
               {:ok, "MAP(VARCHAR, BIGINT)[]"}

      assert Rewrite.engine_type("Nullable(Decimal(38, 2))") == {:ok, "DECIMAL(38, 2)"}
      assert Rewrite.engine_type("Map(String)") == :error
    end
  end
end
