defmodule Smolquery.QueryService.ClickHouseFunctionsTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.ClickHouseFunctions

  setup_all do
    engine = :"clickhouse_functions_#{:erlang.unique_integer([:positive])}"
    start_supervised!({Engine, name: engine})

    Enum.each(ClickHouseFunctions.statements(), &Engine.query!(engine, &1))

    Engine.query!(
      engine,
      "CREATE TABLE logs AS SELECT TIMESTAMP '2026-09-19 10:11:29.123456' + INTERVAL (i) SECOND AS ts, " <>
        "CAST(TIMESTAMP '2026-09-19 10:11:29.123456' AS TIMESTAMP_NS) AS tns, " <>
        "'svc' || (i % 3) AS svc, 'an Error happened, id=' || i AS body, " <>
        "MAP {'k8s.pod.name': 'p' || (i % 2)} AS attrs, i AS n FROM range(200) r(i)"
    )

    %{engine: engine}
  end

  defp rows(engine, sql) do
    {:ok, frame} = Engine.frame(engine, sql)

    Frame.to_rows(frame)
  end

  defp one(engine, expression),
    do: engine |> rows("SELECT #{expression} AS v") |> hd() |> Map.fetch!("v")

  test "every statement defines, twice over, and the names are unique", %{engine: engine} do
    Enum.each(ClickHouseFunctions.statements(), &Engine.query!(engine, &1))

    names = ClickHouseFunctions.names()

    assert names == Enum.uniq(names)
    assert "toStartOfInterval" in names
  end

  describe "HyperDX's Search page" do
    test "the time filter and the histogram's bucket, over both timestamp types", %{
      engine: engine
    } do
      sql =
        "SELECT count(), svc, toStartOfInterval(toDateTime(ts), INTERVAL 1 minute) AS bucket " <>
          "FROM logs WHERE (ts >= fromUnixTimestamp64Milli(1789812689000) AND " <>
          "tns <= fromUnixTimestamp64Milli(1889812689000)) GROUP BY svc, bucket ORDER BY bucket, svc LIMIT 1"

      assert rows(engine, sql) == [
               %{
                 "bucket" => ~N[2026-09-19 10:11:00.000000],
                 "count_star()" => 11,
                 "svc" => "svc0"
               }
             ]

      assert rows(
               engine,
               "SELECT toStartOfInterval(tns, INTERVAL 15 second) AS v FROM logs LIMIT 1"
             ) ==
               [%{"v" => ~N[2026-09-19 10:11:15.000000]}]
    end

    test "the primary-key filter, written with no space around its AND", %{engine: engine} do
      sql =
        "SELECT count() AS c FROM logs WHERE (ts >= fromUnixTimestamp64Milli(0))AND" <>
          "(toStartOfFiveMinutes(ts) >= toStartOfFiveMinutes(fromUnixTimestamp64Milli(0)))"

      assert rows(engine, sql) == [%{"c" => 200}]
    end

    test "a term, a field, an existence and a map-key filter", %{engine: engine} do
      for {predicate, count} <- [
            {"((hasToken(lower(body), lower('ERROR'))))", 200},
            {"((hasToken(lower(body), lower('err'))))", 0},
            {"((NOT hasToken(lower(body), lower('error'))))", 0},
            {"((svc ILIKE '%VC1%'))", 67},
            {"notEmpty(svc) = 1", 200},
            {"notEmpty(svc) != 1", 0},
            {"((attrs['k8s.pod.name'] = 'p1' AND indexHint(mapContains(attrs, 'k8s.pod.name'))))",
             100},
            {"mapContains(attrs, 'absent')", 0}
          ] do
        assert rows(engine, "SELECT count() AS c FROM logs WHERE #{predicate}") == [
                 %{"c" => count}
               ],
               predicate
      end
    end

    test "a row click finds its row by a nanosecond timestamp", %{engine: engine} do
      sql =
        "SELECT count() AS c FROM logs WHERE tns = parseDateTime64BestEffort('2026-09-19T10:11:29.123456000Z', 9)"

      assert rows(engine, sql) == [%{"c" => 200}]
    end
  end

  describe "buckets" do
    test "count from 1970-01-01, as ClickHouse's do", %{engine: engine} do
      assert one(engine, "toStartOfInterval(TIMESTAMP '2026-09-19 10:11:29', INTERVAL 7 day)") ==
               ~N[2026-09-17 00:00:00.000000]

      assert one(engine, "toStartOfFifteenMinutes(TIMESTAMP '2026-09-19 10:11:29')") ==
               ~N[2026-09-19 10:00:00.000000]

      assert one(engine, "toStartOfHour(TIMESTAMP '2026-09-19 10:11:29')") ==
               ~N[2026-09-19 10:00:00.000000]

      assert one(engine, "toStartOfMonth(TIMESTAMP '2026-09-19 10:11:29')") == ~D[2026-09-01]
    end
  end

  describe "conversions" do
    test "epoch numbers to timestamps and back", %{engine: engine} do
      assert one(engine, "fromUnixTimestamp64Milli(1789812689123)") ==
               ~N[2026-09-19 10:11:29.123000]

      assert one(engine, "fromUnixTimestamp(1789812689)") == ~N[2026-09-19 10:11:29.000000]

      assert one(engine, "toUnixTimestamp64Milli(TIMESTAMP '2026-09-19 10:11:29.123')") ==
               1_789_812_689_123

      assert one(engine, "toUnixTimestamp(TIMESTAMP '2026-09-19 10:11:29.9')") == 1_789_812_689

      assert one(engine, "toDateTime(TIMESTAMP '2026-09-19 10:11:29.9')") ==
               ~N[2026-09-19 10:11:29.000000]

      assert one(engine, "toDate('2026-09-19')") == ~D[2026-09-19]
      assert one(engine, "now64(3) IS NOT NULL AND now64() IS NOT NULL")
    end

    test "casts answer zero, NULL or the value", %{engine: engine} do
      assert one(engine, "toFloat64OrDefault(toString(1.5))") == 1.5
      assert one(engine, "toFloat64OrDefault('abc')") == 0.0
      assert one(engine, "toUInt64OrZero('abc')") == 0
      assert one(engine, "toInt64OrNull('abc')") == nil
      assert one(engine, "toInt64('42')") == 42
      assert one(engine, "toString(42)") == "42"
    end
  end

  describe "aggregates and predicates" do
    test "the -If combinators filter, and uniq counts distinct values", %{engine: engine} do
      assert rows(
               engine,
               "SELECT sumIf(n, svc = 'svc1') AS s, avgIf(n, n < 2) AS a, uniq(svc) AS u FROM logs"
             ) ==
               [%{"s" => Decimal.new("6700"), "a" => 0.5, "u" => 3}]
    end

    test "string and array predicates", %{engine: engine} do
      assert one(
               engine,
               "match('abc', '^a.c$') AND startsWith('abc', 'ab') AND endsWith('abc', 'bc')"
             )

      assert one(engine, "has(['a', 'b'], 'b')")
      assert one(engine, "empty('')") == 1
      assert one(engine, "mapKeys(MAP {'a': 'b'})") == ["a"]
    end
  end

  describe "what HyperDX's filters sidebar asks (T-496)" do
    test "map keys, as the edge writes groupUniqArrayArray(1000)(keys)", %{engine: engine} do
      sql =
        "WITH sampledKeys as (SELECT getSubcolumn(attrs, 'keys') AS keys FROM logs LIMIT 3000000) " <>
          "SELECT groupUniqArrayArray(keys, 1000) as keysArr FROM sampledKeys"

      assert rows(engine, sql) == [%{"keysArr" => ["k8s.pod.name"]}]
    end

    test "a field's values, capped", %{engine: engine} do
      assert [%{"v" => values}] = rows(engine, "SELECT groupUniqArray(svc, 2) AS v FROM logs")
      assert Enum.count(values) == 2

      assert [%{"v" => all}] = rows(engine, "SELECT groupUniqArray(svc) AS v FROM logs")
      assert Enum.sort(all) == ["svc0", "svc1", "svc2"]

      assert rows(engine, "SELECT groupUniqArrayIf(svc, n < 1, 5) AS v FROM logs") == [
               %{"v" => ["svc0"]}
             ]

      assert rows(engine, "SELECT groupArray(n, 3) AS v FROM logs WHERE n < 10") == [
               %{"v" => [0, 1, 2]}
             ]
    end

    test "null checks and a conditional quantile", %{engine: engine} do
      assert one(engine, "clickhouse_isNull(NULL) AND clickhouse_isNotNull(1)")
      assert rows(engine, "SELECT quantileIf(n, n < 11, 0.5) AS q FROM logs") == [%{"q" => 5.0}]
      assert one(engine, "getSubcolumn(MAP {'a': 'b'}, 'values')") == ["b"]
      assert one(engine, "lowCardinalityKeys('x')") == "x"
    end
  end

  describe "statements_for/1" do
    test "answers the definitions of the functions a statement names, and no others" do
      sql =
        "SELECT count(),SeverityText,toStartOfInterval(toDateTime(Timestamp), INTERVAL 1 minute) AS b " <>
          "FROM t WHERE Timestamp >= fromUnixTimestamp64Milli(1) AND Timestamp <= fromUnixTimestamp64Milli(2)"

      assert [from_unix, to_date_time, to_start] = ClickHouseFunctions.statements_for(sql)

      assert from_unix =~ "MACRO fromUnixTimestamp64Milli(x)"
      assert to_date_time =~ "MACRO toDateTime(x)"
      assert to_start =~ "MACRO toStartOfInterval(x, i)"
    end

    test "a statement in the engine's own dialect defines nothing" do
      assert ClickHouseFunctions.statements_for(
               "SELECT count(*), date_trunc('hour', ts) FROM logs GROUP BY 2"
             ) ==
               []
    end

    test "reads a name in any case, as the engine resolves it, and only before a parenthesis" do
      assert [statement] =
               ClickHouseFunctions.statements_for("SELECT tofloat64ordefault(tostring(x))") --
                 ClickHouseFunctions.statements_for("SELECT toString(x)")

      assert statement =~ "toFloat64OrDefault"

      assert ClickHouseFunctions.statements_for("SELECT has, t.match, \"uniq\" FROM t") == []
      assert ClickHouseFunctions.statements_for("SELECT my_has(x), t.uniq(x)") == []
    end

    test "every definition it can answer is one of statements/0", %{engine: engine} do
      all = ClickHouseFunctions.statements()

      for name <- ClickHouseFunctions.names() do
        assert [statement] = ClickHouseFunctions.statements_for("SELECT #{name}(x)")
        assert statement in all
      end

      Enum.each(
        ClickHouseFunctions.statements_for("SELECT toDate(x), uniq(y)"),
        &Engine.query!(engine, &1)
      )
    end
  end

  describe "what HyperDX's row click calls" do
    test "the hash of a long string matches the MD5 a client computed", %{engine: engine} do
      expected = :crypto.hash(:md5, String.duplicate("x", 1000)) |> Base.encode16(case: :lower)

      assert one(engine, "lower(hex(clickhouse_MD5(leftUTF8(repeat('x', 1200), 1000))))") ==
               expected
    end

    test "toJSONString is compact JSON text", %{engine: engine} do
      assert one(engine, "toJSONString({'a': 1, 'b': 'x'})") == ~s|{"a":1,"b":"x"}|
    end
  end

  describe "every macro is stable (T-504)" do
    test "stable?/1 knows these macros, in any case, and no other name" do
      assert ClickHouseFunctions.stable?("toStartOfInterval")
      assert ClickHouseFunctions.stable?("fromunixtimestamp64milli")
      refute ClickHouseFunctions.stable?("random")
      refute ClickHouseFunctions.stable?("coin")
    end

    test "each body calls only functions the engine's catalog reports consistent", %{
      engine: engine
    } do
      defined = MapSet.new(ClickHouseFunctions.names(), &String.downcase/1)

      called =
        engine
        |> rows(
          "SELECT macro_definition AS body FROM duckdb_functions() WHERE function_type = 'macro' " <>
            "AND NOT internal AND lower(function_name) NOT IN (#{volatile_names()})"
        )
        |> Enum.flat_map(fn %{"body" => body} ->
          Regex.scan(~r/([A-Za-z_][A-Za-z0-9_]*)\s*\(/, body, capture: :all_but_first)
        end)
        |> Enum.map(fn [name] -> String.downcase(name) end)
        |> Enum.uniq()
        |> Enum.reject(&MapSet.member?(defined, &1))

      assert "time_bucket" in called

      names = Enum.map_join(called, ", ", &"'#{&1}'")

      unstable =
        rows(
          engine,
          "SELECT DISTINCT function_name AS name, function_type AS kind, stability FROM duckdb_functions() " <>
            "WHERE lower(function_name) IN (#{names}) AND function_type <> 'macro' " <>
            "AND coalesce(stability, '') NOT IN ('CONSISTENT', 'CONSISTENT_WITHIN_QUERY')"
        )

      assert Enum.sort_by(unstable, & &1["name"]) == [
               %{"name" => "json_tree", "kind" => "table", "stability" => nil},
               %{"name" => "unnest", "kind" => "table", "stability" => nil}
             ],
             "the catalog rates no table function, so json_tree and unnest have no stability to " <>
               "read; each is a pure function of what it is given, which is what " <>
               "JSONDynamicPathsWithTypes and groupUniqArrayMap lean on (T-521). Any other name " <>
               "here is a macro calling something unrated."
    end

    test "rand and its kin are macros stable?/1 does not vouch for, and each really is volatile (T-526)",
         %{engine: engine} do
      assert ClickHouseFunctions.volatile() == ~w(rand rand32 rand64 randCanonical)

      for name <- ClickHouseFunctions.volatile() do
        refute ClickHouseFunctions.stable?(name)
        refute name in ClickHouseFunctions.names()
        assert [_definition] = ClickHouseFunctions.statements_for("SELECT #{name}()")
      end

      called =
        engine
        |> rows(
          "SELECT macro_definition AS body FROM duckdb_functions() WHERE function_type = 'macro' " <>
            "AND lower(function_name) IN (#{volatile_names()})"
        )
        |> Enum.map(& &1["body"])

      assert [_rand, _rand32, _rand64, _canonical] = called
      assert Enum.all?(called, &(&1 =~ "random()"))

      assert [%{"a" => a, "b" => b, "c" => c}] =
               rows(engine, "SELECT rand() AS a, rand() AS b, randCanonical() AS c")

      assert is_integer(a) and a >= 0 and a < 4_294_967_296
      assert a != b
      assert c >= 0.0 and c < 1.0
    end

    test "cityHash64 takes one to four arguments and is the same for the same input (T-526)", %{
      engine: engine
    } do
      assert [%{"same" => true, "one" => one, "four" => four}] =
               rows(
                 engine,
                 "SELECT cityHash64('a', 1) = cityHash64('a', 1) AS same, cityHash64('a') AS one, " <>
                   "cityHash64('a', 1, 2, 3) AS four"
               )

      assert is_integer(one) and is_integer(four)
      assert ClickHouseFunctions.stable?("cityHash64")
    end

    test "no macro shares its name with a function of the engine's own (review of T-504)" do
      engine = :"clickhouse_functions_bare_#{:erlang.unique_integer([:positive])}"
      start_supervised!({Engine, name: engine}, id: engine)

      names =
        Enum.map_join(
          ClickHouseFunctions.names() ++ ClickHouseFunctions.volatile(),
          ", ",
          &"'#{String.downcase(&1)}'"
        )

      builtins =
        rows(
          engine,
          "SELECT DISTINCT function_name AS name FROM duckdb_functions() WHERE lower(function_name) IN (#{names})"
        )

      assert builtins == []
    end

    defp volatile_names,
      do: Enum.map_join(ClickHouseFunctions.volatile(), ", ", &"'#{String.downcase(&1)}'")

    test "the check would catch a macro over a volatile function", %{engine: engine} do
      assert [%{"stability" => "VOLATILE"} | _more] =
               rows(
                 engine,
                 "SELECT stability FROM duckdb_functions() WHERE function_name = 'random'"
               )
    end
  end
end
