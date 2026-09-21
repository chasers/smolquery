defmodule Smolquery.QueryService.PrunerTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.Pruner

  @engine __MODULE__.Parser
  @conn Engine.connection_name(@engine)

  @events {"analytics", "events"}
  @users {"analytics", "users"}

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  defp statement(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    %{"statements" => [statement]} = result |> Result.one!() |> JSON.decode!()

    statement
  end

  defp conjuncts(sql, refs \\ [@events]), do: Pruner.conjuncts(statement(sql), refs)

  describe "conjuncts/2" do
    test "reads comparisons off a single table's WHERE, whichever side the column is on" do
      assert conjuncts("SELECT * FROM analytics.events WHERE id > 5 AND 100 >= id") ==
               %{@events => [{"id", :gt, 5}, {"id", :le, 100}]}
    end

    test "a table the statement reads twice is pruned by neither reference's WHERE (T-533)" do
      for sql <- [
            "SELECT * FROM analytics.events a JOIN analytics.events b ON a.id = b.id + 1 WHERE a.id > 5",
            "SELECT * FROM analytics.events WHERE id > 5 AND id IN (SELECT max(id) FROM analytics.events)",
            "SELECT (SELECT count(*) FROM analytics.events) AS total, id FROM analytics.events WHERE id > 5",
            "WITH all_events AS (SELECT * FROM analytics.events) " <>
              "SELECT * FROM analytics.events e JOIN all_events USING (id) WHERE e.id > 5",
            "SELECT * FROM analytics.events WHERE id > 5 UNION ALL SELECT * FROM events"
          ] do
        assert conjuncts(sql) == %{}, sql
      end
    end

    test "a table function may read any table, so a statement with one prunes nothing (review of T-533)" do
      sql =
        "SELECT * FROM analytics.events WHERE id > 5 " <>
          "UNION ALL SELECT * FROM query_table('analytics.events')"

      assert conjuncts(sql) == %{}

      assert conjuncts("SELECT * FROM analytics.events, range(3) r WHERE id > 5") == %{}
    end

    test "a reference that renames its columns, or samples, resolves nothing (review of T-533)" do
      for sql <- [
            "SELECT * FROM analytics.events AS e(ts, id) WHERE e.id > 5",
            "SELECT * FROM analytics.events AS e(ts, id) WHERE id > 5",
            "SELECT * FROM analytics.events TABLESAMPLE reservoir(100 ROWS) REPEATABLE (42) WHERE id > 5",
            "SELECT * FROM analytics.events WHERE id > 5 USING SAMPLE 10% (bernoulli, 42)"
          ] do
        assert conjuncts(sql) == %{}, sql
      end

      beside =
        "SELECT * FROM analytics.events AS e(ts, id) JOIN analytics.users u ON u.id = e.id " <>
          "WHERE e.id > 5 AND u.id > 7"

      assert conjuncts(beside, [@events, @users]) == %{@users => [{"id", :gt, 7}]}
    end

    test "a second table read once beside one read twice still prunes (T-533)" do
      sql =
        "SELECT * FROM analytics.events a JOIN analytics.events b ON a.id = b.id " <>
          "JOIN analytics.users u ON u.id = a.id WHERE a.id > 5 AND u.id > 7"

      assert conjuncts(sql, [@events, @users]) == %{@users => [{"id", :gt, 7}]}
    end

    test "a WHERE in a CTE, a FROM subquery or a branch of a set operation is read, as HyperDX's sidebar writes it (T-532)" do
      bound = %{@events => [{"id", :gt, 5}]}

      assert conjuncts(
               "WITH sampled AS (SELECT name AS p FROM analytics.events WHERE id > 5 LIMIT 100) " <>
                 "SELECT list(DISTINCT p) FROM sampled"
             ) == bound

      assert conjuncts(
               "WITH a AS (WITH b AS (SELECT * FROM analytics.events WHERE id > 5) SELECT * FROM b) " <>
                 "SELECT count(*) FROM a x JOIN a y USING (id)"
             ) == bound

      assert conjuncts("SELECT count(*) FROM (SELECT * FROM analytics.events WHERE id > 5) e") ==
               bound

      assert conjuncts(
               "SELECT id FROM analytics.events WHERE id > 5 UNION ALL SELECT id FROM analytics.users WHERE id > 7",
               [@events, @users]
             ) == %{@events => [{"id", :gt, 5}], @users => [{"id", :gt, 7}]}
    end

    test "a WHERE that could name an outer column is not read: an expression subquery, a subquery beside another source (T-532)" do
      for sql <- [
            "SELECT * FROM analytics.users u WHERE EXISTS (SELECT 1 FROM analytics.events WHERE id > 5)",
            "SELECT * FROM analytics.users u, (SELECT * FROM analytics.events WHERE id > u.id) e",
            "SELECT (SELECT max(id) FROM analytics.events WHERE id > 5) FROM analytics.users"
          ] do
        assert conjuncts(sql, [@events, @users]) == %{}, sql
      end
    end

    test "a ClickHouse epoch function over an integer is the timestamp the engine makes of it (T-532)" do
      for {function, n} <- [
            {"fromUnixTimestamp", 1_789_954_478},
            {"fromUnixTimestamp64Milli", 1_789_954_478_123},
            {"fromUnixTimestamp64Micro", 1_789_954_478_123_456}
          ] do
        {:ok, macro} =
          Enum.fetch(
            ClickHouseFunctions.statements_for("#{function}(1)"),
            0
          )

        {:ok, _created} = Connection.query(@conn, macro)
        {:ok, result} = Connection.query(@conn, "SELECT #{function}(#{n})")
        engine = Result.one!(result)

        for argument <- ["#{n}", "CAST(#{n} AS BIGINT)"] do
          sql = "SELECT * FROM analytics.events WHERE ts >= #{function}(#{argument})"

          assert conjuncts(sql) == %{@events => [{"ts", :ge, engine}]}, sql
        end
      end
    end

    test "an epoch function over anything but one integer in range bounds nothing (T-532)" do
      for argument <- ["1.5", "id", "now()", "99999999999999999999", "1, 2"] do
        sql = "SELECT * FROM analytics.events WHERE ts >= fromUnixTimestamp64Milli(#{argument})"

        assert conjuncts(sql) == %{}, sql
      end

      assert conjuncts("SELECT * FROM analytics.events WHERE ts >= fromUnixTimestamp64Nano(5)") ==
               %{}
    end

    test "BETWEEN becomes its two bounds" do
      assert conjuncts("SELECT * FROM analytics.events WHERE id BETWEEN 5 AND 9") ==
               %{@events => [{"id", :ge, 5}, {"id", :le, 9}]}
    end

    test "a timestamp literal is typed, not left a string" do
      assert conjuncts(
               "SELECT * FROM analytics.events WHERE ts > TIMESTAMP '2026-07-31 12:00:00'"
             ) ==
               %{@events => [{"ts", :gt, ~N[2026-07-31 12:00:00]}]}
    end

    test "a date literal is typed too" do
      assert conjuncts("SELECT * FROM analytics.events WHERE day >= DATE '2026-07-31'") ==
               %{@events => [{"day", :ge, ~D[2026-07-31]}]}
    end

    test "qualified columns resolve through join aliases" do
      sql = """
      SELECT * FROM analytics.events e JOIN analytics.users u ON u.id = e.user_id
       WHERE e.id > 5 AND u.name = 'ada'
      """

      assert conjuncts(sql, [@events, @users]) ==
               %{@events => [{"id", :gt, 5}], @users => [{"name", :eq, "ada"}]}
    end

    test "an unqualified column in a join resolves to nothing" do
      sql = """
      SELECT * FROM analytics.events e JOIN analytics.users u ON u.id = e.user_id
       WHERE id > 5
      """

      assert conjuncts(sql, [@events, @users]) == %{}
    end

    test "an unqualified column beside a subquery source resolves to nothing" do
      sql = """
      SELECT * FROM analytics.events e, (SELECT 1 AS id) s WHERE id > 5
      """

      assert conjuncts(sql, [@events]) == %{}
    end

    test "an OR prunes nothing, but its AND siblings still count" do
      assert conjuncts("SELECT * FROM analytics.events WHERE (id > 5 OR name = 'x') AND id < 100") ==
               %{@events => [{"id", :lt, 100}]}
    end

    test "a column-to-column comparison prunes nothing" do
      assert conjuncts("SELECT * FROM analytics.events WHERE id > user_id") == %{}
    end

    test "a query with no WHERE prunes nothing" do
      assert conjuncts("SELECT * FROM analytics.events") == %{}
    end

    test "a side of a set operation prunes its own table, and not one both sides read (T-532)" do
      sql = "SELECT id FROM analytics.events WHERE id > 5 UNION ALL SELECT 1"

      assert conjuncts(sql) == %{@events => [{"id", :gt, 5}]}

      both =
        "SELECT id FROM analytics.events WHERE id > 5 UNION ALL SELECT id FROM analytics.events WHERE id < 2"

      assert conjuncts(both) == %{}
    end
  end

  describe "keep?/2" do
    defp entry(stats), do: %{"id" => "01A", "stats" => stats}

    defp int_stats(min, max),
      do: %{"id" => %{"min" => min, "max" => max, "null_count" => 0}}

    test "drops what the bounds rule out, keeps what they cannot" do
      cases = [
        {int_stats(1, 10), {"id", :gt, 10}, false},
        {int_stats(1, 10), {"id", :gt, 9}, true},
        {int_stats(1, 10), {"id", :ge, 11}, false},
        {int_stats(1, 10), {"id", :ge, 10}, true},
        {int_stats(1, 10), {"id", :lt, 1}, false},
        {int_stats(1, 10), {"id", :lt, 2}, true},
        {int_stats(1, 10), {"id", :le, 0}, false},
        {int_stats(1, 10), {"id", :le, 1}, true},
        {int_stats(1, 10), {"id", :eq, 0}, false},
        {int_stats(1, 10), {"id", :eq, 11}, false},
        {int_stats(1, 10), {"id", :eq, 5}, true}
      ]

      for {stats, conjunct, expected} <- cases do
        assert Pruner.keep?(entry(stats), [conjunct]) == expected,
               "#{inspect(conjunct)} against #{inspect(stats)}"
      end
    end

    test "one impossible conjunct is enough to drop an entry" do
      refute Pruner.keep?(entry(int_stats(1, 10)), [{"id", :gt, 0}, {"id", :gt, 100}])
    end

    test "a column without stats keeps the entry" do
      assert Pruner.keep?(entry(%{}), [{"id", :gt, 100}])
    end

    test "a nil bound keeps the entry" do
      stats = %{"id" => %{"min" => nil, "max" => nil, "null_count" => 3}}

      assert Pruner.keep?(entry(stats), [{"id", :gt, 100}])
    end

    test "a type mismatch keeps the entry rather than comparing nonsense" do
      assert Pruner.keep?(entry(int_stats(1, 10)), [{"id", :gt, "100"}])
    end

    test "tagged datetime bounds compare as datetimes" do
      stats = %{
        "ts" => %{
          "min" => %{"type" => "naive_datetime", "value" => "2026-07-01T00:00:00"},
          "max" => %{"type" => "naive_datetime", "value" => "2026-07-15T00:00:00"},
          "null_count" => 0
        }
      }

      refute Pruner.keep?(entry(stats), [{"ts", :gt, ~N[2026-07-31 00:00:00]}])
      assert Pruner.keep?(entry(stats), [{"ts", :gt, ~N[2026-07-10 00:00:00]}])
    end

    test "string bounds compare as strings" do
      stats = %{"name" => %{"min" => "alpha", "max" => "delta", "null_count" => 0}}

      refute Pruner.keep?(entry(stats), [{"name", :eq, "zeta"}])
      assert Pruner.keep?(entry(stats), [{"name", :eq, "beta"}])
    end

    test "resolves a column through ids, so a dropped column's bounds never prune its successor (PL-62)" do
      bounds = %{"min" => 1, "max" => 10, "null_count" => 0}
      old = %{"stats" => %{"ts_int" => bounds}, "field_ids" => %{"id" => 1, "ts_int" => 2}}
      catalog = %{"id" => 1, "ts_int" => 3}

      assert Pruner.keep?(old, [{"ts_int", :gt, 100}], catalog)
      refute Pruner.keep?(old, [{"ts_int", :gt, 100}], %{"id" => 1, "ts_int" => 2})
    end

    test "finds a column's bounds under whatever the file named it" do
      bounds = %{"min" => 1, "max" => 10, "null_count" => 0}
      renamed = %{"stats" => %{"old" => bounds}, "field_ids" => %{"old" => 3}}

      refute Pruner.keep?(renamed, [{"ts_int", :gt, 100}], %{"ts_int" => 3})
    end

    test "without ids on either side, the name is the key, as before" do
      bounds = %{"min" => 1, "max" => 10, "null_count" => 0}
      legacy = %{"stats" => %{"ts_int" => bounds}}

      refute Pruner.keep?(legacy, [{"ts_int", :gt, 100}], %{"ts_int" => 3})
      refute Pruner.keep?(legacy, [{"ts_int", :gt, 100}], nil)
    end
  end
end
