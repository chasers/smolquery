defmodule Smolquery.QueryService.FoldTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.Fold
  alias Smolquery.QueryService.Pruner

  @engine __MODULE__.Engine
  @conn Engine.connection_name(@engine)
  @events {"analytics", "events"}

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    for macro <- ClickHouseFunctions.statements(), do: Engine.query!(@engine, macro)

    :ok
  end

  defp statement(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    %{"statements" => [statement]} = result |> Result.one!() |> JSON.decode!()

    statement
  end

  defp folded(sql, lockdown \\ false) do
    ast = statement(sql)

    {ast, Fold.bounds(@conn, Pruner.unread_bounds(ast, [@events]), lockdown)}
  end

  defp conjuncts(sql) do
    {ast, folded} = folded(sql)

    Pruner.conjuncts(ast, [@events], [], folded)
  end

  describe "bounds/3" do
    test "a bound written as an expression is the value the engine makes of it" do
      for {bound, value} <- [
            {"toDateTime64('2026-09-19 10:11:00.250', 3)", ~N[2026-09-19 10:11:00.250000]},
            {"toDateTime('2026-09-19 10:11:00')", ~N[2026-09-19 10:11:00.000000]},
            {"fromUnixTimestamp64Milli(1789812660000) - INTERVAL 1 HOUR",
             ~N[2026-09-19 09:11:00.000000]},
            {"TIMESTAMP '2026-09-19 10:11:00' + INTERVAL 5 MINUTE",
             ~N[2026-09-19 10:16:00.000000]},
            {"CAST('2026-09-19' AS DATE) + 1", ~D[2026-09-20]},
            {"40 + 2", 42}
          ] do
        sql = "SELECT * FROM analytics.events WHERE ts >= #{bound}"

        assert conjuncts(sql) == %{@events => [{"ts", :ge, value}]}, sql
      end
    end

    test "what the pruner reads by itself is never sent to the engine" do
      for sql <- [
            "SELECT * FROM analytics.events WHERE id > 5 AND ts > TIMESTAMP '2026-01-01 00:00:00'",
            "SELECT * FROM analytics.events WHERE ts >= fromUnixTimestamp64Milli(1789812660000)",
            "SELECT * FROM analytics.events WHERE id BETWEEN 5 AND 9"
          ] do
        assert Pruner.unread_bounds(statement(sql), [@events]) == [], sql
      end
    end

    test "only a side the pruner could use is sent: a WHERE conjunct against a column (review of T-534)" do
      for sql <- [
            "SELECT name, count(*) FROM analytics.events GROUP BY name HAVING count(*) > 5 + 1",
            "SELECT id > 40 + 2 FROM analytics.events",
            "SELECT * FROM analytics.events WHERE ts NOT BETWEEN now() - INTERVAL 1 HOUR AND now()",
            "SELECT * FROM analytics.events WHERE 40 + 2 > 5 + 1",
            "SELECT * FROM analytics.events WHERE id > 40 + 2 OR id < 5 + 1",
            "SELECT * FROM analytics.users u WHERE EXISTS (SELECT 1 FROM analytics.events WHERE id > 40 + 2)"
          ] do
        assert Pruner.unread_bounds(statement(sql), [@events]) == [], sql
      end
    end

    test "an expression that names a column, a subquery or a parameter is not folded" do
      for sql <- [
            "SELECT * FROM analytics.events WHERE ts >= other_ts - INTERVAL 1 HOUR",
            "SELECT * FROM analytics.events WHERE id > (SELECT max(id) FROM analytics.events) - 5",
            "SELECT * FROM analytics.events WHERE id > $1 + 5",
            "SELECT * FROM analytics.events WHERE id > list_sum(list_transform([1, 2], x -> x + 1))"
          ] do
        assert {_ast, %{} = none} = folded(sql)
        assert none == %{}, sql
      end
    end

    test "a side that is volatile, a macro not ours, or cannot be evaluated loses itself and nothing else" do
      Engine.query!(@engine, "CREATE OR REPLACE MACRO theirs(x) AS x + 1")

      for bound <- [
            "random() * 100",
            "theirs(5)",
            "40 + 2 + theirs(1)",
            "CAST('abc' AS INTEGER) + 1",
            "1 // 0 + CAST('x' AS INTEGER)"
          ] do
        sql = "SELECT * FROM analytics.events WHERE id > 40 + 2 AND id < #{bound}"

        assert conjuncts(sql) == %{@events => [{"id", :gt, 42}]}, sql
      end
    end

    test "an expression with names inside it, a struct, is rendered as it was written" do
      sql =
        "SELECT * FROM analytics.events WHERE id > 40 + 2 AND id < struct_extract({'a': 7}, 'a') + 1"

      assert conjuncts(sql) == %{@events => [{"id", :gt, 42}, {"id", :lt, 8}]}
    end

    test "a bound that reads the clock is a lower bound and never an upper one" do
      lower = conjuncts("SELECT * FROM analytics.events WHERE ts >= now() - INTERVAL 15 MINUTE")

      assert %{@events => [{"ts", :ge, %NaiveDateTime{} = since}]} = lower
      assert NaiveDateTime.diff(NaiveDateTime.utc_now(), since, :second) in 890..910

      assert conjuncts("SELECT * FROM analytics.events WHERE ts <= now()") == %{}
      assert conjuncts("SELECT * FROM analytics.events WHERE now() - INTERVAL 1 HOUR > ts") == %{}

      between =
        "SELECT * FROM analytics.events WHERE ts BETWEEN now64() - INTERVAL 1 HOUR AND now64()"

      assert %{@events => [{"ts", :ge, %NaiveDateTime{}}]} = conjuncts(between)
    end

    test "a fixed bound beside a clock one still bounds from above (review of T-534)" do
      sql =
        "SELECT * FROM analytics.events WHERE ts >= now() - INTERVAL 1 HOUR " <>
          "AND ts <= toDateTime64('2030-01-01 00:00:00', 3)"

      assert %{
               @events => [
                 {"ts", :ge, %NaiveDateTime{}},
                 {"ts", :le, ~N[2030-01-01 00:00:00.000000]}
               ]
             } = conjuncts(sql)
    end

    test "a clock expression that does not rise with the clock is not folded (review of T-534)" do
      for bound <- [
            "TIMESTAMP '2026-09-19 10:00:00' - (now() - TIMESTAMPTZ '2026-09-19 10:00:00+00')",
            "TIMESTAMPTZ '2030-01-01 00:00:00+00' - (now() - now())",
            "to_timestamp(-epoch(now()))",
            "now() - (now() - TIMESTAMPTZ '2020-01-01 00:00:00+00')"
          ] do
        assert conjuncts("SELECT * FROM analytics.events WHERE ts >= #{bound}") == %{}, bound
      end

      for bound <- [
            "now()",
            "now() - INTERVAL 15 MINUTE",
            "INTERVAL 1 MINUTE + now()",
            "CAST(now() AS TIMESTAMP) - INTERVAL 1 DAY",
            "date_trunc('hour', now())",
            "toDateTime(now64(3) - INTERVAL 1 HOUR)"
          ] do
        assert %{@events => [{"ts", :ge, _since}]} =
                 conjuncts("SELECT * FROM analytics.events WHERE ts >= #{bound}"),
               bound
      end
    end

    test "a TIMESTAMP_NS bound arrives cut to the microsecond, and bounds from above a microsecond up (review of T-534)" do
      bound = "parseDateTime64BestEffort('2026-09-19T10:11:00.1234567Z', 9)"

      assert conjuncts("SELECT * FROM analytics.events WHERE ts < #{bound}") ==
               %{@events => [{"ts", :lt, ~N[2026-09-19 10:11:00.123457]}]}

      assert conjuncts("SELECT * FROM analytics.events WHERE ts >= #{bound}") ==
               %{@events => [{"ts", :ge, ~N[2026-09-19 10:11:00.123456]}]}

      assert conjuncts("SELECT * FROM analytics.events WHERE ts = #{bound}") == %{}

      file = %{
        "id" => "01A",
        "stats" => %{
          "ts" => %{
            "min" => %{"type" => "naive_datetime", "value" => "2026-09-19T10:11:00.123456"},
            "max" => %{"type" => "naive_datetime", "value" => "2026-09-19T10:11:00.123457"},
            "null_count" => 0
          }
        }
      }

      %{@events => above} = conjuncts("SELECT * FROM analytics.events WHERE ts < #{bound}")

      assert Pruner.keep?(file, above)
    end

    test "a TIMESTAMPTZ is this module's bound only when the engine's zone is UTC (review of T-534)" do
      sql =
        "SELECT * FROM analytics.events WHERE ts >= TIMESTAMPTZ '2026-09-19 10:00:00-07' + INTERVAL 1 HOUR"

      assert conjuncts(sql) == %{@events => [{"ts", :ge, ~N[2026-09-19 18:00:00.000000]}]}

      Engine.query!(@engine, "SET TimeZone = 'America/Phoenix'")

      try do
        assert conjuncts(sql) == %{}

        assert conjuncts("SELECT * FROM analytics.events WHERE ts >= now() - INTERVAL 1 HOUR") ==
                 %{}

        assert conjuncts(
                 "SELECT * FROM analytics.events WHERE ts >= toDateTime64('2026-09-19 10:00:00', 3)"
               ) ==
                 %{@events => [{"ts", :ge, ~N[2026-09-19 10:00:00.000000]}]}
      after
        Engine.query!(@engine, "SET TimeZone = 'UTC'")
      end
    end

    test "every ClickHouse macro that reads the clock is known to read it" do
      reading =
        for statement <- ClickHouseFunctions.statements(),
            statement =~ ~r/\b(now|current_\w+|today|get_current_time\w*)\s*\(/i,
            [_all, name] = Regex.run(~r/MACRO\s+"?(\w+)"?\s*\(/i, statement),
            do: String.downcase(name)

      assert Enum.sort(reading) == Enum.sort(Fold.clock_macros())
    end

    test "a value that is no bound, a NULL or an interval, bounds nothing" do
      for bound <- ["CAST(NULL AS TIMESTAMP) + INTERVAL 1 HOUR", "INTERVAL 1 HOUR * 2"] do
        assert conjuncts("SELECT * FROM analytics.events WHERE ts >= #{bound}") == %{}, bound
      end
    end

    test "under lockdown the fold runs with extension autoload off" do
      {_ast, folded} = folded("SELECT * FROM analytics.events WHERE id > 40 + 2", true)

      assert [{:fixed, 42}] = Map.values(folded)

      assert {:ok, %Result{rows: [["false"]]}} =
               Connection.query(
                 @conn,
                 "SELECT value FROM duckdb_settings() WHERE name = 'autoload_known_extensions'"
               )
    end
  end
end
