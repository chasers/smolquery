defmodule Smolquery.QueryService.PrunerTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
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

    test "a set operation prunes nothing" do
      sql = "SELECT id FROM analytics.events WHERE id > 5 UNION ALL SELECT 1"

      assert conjuncts(sql) == %{}
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
  end
end
