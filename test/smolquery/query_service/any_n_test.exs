defmodule Smolquery.QueryService.AnyNTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.AnyN

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

  defp spec(sql, refs \\ [@events]), do: AnyN.spec(statement(sql), refs)

  defp entry(id, rows), do: %{"id" => id, "row_count" => rows}

  defp ids(entries), do: Enum.map(entries, & &1["id"])

  describe "spec/2" do
    test "an unordered, unfiltered LIMIT over one table qualifies" do
      assert spec("SELECT * FROM analytics.events LIMIT 50") == %{ref: @events, limit: 50}

      assert spec("SELECT id, name FROM analytics.events e LIMIT 10") == %{
               ref: @events,
               limit: 10
             }
    end

    test "the OFFSET rows are read before they are skipped, so they count" do
      assert spec("SELECT * FROM analytics.events LIMIT 5 OFFSET 20") == %{
               ref: @events,
               limit: 25
             }
    end

    test "casts, constants, and an EXCLUDE project rows without reading them all" do
      assert %{limit: 3} =
               spec("SELECT id::text, 1, * EXCLUDE (name) FROM analytics.events LIMIT 3")
    end

    test "a WHERE does not qualify: row counts cannot say how many rows survive it" do
      assert spec("SELECT * FROM analytics.events WHERE id > 1 LIMIT 5") == nil
    end

    test "an ORDER BY does not qualify: that is the Top-N bound's case" do
      assert spec("SELECT * FROM analytics.events ORDER BY id DESC LIMIT 5") == nil
    end

    test "no LIMIT, a non-constant LIMIT, or a percentage does not qualify" do
      assert spec("SELECT * FROM analytics.events") == nil
      assert spec("SELECT * FROM analytics.events LIMIT 2 + 3") == nil
      assert spec("SELECT * FROM analytics.events LIMIT 10%") == nil
      assert spec("SELECT * FROM analytics.events LIMIT 0") == nil
    end

    test "any function call in the select list does not qualify: it may aggregate" do
      assert spec("SELECT count(*) FROM analytics.events LIMIT 1") == nil
      assert spec("SELECT max(id) FROM analytics.events LIMIT 1") == nil
      assert spec("SELECT id + 1 FROM analytics.events LIMIT 5") == nil
      assert spec("SELECT * REPLACE (id + 1 AS id) FROM analytics.events LIMIT 5") == nil
    end

    test "DISTINCT, GROUP BY, HAVING, QUALIFY, SAMPLE, and GROUP BY ALL do not qualify" do
      assert spec("SELECT DISTINCT name FROM analytics.events LIMIT 5") == nil
      assert spec("SELECT name FROM analytics.events GROUP BY name LIMIT 5") == nil

      assert spec("SELECT name FROM analytics.events GROUP BY name HAVING name > 'a' LIMIT 5") ==
               nil

      assert spec("SELECT name FROM analytics.events QUALIFY row_number() OVER () = 1 LIMIT 5") ==
               nil

      assert spec("SELECT * FROM analytics.events USING SAMPLE 10 LIMIT 5") == nil
      assert spec("SELECT name FROM analytics.events GROUP BY ALL LIMIT 5") == nil
    end

    test "a window expression, a subquery, a CTE, or a join does not qualify" do
      assert spec("SELECT row_number() OVER () FROM analytics.events LIMIT 5") == nil
      assert spec("SELECT (SELECT 1) FROM analytics.events LIMIT 5") == nil
      assert spec("WITH c AS (SELECT 1) SELECT * FROM analytics.events LIMIT 5") == nil

      assert spec(
               "SELECT * FROM analytics.events e JOIN analytics.users u ON u.id = e.id LIMIT 5",
               [@events, @users]
             ) == nil
    end

    test "a FROM that is not one of the plan's references does not qualify" do
      assert spec("SELECT * FROM analytics.users LIMIT 5") == nil
      assert spec("SELECT * FROM (SELECT * FROM analytics.events) LIMIT 5") == nil
    end
  end

  describe "trim/3" do
    test "takes the newest entries by id until their rows cover the limit" do
      entries = for k <- 1..5, do: entry("0#{k}", 10)

      assert ids(AnyN.trim(entries, 0, 25)) == ["05", "04", "03"]
    end

    test "the sealed tier covers first: enough sealed rows need no hot file" do
      entries = for k <- 1..5, do: entry("0#{k}", 10)

      assert AnyN.trim(entries, 25, 25) == []
      assert ids(AnyN.trim(entries, 20, 25)) == ["05"]
    end

    test "unknown sealed statistics count as no rows" do
      entries = for k <- 1..5, do: entry("0#{k}", 10)

      assert ids(AnyN.trim(entries, :unavailable, 15)) == ["05", "04"]
    end

    test "entries short of the limit are all kept" do
      entries = for k <- 1..3, do: entry("0#{k}", 10)

      assert ids(AnyN.trim(entries, 0, 100)) == ["03", "02", "01"]
    end

    test "an entry without a row count is taken and counts for nothing" do
      entries = [entry("01", 10), %{"id" => "02"}]

      assert ids(AnyN.trim(entries, 0, 5)) == ["02", "01"]
    end
  end
end
