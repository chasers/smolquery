defmodule Smolquery.QueryService.NullabilityTest do
  @moduledoc """
  Which result columns can never be `NULL` (T-510), over statements DuckDB
  parsed: the same `json_serialize_sql` tree the planner hands the module.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Nullability
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @engine __MODULE__.Engine

  @schemas %{
    {"logs", "events"} =>
      Schema.new!([
        Field.new!("id", :int64, nullable: false),
        Field.new!("ts_int", :int64),
        Field.new!("ts", :timestamp, materialized: "epoch_ms(ts_int)", nullable: false),
        Field.new!("maybe_ts", :timestamp, materialized: "epoch_ms(ts_int)"),
        Field.new!("level", :string)
      ])
  }

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: [:json]})
    :ok
  end

  defp columns(sql) do
    {:ok, %{rows: [[json]]}} =
      Engine.query(@engine, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    Nullability.columns(JSON.decode!(json), @schemas)
  end

  test "HyperDX's histogram: the bucket over a non-nullable timestamp, count(), and a nullable column" do
    sql =
      "SELECT count(), level, toStartOfInterval(toDateTime(ts), INTERVAL 1 minute) AS b, " <>
        "toStartOfInterval(maybe_ts, INTERVAL 30 second) AS m " <>
        "FROM logs.events WHERE ts >= fromUnixTimestamp64Milli(1) GROUP BY level, b, m ORDER BY b LIMIT 10"

    assert columns(sql) == [true, false, true, false]
  end

  test "a column keeps its declaration through a star, a subquery, a CTE and an alias" do
    assert columns("SELECT * FROM logs.events") == [true, false, true, false, false]
    assert columns("SELECT e.id, e.level FROM logs.events AS e") == [true, false]

    assert columns(
             "WITH c AS (SELECT id AS n, level FROM logs.events) SELECT t.n, t.level, n + 1 FROM c AS t"
           ) ==
             [true, false, true]

    assert columns("SELECT * FROM (SELECT ts, 1 AS one FROM logs.events) s") == [true, true]
  end

  test "constants, casts, CASE, COALESCE and IS NULL" do
    assert columns(
             "SELECT 1, 'a', NULL, CAST(id AS VARCHAR), TRY_CAST(id AS VARCHAR) FROM logs.events"
           ) ==
             [true, true, false, true, false]

    assert columns(
             "SELECT CASE WHEN id > 1 THEN 'a' ELSE 'b' END, CASE WHEN id > 1 THEN 'a' END, " <>
               "COALESCE(level, 'none'), level IS NULL, id > 1 AND id < 9 FROM logs.events"
           ) == [true, false, true, true, true]
  end

  test "an aggregate other than count is non-null only under an explicit GROUP BY" do
    assert columns(
             "SELECT count(*), count(level), max(id), max(level) FROM logs.events GROUP BY id"
           ) ==
             [true, true, true, false]

    assert columns("SELECT count(*), max(id) FROM logs.events") == [true, false]
    assert columns("SELECT sum(id) FILTER (WHERE id > 1) FROM logs.events GROUP BY id") == [false]
  end

  test "what it cannot rule out may be NULL" do
    assert columns("SELECT e.id, o.id FROM logs.events e LEFT JOIN logs.events o ON e.id = o.id") ==
             [false, false]

    assert columns("SELECT e.id, o.id FROM logs.events e JOIN logs.events o ON e.id = o.id") ==
             [true, true]

    assert columns("SELECT id FROM logs.events e JOIN logs.events o ON e.id = o.id") == [false]

    assert columns("SELECT some_function(id), id // 0, id FROM logs.events") == [
             false,
             false,
             true
           ]

    assert columns("SELECT id FROM logs.events GROUP BY ROLLUP (id)") == [false]
    assert columns("SELECT * FROM range(3)") == :unknown
    assert columns("SELECT * EXCLUDE (level) FROM logs.events") == :unknown
    assert columns("SELECT id FROM logs.events UNION ALL SELECT 1") == :unknown
    assert columns("SELECT id FROM other.table_unknown") == [false]
  end
end
