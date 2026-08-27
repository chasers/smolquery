defmodule Smolquery.QueryService.VariantResultsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.VariantResults
  alias Smolquery.Schema

  @engine __MODULE__.Instance

  setup do
    start_supervised!({Engine, name: @engine})

    Engine.query!(
      @engine,
      "CREATE TABLE events AS SELECT 1::BIGINT AS id, " <>
        ~s('{"host":"h1","n":1,"tags":["a"]}'::JSON::VARIANT AS attrs) <>
        " UNION ALL SELECT 2, NULL"
    )

    %{connection: Engine.connection_name(@engine)}
  end

  defp plan(sql, schema) do
    %Plan{sql: sql, canonical_sql: sql, snapshot: 1, schemas: %{{"a", "events"} => schema}}
  end

  defp variant_schema, do: Schema.new!([{"id", :int64}, {"attrs", :variant}])

  test "casts each VARIANT result column to JSON and names it", %{connection: connection} do
    sql = "SELECT id, attrs, attrs['host']::VARCHAR AS host FROM events ORDER BY id"

    assert {:ok, plan, ["attrs"]} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))

    assert plan.sql == plan.canonical_sql
    assert plan.sql == ~s|SELECT "id", "attrs"::JSON AS "attrs", "host" FROM (#{sql})|

    {:ok, frame} = Engine.frame(@engine, plan.sql)

    assert Frame.to_rows(frame, json_columns: ["attrs"]) == [
             %{
               "id" => 1,
               "attrs" => %{"host" => "h1", "n" => 1, "tags" => ["a"]},
               "host" => "h1"
             },
             %{"id" => 2, "attrs" => nil, "host" => nil}
           ]
  end

  test "leaves a plan alone when no VARIANT reaches the result", %{connection: connection} do
    sql = "SELECT id, attrs['host']::VARCHAR AS host FROM events"

    assert VariantResults.prepare(connection, plan(sql, variant_schema())) ==
             {:ok, plan(sql, variant_schema()), []}
  end

  test "never describes a plan whose tables declare no variant", %{connection: connection} do
    schema = Schema.new!([{"id", :int64}])
    sql = "SELECT attrs FROM events"

    assert VariantResults.prepare(connection, plan(sql, schema)) == {:ok, plan(sql, schema), []}

    assert VariantResults.prepare(connection, %Plan{sql: sql, snapshot: 1}) ==
             {:ok, %Plan{sql: sql, snapshot: 1}, []}
  end

  test "refuses a VARIANT nested in a struct or list", %{connection: connection} do
    sql = "SELECT {'v': attrs} AS s FROM events"

    assert {:error, {:invalid_query, message}} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))

    assert message =~ ~s|column "s" has type STRUCT(v VARIANT)|
  end

  test "a query that does not bind is the planner's error, not a crash", %{connection: connection} do
    assert {:error, _reason} =
             VariantResults.prepare(connection, plan("SELECT nope FROM events", variant_schema()))
  end
end
