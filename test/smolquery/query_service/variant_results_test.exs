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

  defp rows(sql, json_columns) do
    {:ok, frame} = Engine.frame(@engine, sql)

    Frame.to_rows(frame, json_columns: json_columns)
  end

  test "casts each VARIANT result column to JSON, names it, and keeps the described outputs", %{
    connection: connection
  } do
    sql = "SELECT id, attrs, attrs['host']::VARCHAR AS host FROM events ORDER BY id"

    assert {:ok, %{plan: plan, json_columns: ["attrs"], outputs: outputs}} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))

    assert outputs == [{"id", "BIGINT"}, {"attrs", "VARIANT"}, {"host", "VARCHAR"}]
    assert plan.sql == plan.canonical_sql
    assert plan.sql == ~s|SELECT "id", "attrs"::JSON AS "attrs", "host" FROM (#{sql})|

    assert rows(plan.sql, ["attrs"]) == [
             %{
               "id" => 1,
               "attrs" => %{"host" => "h1", "n" => 1, "tags" => ["a"]},
               "host" => "h1"
             },
             %{"id" => 2, "attrs" => nil, "host" => nil}
           ]
  end

  test "quotes a result label DuckDB made, not only an identifier", %{connection: connection} do
    sql = "SELECT attrs['host'], count(*) FROM events GROUP BY ALL ORDER BY 1"

    assert {:ok, %{plan: plan, json_columns: ["attrs['host']"]}} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))

    assert plan.sql ==
             ~s|SELECT "attrs['host']"::JSON AS "attrs['host']", "count_star()" FROM (#{sql})|

    assert rows(plan.sql, ["attrs['host']"]) == [
             %{"attrs['host']" => "h1", "count_star()" => 1},
             %{"attrs['host']" => nil, "count_star()" => 1}
           ]
  end

  test "a repeated result name is cast each time it appears", %{connection: connection} do
    sql = "SELECT attrs, attrs FROM events WHERE id = 1"

    assert {:ok, %{json_columns: ["attrs", "attrs"], plan: plan}} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))

    assert {:ok, %{rows: [[text, text]]}} = Engine.query(@engine, plan.sql)
    assert JSON.decode!(text) == %{"host" => "h1", "n" => 1, "tags" => ["a"]}
  end

  test "leaves a plan alone when no VARIANT reaches the result, but hands back the outputs", %{
    connection: connection
  } do
    sql = "SELECT count(*) FROM events"

    assert VariantResults.prepare(connection, plan(sql, variant_schema())) ==
             {:ok,
              %VariantResults{
                plan: plan(sql, variant_schema()),
                json_columns: [],
                outputs: [{"count_star()", "BIGINT"}]
              }}
  end

  test "never describes a plan whose tables declare no variant", %{connection: connection} do
    schema = Schema.new!([{"id", :int64}])
    sql = "SELECT attrs FROM events"

    assert VariantResults.prepare(connection, plan(sql, schema)) ==
             {:ok, %VariantResults{plan: plan(sql, schema), json_columns: [], outputs: nil}}

    refute VariantResults.variant_reachable?(plan(sql, schema))
    refute VariantResults.variant_reachable?(%Plan{sql: sql, snapshot: 1})
    assert VariantResults.variant_reachable?(plan(sql, variant_schema()))
  end

  test "refuses a VARIANT nested in a struct or list", %{connection: connection} do
    for sql <- ["SELECT {'v': attrs} AS s FROM events", "SELECT [attrs] AS l FROM events"] do
      assert {:error, {:invalid_query, message}} =
               VariantResults.prepare(connection, plan(sql, variant_schema()))

      assert message =~ "VARIANT nested in a struct or list"
    end
  end

  test "a name or literal that merely says VARIANT is not a nested variant", %{
    connection: connection
  } do
    sql = "SELECT {'VARIANT_ID': id} AS s, 'VARIANT'::VARCHAR AS word FROM events"

    assert {:ok, %{json_columns: []}} =
             VariantResults.prepare(connection, plan(sql, variant_schema()))
  end

  test "a query that does not bind is the engine's error, not a crash", %{
    connection: connection
  } do
    assert {:error, %Adbc.Error{}} =
             VariantResults.prepare(connection, plan("SELECT nope FROM events", variant_schema()))
  end

  describe "explain_export_failure/3" do
    test "names a VARIANT with no table behind it", %{connection: connection} do
      sql = "SELECT '1'::VARIANT AS v"
      error = %Adbc.Error{message: "Generic Error: Internal Arrow error: ComputeError"}

      assert {:invalid_query, message} =
               VariantResults.explain_export_failure(
                 connection,
                 plan(sql, variant_schema()),
                 error
               )

      assert message =~ ~s(column "v" is a VARIANT)
    end

    test "passes any other error through", %{connection: connection} do
      plan = plan("SELECT id FROM events", variant_schema())
      other = %Adbc.Error{message: "Catalog Error: Table with name x does not exist"}
      arrow = %Adbc.Error{message: "Generic Error: Internal Arrow error: ComputeError"}

      assert VariantResults.explain_export_failure(connection, plan, other) == other
      assert VariantResults.explain_export_failure(connection, plan, arrow) == arrow
    end
  end
end
