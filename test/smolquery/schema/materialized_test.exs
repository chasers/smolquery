defmodule Smolquery.Schema.MaterializedTest do
  @moduledoc """
  The three gates a materialized expression passes at definition time
  (PL-61 L4), each against a real DuckDB engine and nothing else.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Schema.Materialized

  @schema Schema.new!([
            {"id", :int64, nullable: false, id: 1},
            {"ts_int", :int64, id: 2},
            {"label", :string, id: 3},
            {"derived", :timestamp, id: 4, materialized: "epoch_ms(ts_int)"}
          ])

  defp validate(expression, type \\ :timestamp),
    do: Materialized.validate(@schema, Field.new!("m", type, materialized: expression))

  describe "what passes" do
    test "a function over a regular column: the canonical text and the source ids" do
      assert {:ok, %Materialized{} = definition} = validate("epoch_ms(ts_int)")

      assert definition.expression == "epoch_ms(ts_int)"
      assert definition.canonical == "epoch_ms(ts_int)"
      assert definition.sources == [2]
    end

    test "operators, casts, CASE, BETWEEN, constants, and several sources once each" do
      assert {:ok, %Materialized{sources: [1, 2, 3]}} =
               validate(
                 "CASE WHEN ts_int BETWEEN 0 AND 10 THEN CAST(id AS VARCHAR) ELSE label || 'x' END",
                 :string
               )
    end

    test "a comment or a trailing semicolon does not survive into the canonical text" do
      assert {:ok, %Materialized{canonical: "epoch_ms(ts_int)"}} =
               validate("epoch_ms(ts_int) -- computed\n")

      assert {:ok, %Materialized{canonical: "epoch_ms(ts_int)"}} = validate("epoch_ms(ts_int);")
    end

    test "a schema without ids yields the source names, for the catalog to map later" do
      schema = Schema.new!([{"id", :int64}, {"ts_int", :int64}])

      assert {:ok, %Materialized{sources: ["ts_int"]}} =
               Materialized.validate(
                 schema,
                 Field.new!("m", :timestamp, materialized: "epoch_ms(ts_int)")
               )
    end
  end

  describe "gate 1: the parse and the walk" do
    test "a syntax error names DuckDB's reason" do
      assert {:error, {:invalid_materialized, {:unparseable, message}}} = validate("epoch_ms(")
      assert message =~ "syntax error"
    end

    test "two expressions, or two statements, are refused" do
      assert {:error, {:invalid_materialized, :one_expression}} = validate("id, ts_int")
      assert {:error, {:invalid_materialized, _detail}} = validate("id; SELECT 1")
    end

    test "a subquery, a window, a star, a parameter, a lambda are refused by class" do
      assert {:error, {:invalid_materialized, {:unsupported_expression, "SUBQUERY"}}} =
               validate("(SELECT max(ts_int))")

      assert {:error, {:invalid_materialized, {:unsupported_expression, "WINDOW"}}} =
               validate("row_number() OVER ()", :int64)

      assert {:error, {:invalid_materialized, {:unsupported_expression, "STAR"}}} =
               validate("*", :int64)

      assert {:error, {:invalid_materialized, {:unsupported_expression, "PARAMETER"}}} =
               validate("$1", :int64)

      assert {:error, {:invalid_materialized, {:unsupported_expression, "LAMBDA"}}} =
               validate("list_transform([ts_int], x -> x + 1)[1]", :int64)
    end

    test "a column must be a regular column of this table, named without a qualifier" do
      assert {:error, {:invalid_materialized, {:unknown_column, "nope"}}} = validate("nope + 1")

      assert {:error, {:invalid_materialized, {:materialized_column, "derived"}}} =
               validate("derived + INTERVAL 1 HOUR")

      assert {:error, {:invalid_materialized, {:qualified_column, "t.ts_int"}}} =
               validate("t.ts_int")
    end

    test "an unknown function is refused by name" do
      assert {:error, {:invalid_materialized, {:unknown_function, "no_such_function"}}} =
               validate("no_such_function(ts_int)")
    end

    test "current_setting is refused by name" do
      assert {:error, {:invalid_materialized, {:unsupported_function, "current_setting"}}} =
               validate("current_setting('home_directory')", :string)
    end
  end

  describe "gate 1: the statement shape" do
    test "a FROM, WHERE, modifier or alias is refused as more than one expression" do
      for tail <- ["id FROM t", "id WHERE ts_int > 1", "DISTINCT id", "id LIMIT 1", "id AS x"] do
        assert {:error, {:invalid_materialized, :one_expression}} = validate(tail, :int64), tail
      end
    end
  end

  describe "gate 2: determinism" do
    test "now(), random() and gen_random_uuid() are refused" do
      assert {:error, {:invalid_materialized, {:inconsistent_function, "now"}}} =
               validate("now()")

      assert {:error, {:invalid_materialized, {:inconsistent_function, "random"}}} =
               validate("random()", :float64)

      assert {:error, {:invalid_materialized, {:inconsistent_function, "gen_random_uuid"}}} =
               validate("gen_random_uuid()::VARCHAR", :string)
    end
  end

  describe "gate 3: the locked-down probe" do
    test "an aggregate passes no gate: it is not a scalar function, and it would fail every write" do
      assert {:error, {:invalid_materialized, {:not_scalar, "sum", ["aggregate"]}}} =
               validate("sum(ts_int)", :int64)

      assert {:error, {:invalid_materialized, {:not_scalar, "count_star", ["aggregate"]}}} =
               validate("count(*)", :int64)
    end

    test "wall-clock and environment readers are refused by name whatever their stability" do
      assert {:error, {:invalid_materialized, {:unsupported_function, "current_localtimestamp"}}} =
               validate("current_localtimestamp()")

      assert {:error, {:invalid_materialized, {:unsupported_function, "version"}}} =
               validate("version()", :string)
    end

    test "a time-zone-dependent value is refused: a cast to TIMESTAMPTZ, or a function that yields one" do
      assert {:error, {:invalid_materialized, {:zoned_type, "TIMESTAMP WITH TIME ZONE"}}} =
               validate("CAST(epoch_ms(ts_int) AS TIMESTAMPTZ)::VARCHAR", :string)

      assert {:error, {:invalid_materialized, {:unsupported_function, "to_timestamp"}}} =
               validate("to_timestamp(ts_int)")

      assert {:error, {:invalid_materialized, {:unsupported_function, "timezone"}}} =
               validate("make_timestamp(ts_int) AT TIME ZONE 'UTC'")
    end

    test "a table function is not a scalar function, so it never reaches the probe" do
      assert {:error, {:invalid_materialized, {:not_scalar, "read_text", ["table"]}}} =
               validate("read_text('/etc/hostname')", :string)
    end

    test "the expression must cast to the column's type" do
      assert {:error, {:invalid_materialized, {:does_not_bind, message}}} =
               validate("{'a': 1}", :timestamp)

      assert message =~ "Binder Error" or message =~ "Conversion Error"
    end
  end

  test "message/1 reads as a sentence for every refusal" do
    assert Materialized.message({:inconsistent_function, "now"}) =~ "not deterministic"
    assert Materialized.message({:unknown_column, "x"}) =~ "does not have: x"
    assert Materialized.message({:unsupported_expression, "SUBQUERY"}) =~ "subquery"
  end
end
