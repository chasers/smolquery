defmodule Smolquery.SchemaTest do
  use ExUnit.Case, async: true

  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @logical_types [
    :int64,
    :float64,
    :string,
    :bool,
    :timestamp,
    :date,
    {:numeric, 38, 2}
  ]

  describe "new/1" do
    test "accepts tuples, tuples with options, and Field structs" do
      assert {:ok, schema} =
               Schema.new([
                 {"id", :int64, nullable: false},
                 {"ts", :timestamp},
                 Field.new!("label", :string)
               ])

      assert Schema.names(schema) == ["id", "ts", "label"]
      assert [%Field{nullable: false}, %Field{nullable: true}, %Field{}] = schema.fields
    end

    test "rejects an empty schema" do
      assert Schema.new([]) == {:error, :empty_schema}
    end

    test "rejects duplicate column names" do
      assert Schema.new([{"id", :int64}, {"id", :string}]) ==
               {:error, {:duplicate_columns, ["id"]}}
    end

    test "propagates a bad field" do
      assert Schema.new([{"id", :int64}, {"bad name", :string}]) ==
               {:error, {:invalid_identifier, "bad name"}}
    end

    test "rejects a spec it cannot read" do
      assert Schema.new(["id"]) == {:error, {:invalid_field_spec, "id"}}
    end
  end

  describe "new!/1" do
    test "raises on an invalid schema" do
      assert_raise ArgumentError, ~r/invalid schema/, fn -> Schema.new!([{"id", :int32}]) end
    end
  end

  describe "field/2" do
    test "finds a field by name" do
      schema = Schema.new!([{"id", :int64}])

      assert {:ok, %Field{name: "id"}} = Schema.field(schema, "id")
      assert Schema.field(schema, "missing") == :error
    end
  end

  describe "validate_type/1" do
    test "accepts every supported logical type" do
      for type <- @logical_types do
        assert Schema.validate_type(type) == {:ok, type}
      end
    end

    test "bounds numeric precision and scale" do
      assert {:ok, _type} = Schema.validate_type({:numeric, 38, 38})
      assert {:error, {:unsupported_type, _}} = Schema.validate_type({:numeric, 39, 2})
      assert {:error, {:unsupported_type, _}} = Schema.validate_type({:numeric, 10, 11})
      assert {:error, {:unsupported_type, _}} = Schema.validate_type({:numeric, 0, 0})
    end

    test "rejects unknown types" do
      assert Schema.validate_type(:int32) == {:error, {:unsupported_type, :int32}}
      assert Schema.validate_type("BIGINT") == {:error, {:unsupported_type, "BIGINT"}}
    end
  end

  describe "type mapping" do
    test "round-trips every logical type through Explorer dtypes" do
      for type <- @logical_types do
        assert {:ok, dtype} = Schema.explorer_dtype(type)
        assert Schema.logical_from_explorer(dtype) == {:ok, type}
      end
    end

    test "round-trips every logical type through DuckDB type names" do
      for type <- @logical_types do
        assert {:ok, name} = Schema.duckdb_type(type)
        assert Schema.logical_from_duckdb(name) == {:ok, type}
      end
    end

    test "maps to the types Milestone 1 verified" do
      assert Schema.explorer_dtype(:int64) == {:ok, {:s, 64}}
      assert Schema.explorer_dtype(:timestamp) == {:ok, {:naive_datetime, :microsecond}}
      assert Schema.explorer_dtype({:numeric, 38, 2}) == {:ok, {:decimal, 38, 2}}
      assert Schema.duckdb_type(:float64) == {:ok, "DOUBLE"}
      assert Schema.duckdb_type(:string) == {:ok, "VARCHAR"}
      assert Schema.duckdb_type({:numeric, 38, 2}) == {:ok, "DECIMAL(38,2)"}
    end

    test "reads DuckDB type names case-insensitively and with spacing" do
      assert Schema.logical_from_duckdb("bigint") == {:ok, :int64}
      assert Schema.logical_from_duckdb("decimal(18, 4)") == {:ok, {:numeric, 18, 4}}
    end

    test "reports types it does not carry" do
      assert Schema.duckdb_type(:blob) == {:error, {:unsupported_type, :blob}}
      assert Schema.explorer_dtype(:blob) == {:error, {:unsupported_type, :blob}}
      assert Schema.logical_from_explorer({:s, 32}) == {:error, {:unsupported_type, {:s, 32}}}

      assert Schema.logical_from_duckdb("STRUCT(a INT)") ==
               {:error, {:unsupported_type, "STRUCT(a INT)"}}
    end
  end

  describe "explorer_dtypes/1" do
    test "pairs column names with dtypes in schema order" do
      schema = Schema.new!([{"id", :int64}, {"amount", {:numeric, 12, 3}}])

      assert Schema.explorer_dtypes(schema) ==
               {:ok, [{"id", {:s, 64}}, {"amount", {:decimal, 12, 3}}]}
    end
  end

  describe "projection/2" do
    test "selects every declared column in the schema's order, cast to its type" do
      schema = Schema.new!([{"id", :int64}, {"amount", {:numeric, 12, 3}}])

      assert Schema.projection(schema, ["amount", "id"]) ==
               {:ok,
                ~s|CAST("id" AS BIGINT) AS "id", CAST("amount" AS DECIMAL(12,3)) AS "amount"|}
    end

    test "substitutes a typed NULL for a column the relation does not carry" do
      schema = Schema.new!([{"id", :int64}, {"name", :string}])

      assert Schema.projection(schema, ["id"]) ==
               {:ok, ~s|CAST("id" AS BIGINT) AS "id", CAST(NULL AS VARCHAR) AS "name"|}
    end

    test "drops a column the schema does not declare" do
      schema = Schema.new!([{"id", :int64}])

      assert Schema.projection(schema, ["id", "name"]) == {:ok, ~s|CAST("id" AS BIGINT) AS "id"|}
    end

    test "reports a type it cannot render" do
      schema = %Schema{fields: [%Field{name: "b", type: :blob}]}

      assert Schema.projection(schema, ["b"]) == {:error, {:unsupported_type, :blob}}
    end
  end

  describe "column_definitions/1" do
    test "renders quoted DDL preserving order and nullability" do
      schema =
        Schema.new!([
          {"id", :int64, nullable: false},
          {"ts", :timestamp},
          {"amount", {:numeric, 38, 2}}
        ])

      assert Schema.column_definitions(schema) ==
               {:ok, ~s|"id" BIGINT NOT NULL, "ts" TIMESTAMP, "amount" DECIMAL(38,2)|}
    end
  end
end
