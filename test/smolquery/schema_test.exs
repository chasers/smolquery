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
    {:numeric, 38, 2},
    {:map, :string, :string},
    :variant
  ]

  @explorer_types @logical_types -- [{:map, :string, :string}, :variant]

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
      assert schema.clustering == []
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

  describe "clustering_columns/1" do
    test "is empty for a schema with no clustering key" do
      assert Schema.clustering_columns(Schema.new!([{"id", :int64}])) == []
    end

    test "keeps the key's own order, not the schema's" do
      schema = %{
        Schema.new!([{"id", :int64}, {"ts", :timestamp}])
        | clustering: ["ts", "id"]
      }

      assert Schema.clustering_columns(schema) == ["ts", "id"]
    end

    test "drops a column the schema no longer declares" do
      schema = %{
        Schema.new!([{"id", :int64}, {"ts", :timestamp}])
        | clustering: ["ts", "gone", "id"]
      }

      assert Schema.clustering_columns(schema) == ["ts", "id"]
    end

    test "is empty when the schema declares none of the key" do
      schema = %{Schema.new!([{"id", :int64}]) | clustering: ["gone", "also_gone"]}

      assert Schema.clustering_columns(schema) == []
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
    test "round-trips every Explorer-writable logical type through Explorer dtypes" do
      for type <- @explorer_types do
        assert {:ok, dtype} = Schema.explorer_dtype(type)
        assert Schema.logical_from_explorer(dtype) == {:ok, type}
      end
    end

    test "a map has no Explorer dtype, so Explorer-side paths fall back on it" do
      assert Schema.explorer_dtype({:map, :string, :string}) ==
               {:error, {:unsupported_type, {:map, :string, :string}}}

      assert {:error, {:unsupported_type, _map}} =
               Schema.explorer_dtypes(
                 Schema.new!([{"id", :int64}, {"attrs", {:map, :string, :string}}])
               )
    end

    test "a variant has no Explorer dtype, is stored as JSON, and is queried as VARIANT" do
      assert Schema.explorer_dtype(:variant) == {:error, {:unsupported_type, :variant}}
      assert Schema.duckdb_type(:variant) == {:ok, "JSON"}
      assert Schema.logical_from_duckdb("json") == {:ok, :variant}
      assert Schema.view_cast(:variant) == {:cast, "VARIANT"}
      assert Schema.api_type(:variant) == {:ok, "VARIANT"}
      assert Schema.type_from_api("Variant") == {:ok, :variant}
    end

    test "every other type needs no cast in a view" do
      for type <- @explorer_types ++ [{:map, :string, :string}] do
        assert Schema.view_cast(type) == :none
      end
    end

    test "speaks the map in DuckDB's and ClickHouse's words" do
      assert Schema.duckdb_type({:map, :string, :string}) == {:ok, "MAP(VARCHAR, VARCHAR)"}
      assert Schema.logical_from_duckdb("map(varchar,varchar)") == {:ok, {:map, :string, :string}}
      assert Schema.api_type({:map, :string, :string}) == {:ok, "MAP(STRING, STRING)"}
      assert Schema.type_from_api("map(string,string)") == {:ok, {:map, :string, :string}}
      assert Schema.type_from_api("MAP( STRING , STRING )") == {:ok, {:map, :string, :string}}

      assert Schema.type_from_api("MAP(STRING, INT64)") ==
               {:error, {:unsupported_type, "MAP(STRING, INT64)"}}

      assert Schema.logical_from_duckdb("MAP(VARCHAR, BIGINT)") ==
               {:error, {:unsupported_type, "MAP(VARCHAR, BIGINT)"}}
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

    test "round-trips every logical type through API names" do
      for type <- @logical_types do
        assert {:ok, name} = Schema.api_type(type)
        assert Schema.type_from_api(name) == {:ok, type}
      end
    end

    test "speaks the BigQuery-flavored API vocabulary" do
      assert Schema.api_type(:int64) == {:ok, "INT64"}
      assert Schema.api_type(:bool) == {:ok, "BOOL"}
      assert Schema.api_type({:numeric, 38, 2}) == {:ok, "NUMERIC(38,2)"}
      assert Schema.type_from_api("string") == {:ok, :string}
      assert Schema.type_from_api("numeric(18, 4)") == {:ok, {:numeric, 18, 4}}
    end

    test "rejects API names it does not carry" do
      assert Schema.type_from_api("GEOGRAPHY") == {:error, {:unsupported_type, "GEOGRAPHY"}}

      assert Schema.type_from_api("NUMERIC(99,0)") ==
               {:error, {:unsupported_type, {:numeric, 99, 0}}}

      assert Schema.type_from_api(42) == {:error, {:unsupported_type, 42}}
    end

    test "reports types it does not carry" do
      assert Schema.duckdb_type(:blob) == {:error, {:unsupported_type, :blob}}
      assert Schema.explorer_dtype(:blob) == {:error, {:unsupported_type, :blob}}
      assert Schema.logical_from_explorer({:s, 32}) == {:error, {:unsupported_type, {:s, 32}}}

      assert Schema.logical_from_duckdb("STRUCT(a INT)") ==
               {:error, {:unsupported_type, "STRUCT(a INT)"}}
    end
  end

  describe "value_from_json/2" do
    test "coerces what the insert path accepts" do
      assert Schema.value_from_json(:int64, 42) == {:ok, 42}
      assert Schema.value_from_json(:int64, "9007199254740993") == {:ok, 9_007_199_254_740_993}
      assert Schema.value_from_json(:float64, 1.5) == {:ok, 1.5}
      assert Schema.value_from_json(:float64, 2) == {:ok, 2.0}
      assert Schema.value_from_json(:float64, "1.25") == {:ok, 1.25}
      assert Schema.value_from_json(:string, "hi") == {:ok, "hi"}
      assert Schema.value_from_json(:bool, true) == {:ok, true}
      assert Schema.value_from_json(:date, "2026-08-01") == {:ok, ~D[2026-08-01]}

      assert Schema.value_from_json(:timestamp, "2026-08-01T10:00:00") ==
               {:ok, ~N[2026-08-01 10:00:00]}

      assert Schema.value_from_json({:numeric, 38, 2}, "12.50") == {:ok, Decimal.new("12.50")}
      assert Schema.value_from_json({:numeric, 38, 2}, 12) == {:ok, Decimal.new(12)}
      assert Schema.value_from_json({:numeric, 38, 2}, 12.5) == {:ok, Decimal.from_float(12.5)}
    end

    test "a map keeps strings and writes any other value as its JSON text" do
      map = {:map, :string, :string}

      assert Schema.value_from_json(map, %{"host" => "h1", "pod" => nil}) ==
               {:ok, %{"host" => "h1", "pod" => nil}}

      assert Schema.value_from_json(map, %{
               "n" => 1,
               "ratio" => 1.5,
               "ok" => true,
               "tags" => ["a", "b"],
               "nested" => %{"k" => "v"}
             }) ==
               {:ok,
                %{
                  "n" => "1",
                  "ratio" => "1.5",
                  "ok" => "true",
                  "tags" => ~s(["a","b"]),
                  "nested" => ~s({"k":"v"})
                }}

      assert Schema.value_from_json(map, %{}) == {:ok, %{}}
    end

    test "a variant takes any JSON value as it is" do
      for value <- [
            %{"host" => "h1", "n" => 1, "tags" => ["a", %{"k" => nil}]},
            [1, "a"],
            "s",
            1,
            1.5,
            true
          ] do
        assert Schema.value_from_json(:variant, value) == {:ok, value}
      end

      assert Schema.value_from_json(:variant, Decimal.new(1)) ==
               {:error, {:invalid_value, :variant, Decimal.new(1)}}
    end

    test "a map rejects anything that is not an object, the way read_json does" do
      map = {:map, :string, :string}
      entries = [%{"key" => "host", "value" => "h1"}]

      assert Schema.value_from_json(map, "host=h1") == {:error, {:invalid_value, map, "host=h1"}}
      assert Schema.value_from_json(map, 1) == {:error, {:invalid_value, map, 1}}
      assert Schema.value_from_json(map, []) == {:error, {:invalid_value, map, []}}
      assert Schema.value_from_json(map, entries) == {:error, {:invalid_value, map, entries}}
    end

    test "converts an offset timestamp to naive UTC" do
      assert Schema.value_from_json(:timestamp, "2026-08-01T10:00:00+02:00") ==
               {:ok, ~N[2026-08-01 08:00:00]}

      assert Schema.value_from_json(:timestamp, "2026-08-01T10:00:00Z") ==
               {:ok, ~N[2026-08-01 10:00:00]}
    end

    test "nil passes through for every type" do
      for type <- @logical_types do
        assert Schema.value_from_json(type, nil) == {:ok, nil}
      end
    end

    test "rejects what it cannot coerce" do
      assert Schema.value_from_json(:int64, 1.5) == {:error, {:invalid_value, :int64, 1.5}}

      assert Schema.value_from_json(:int64, "12abc") ==
               {:error, {:invalid_value, :int64, "12abc"}}

      assert Schema.value_from_json(:int64, true) == {:error, {:invalid_value, :int64, true}}
      assert Schema.value_from_json(:string, 1) == {:error, {:invalid_value, :string, 1}}

      assert Schema.value_from_json(:string, <<0xFF, 0xFE>>) ==
               {:error, {:invalid_value, :string, <<0xFF, 0xFE>>}}

      assert Schema.value_from_json(:bool, "true") == {:error, {:invalid_value, :bool, "true"}}

      assert Schema.value_from_json(:date, "01/02/2026") ==
               {:error, {:invalid_value, :date, "01/02/2026"}}

      assert Schema.value_from_json(:timestamp, "yesterday") ==
               {:error, {:invalid_value, :timestamp, "yesterday"}}

      assert Schema.value_from_json({:numeric, 38, 2}, "12.5x") ==
               {:error, {:invalid_value, {:numeric, 38, 2}, "12.5x"}}
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

  describe "what Explorer can write" do
    test "a schema with a map or variant is not Explorer-writable, and names the field" do
      plain = Schema.new!([{"id", :int64}, {"name", :string}])

      mapped =
        Schema.new!([{"id", :int64}, {"attrs", {:map, :string, :string}}, {"doc", :variant}])

      assert Schema.explorer_unwritable(plain) == :none
      assert {:ok, %Field{name: "attrs"}} = Schema.explorer_unwritable(mapped)
      assert Schema.readable_explorer_dtypes(mapped) == [{"id", {:s, 64}}]
    end

    test "a map or variant cannot cluster; everything else can" do
      refute Schema.clustering_type?({:map, :string, :string})
      refute Schema.clustering_type?(:variant)

      for type <- @explorer_types, do: assert(Schema.clustering_type?(type))
    end
  end
end
