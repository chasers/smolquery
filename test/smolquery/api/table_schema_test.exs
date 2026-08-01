defmodule Smolquery.Api.TableSchemaTest do
  use ExUnit.Case, async: true

  alias Smolquery.Api.TableSchema
  alias Smolquery.Schema

  @schema Schema.new!([
            {"id", :int64, nullable: false},
            {"ts", :timestamp},
            {"amount", {:numeric, 38, 2}}
          ])

  @json [
    %{"name" => "id", "type" => "INT64", "nullable" => false},
    %{"name" => "ts", "type" => "TIMESTAMP", "nullable" => true},
    %{"name" => "amount", "type" => "NUMERIC(38,2)", "nullable" => true}
  ]

  describe "to_json/1" do
    test "serializes every field with its API type name" do
      assert TableSchema.to_json(@schema) == @json
    end
  end

  describe "from_json/1" do
    test "round-trips what to_json/1 wrote" do
      assert TableSchema.from_json(@json) == {:ok, @schema}
    end

    test "nullable defaults to true" do
      assert {:ok, schema} = TableSchema.from_json([%{"name" => "id", "type" => "INT64"}])
      assert schema == Schema.new!([{"id", :int64}])
    end

    test "rejects an unsupported type" do
      assert TableSchema.from_json([%{"name" => "g", "type" => "GEOGRAPHY"}]) ==
               {:error, {:unsupported_type, "GEOGRAPHY"}}
    end

    test "rejects a field without a name or type" do
      assert {:error, {:invalid_field, _field}} = TableSchema.from_json([%{"name" => "id"}])
      assert {:error, {:invalid_field, _field}} = TableSchema.from_json([%{"type" => "INT64"}])
    end

    test "rejects a non-boolean nullable" do
      assert {:error, {:invalid_field, %{"nullable" => "yes"}}} =
               TableSchema.from_json([%{"name" => "id", "type" => "INT64", "nullable" => "yes"}])
    end

    test "rejects a schema that is not a list" do
      assert TableSchema.from_json(%{"fields" => []}) ==
               {:error, {:invalid_schema, %{"fields" => []}}}
    end

    test "surfaces Schema.new/1 rejections" do
      assert TableSchema.from_json([]) == {:error, :empty_schema}

      assert {:error, {:duplicate_columns, ["id"]}} =
               TableSchema.from_json([
                 %{"name" => "id", "type" => "INT64"},
                 %{"name" => "id", "type" => "STRING"}
               ])
    end
  end
end
