defmodule Smolquery.QueryService.ViewsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Views
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  describe "sources_select/2 (PL-62)" do
    test "projects each field-id group by id, reads unidentified sources by name, and unions by name" do
      schema =
        Schema.new!([Field.new!("id", :int64, id: 1), Field.new!("ts_int", :string, id: 3)])

      sources = [
        %{"url" => "http://a/2.parquet", "field_ids" => %{"id" => 1, "ts_int" => 3}},
        %{"url" => "http://a/1.parquet", "field_ids" => %{"id" => 1, "ts_int" => 2}},
        %{"url" => "/sealed/legacy.parquet"}
      ]

      assert Views.sources_select(schema, sources) ==
               ~s|SELECT * FROM read_parquet(['/sealed/legacy.parquet'], union_by_name := true)| <>
                 " UNION ALL BY NAME " <>
                 ~s|SELECT CAST("id" AS BIGINT) AS "id", CAST(NULL AS VARCHAR) AS "ts_int" | <>
                 ~s|FROM read_parquet(['http://a/1.parquet'], union_by_name := true)| <>
                 " UNION ALL BY NAME " <>
                 ~s|SELECT CAST("id" AS BIGINT) AS "id", CAST("ts_int" AS VARCHAR) AS "ts_int" | <>
                 ~s|FROM read_parquet(['http://a/2.parquet'], union_by_name := true)|
    end

    test "files that agree share one scan" do
      schema = Schema.new!([Field.new!("id", :int64, id: 1)])
      ids = %{"id" => 1}

      sources = [
        %{"url" => "http://a/1.parquet", "field_ids" => ids},
        %{"url" => "http://a/2.parquet", "field_ids" => ids}
      ]

      assert Views.sources_select(schema, sources) ==
               ~s|SELECT CAST("id" AS BIGINT) AS "id" | <>
                 ~s|FROM read_parquet(['http://a/1.parquet', 'http://a/2.parquet'], union_by_name := true)|
    end
  end

  test "projects the catalog's columns, in order, quoted" do
    schema = Schema.new!([{"id", :int64}, {"attrs", {:map, :string, :string}}])

    assert Views.table_view({"analytics", "events"}, schema, "SELECT 1") == [
             ~s|CREATE SCHEMA IF NOT EXISTS "analytics"|,
             ~s|CREATE OR REPLACE VIEW "analytics"."events" AS SELECT "id", "attrs" FROM (SELECT 1)|
           ]
  end

  test "casts a variant column from its stored JSON to the VARIANT a query sees" do
    schema = Schema.new!([{"id", :int64}, {"doc", :variant}])

    assert [_schema, view] = Views.table_view({"analytics", "events"}, schema, "SELECT 1")

    assert view ==
             ~s|CREATE OR REPLACE VIEW "analytics"."events" AS SELECT "id", "doc"::VARIANT AS "doc" FROM (SELECT 1)|
  end
end
