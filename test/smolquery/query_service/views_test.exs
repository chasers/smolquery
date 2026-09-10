defmodule Smolquery.QueryService.ViewsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Views
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  describe "sources_select/2 (PL-62)" do
    test "projects each field-id group by id and reads it lazily; unidentified sources read by name, unioned" do
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
                 ~s|FROM read_parquet(['http://a/1.parquet'])| <>
                 " UNION ALL BY NAME " <>
                 ~s|SELECT CAST("id" AS BIGINT) AS "id", CAST("ts_int" AS VARCHAR) AS "ts_int" | <>
                 ~s|FROM read_parquet(['http://a/2.parquet'])|
    end

    test "a sealed file without ids is projected as of its registration snapshot; later columns read NULL" do
      schema =
        Schema.new!([
          Field.new!("id", :int64, id: 1, since: 2),
          Field.new!("label", :string, id: 7, since: 9)
        ])

      sources = [
        %{"url" => "/sealed/old.parquet", "snapshot" => 5, "columns" => ["id", "label"]},
        %{"url" => "/sealed/new.parquet", "field_ids" => %{"id" => 1, "label" => 7}}
      ]

      assert Views.sources_select(schema, sources) ==
               ~s|SELECT CAST("id" AS BIGINT) AS "id", CAST(NULL AS VARCHAR) AS "label" | <>
                 ~s|FROM read_parquet(['/sealed/old.parquet'], union_by_name := true)| <>
                 " UNION ALL BY NAME " <>
                 ~s|SELECT CAST("id" AS BIGINT) AS "id", CAST("label" AS VARCHAR) AS "label" | <>
                 ~s|FROM read_parquet(['/sealed/new.parquet'])|
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
                 ~s|FROM read_parquet(['http://a/1.parquet', 'http://a/2.parquet'])|
    end
  end

  test "projects the catalog's columns, in order, quoted" do
    schema = Schema.new!([{"id", :int64}, {"attrs", {:map, :string, :string}}])

    assert Views.table_view({"analytics", "events"}, schema, "SELECT 1") == [
             ~s|CREATE SCHEMA IF NOT EXISTS "analytics"|,
             ~s|CREATE OR REPLACE VIEW "analytics"."events" AS SELECT "id", "attrs" FROM (SELECT 1)|
           ]
  end

  test "a materialized column reads as coalesce(stored, expression) only where a source may lack it (PL-61)" do
    schema =
      Schema.new!([
        Field.new!("id", :int64, id: 1, since: 2),
        Field.new!("ts_int", :int64, id: 2, since: 2),
        Field.new!("ts", :timestamp,
          id: 3,
          since: 9,
          materialized: %Smolquery.Schema.Materialized{
            expression: "epoch_ms(ts_int)",
            canonical: "epoch_ms(ts_int)",
            sources: [2]
          }
        )
      ])

    carrying = %{"url" => "a", "field_ids" => %{"id" => 1, "ts_int" => 2, "ts" => 3}}
    lacking = %{"url" => "b", "field_ids" => %{"id" => 1, "ts_int" => 2}}
    sealed_before = %{"url" => "c", "snapshot" => 5}
    sealed_after = %{"url" => "d", "snapshot" => 9}
    raced = %{"url" => "f", "snapshot" => 9, "column_ids" => [1, 2]}
    recorded = %{"url" => "g", "snapshot" => 3, "column_ids" => [1, 2, 3]}
    legacy = %{"url" => "e"}

    assert Views.recomputed(schema, [carrying, sealed_after, recorded]) == []
    assert Views.recomputed(schema, [carrying, lacking]) == ["ts"]
    assert Views.recomputed(schema, [sealed_before]) == ["ts"]
    assert Views.recomputed(schema, [raced]) == ["ts"]
    assert Views.recomputed(schema, [legacy]) == ["ts"]

    assert [_schema, plain] = Views.table_view({"analytics", "events"}, schema, "SELECT 1")
    assert plain =~ ~s|SELECT "id", "ts_int", "ts" FROM|

    assert [_schema, computed] =
             Views.table_view({"analytics", "events"}, schema, "SELECT 1", ["ts"])

    assert computed =~
             ~s|SELECT "id", "ts_int", coalesce("ts", TRY(CAST((epoch_ms(ts_int)) AS TIMESTAMP))) AS "ts" FROM|
  end

  test "casts a variant column from its stored JSON to the VARIANT a query sees" do
    schema = Schema.new!([{"id", :int64}, {"doc", :variant}])

    assert [_schema, view] = Views.table_view({"analytics", "events"}, schema, "SELECT 1")

    assert view ==
             ~s|CREATE OR REPLACE VIEW "analytics"."events" AS SELECT "id", "doc"::VARIANT AS "doc" FROM (SELECT 1)|
  end
end
