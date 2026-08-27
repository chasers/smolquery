defmodule Smolquery.QueryService.ViewsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Views
  alias Smolquery.Schema

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
