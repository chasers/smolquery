defmodule SmolqueryPg.PgCatalog.RewriteTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.PgCatalog.Rewrite

  test "a name literal cast to regclass becomes the catalog lookup it means" do
    assert Rewrite.pre("SELECT 1 WHERE d.classoid = 'pg_class'::regclass", %{}) ==
             "SELECT 1 WHERE d.classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_class')"

    assert Rewrite.pre("SELECT 'pg_catalog.pg_class'::regclass AS r", %{}) ==
             "SELECT (SELECT oid FROM pg_class WHERE relname = 'pg_class') AS r"

    assert Rewrite.pre("SELECT 'int4'::REGTYPE", %{}) ==
             "SELECT (SELECT oid FROM pg_type WHERE typname = 'int4')"
  end

  test "a non-literal regclass operand still casts to BIGINT" do
    assert Rewrite.pre("SELECT c.oid::regclass FROM pg_class c", %{}) ==
             "SELECT c.oid::BIGINT FROM pg_class c"
  end

  test "a string not followed by a reg cast is left alone" do
    assert Rewrite.pre("SELECT 'pg_class'::text", %{}) == "SELECT 'pg_class'::text"
  end

  test "current_setting inlines the session's value, or the server's, as a literal (T-412)" do
    assert Rewrite.pre("SELECT current_setting('TimeZone')", %{"TimeZone" => "UTC"}) ==
             "SELECT 'UTC'"

    assert Rewrite.pre("SELECT current_setting('max_index_keys')::int", %{}) ==
             "SELECT '32'::int"

    assert Rewrite.pre("SELECT current_setting('no_such_setting', true) AS v", %{}) ==
             "SELECT '' AS v"
  end
end
