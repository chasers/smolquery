defmodule Smolquery.DdlTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.Ddl
  alias Smolquery.Ddl.AlterTable
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Test.MapCatalog

  describe "parse/1: what is not DDL" do
    test "anything that does not begin with ALTER is the planner's" do
      assert Ddl.parse("SELECT 1") == :not_ddl
      assert Ddl.parse("  with x as (select 1) select * from x") == :not_ddl
      assert Ddl.parse("CREATE TABLE analytics.events (id BIGINT)") == :not_ddl
      assert Ddl.parse("-- ALTER TABLE in a comment\nSELECT 1") == :not_ddl
      assert Ddl.parse("alteration") == :not_ddl
    end
  end

  describe "parse/1: ADD COLUMN" do
    test "the plain form" do
      assert {:ok,
              %AlterTable{
                table: {"analytics", "events"},
                change: {:add_column, %Field{name: "country", type: :string, nullable: true}},
                materialized: nil,
                if_exists: false
              }} = Ddl.parse("ALTER TABLE analytics.events ADD COLUMN country STRING")
    end

    test "COLUMN is optional, keywords are case-insensitive, a trailing semicolon is fine" do
      assert {:ok, %AlterTable{change: {:add_column, %Field{name: "n", type: :int64}}}} =
               Ddl.parse("alter table analytics.events add n bigint;")
    end

    test "IF NOT EXISTS" do
      assert {:ok, %AlterTable{if_exists: true, change: {:add_column, %Field{name: "n"}}}} =
               Ddl.parse("ALTER TABLE analytics.events ADD COLUMN IF NOT EXISTS n INT64")
    end

    test "every type spelling lands on the same logical type" do
      for {spelling, type} <- [
            {"BIGINT", :int64},
            {"INT64", :int64},
            {"INTEGER", :int64},
            {"DOUBLE", :float64},
            {"FLOAT64", :float64},
            {"VARCHAR", :string},
            {"STRING", :string},
            {"TEXT", :string},
            {"BOOLEAN", :bool},
            {"BOOL", :bool},
            {"TIMESTAMP", :timestamp},
            {"DATE", :date},
            {"DECIMAL(38, 2)", {:numeric, 38, 2}},
            {"NUMERIC(10,3)", {:numeric, 10, 3}},
            {"MAP(STRING, STRING)", {:map, :string, :string}},
            {"MAP(VARCHAR, VARCHAR)", {:map, :string, :string}},
            {"VARIANT", :variant},
            {"JSON", :variant}
          ] do
        assert {:ok, %AlterTable{change: {:add_column, %Field{type: ^type}}}} =
                 Ddl.parse("ALTER TABLE ds.t ADD COLUMN c #{spelling}"),
               spelling
      end
    end

    test "an unquoted name keeps its case; a quoted one is taken verbatim" do
      assert {:ok, %AlterTable{change: {:add_column, %Field{name: "myColumn"}}}} =
               Ddl.parse("ALTER TABLE ds.t ADD COLUMN myColumn STRING")

      assert {:ok, %AlterTable{table: {"My_DS", "T"}, change: {:add_column, %Field{name: "Col"}}}} =
               Ddl.parse(~s|ALTER TABLE "My_DS"."T" ADD COLUMN "Col" STRING|)
    end

    test "MATERIALIZED keeps the expression as raw text" do
      assert {:ok, %AlterTable{materialized: "epoch_ms(ts_int) + INTERVAL '1 hour'"}} =
               Ddl.parse(
                 "ALTER TABLE ds.t ADD COLUMN ts TIMESTAMP MATERIALIZED epoch_ms(ts_int) + INTERVAL '1 hour';"
               )

      assert Ddl.parse("ALTER TABLE ds.t ADD COLUMN ts TIMESTAMP MATERIALIZED") ==
               {:error, {:invalid_ddl, "MATERIALIZED needs an expression"}}
    end

    test "NOT NULL, DEFAULT and other trailing clauses are refused with the clause named" do
      assert Ddl.parse("ALTER TABLE ds.t ADD COLUMN n BIGINT NOT NULL") ==
               {:error, {:unsupported_ddl, "NOT NULL"}}

      assert Ddl.parse("ALTER TABLE ds.t ADD COLUMN n BIGINT DEFAULT 0") ==
               {:error, {:unsupported_ddl, "DEFAULT"}}
    end

    test "an unknown type names the spelling it saw" do
      assert Ddl.parse("ALTER TABLE ds.t ADD COLUMN g GEOGRAPHY") ==
               {:error, {:unsupported_type, "GEOGRAPHY"}}
    end

    test "a bad identifier is the same refusal the API gives" do
      assert Ddl.parse(~s|ALTER TABLE ds.t ADD COLUMN "bad name" STRING|) ==
               {:error, {:invalid_identifier, "bad name"}}
    end
  end

  describe "parse/1: DROP COLUMN" do
    test "the plain form, with and without COLUMN, with IF EXISTS" do
      assert {:ok, %AlterTable{table: {"ds", "t"}, change: {:drop_column, "c"}, if_exists: false}} =
               Ddl.parse("ALTER TABLE ds.t DROP COLUMN c")

      assert {:ok, %AlterTable{change: {:drop_column, "c"}}} =
               Ddl.parse("ALTER TABLE ds.t DROP c")

      assert {:ok, %AlterTable{change: {:drop_column, "c"}, if_exists: true}} =
               Ddl.parse("ALTER TABLE ds.t DROP COLUMN IF EXISTS c")
    end

    test "anything after the column is refused" do
      assert Ddl.parse("ALTER TABLE ds.t DROP COLUMN c CASCADE") ==
               {:error, {:unsupported_ddl, "CASCADE"}}
    end
  end

  describe "parse/1: the table reference" do
    test "must be dataset.table" do
      assert Ddl.parse("ALTER TABLE events DROP COLUMN c") ==
               {:error, {:unqualified_table, "events"}}

      assert {:error, {:invalid_ddl, message}} =
               Ddl.parse("ALTER TABLE lake.analytics.events DROP COLUMN c")

      assert message =~ "one part too many"
    end
  end

  describe "parse/1: what is ALTER but not ours" do
    test "other ALTER forms and other actions" do
      assert Ddl.parse("ALTER VIEW v RENAME TO w") ==
               {:error, {:invalid_ddl, "only ALTER TABLE is supported"}}

      assert Ddl.parse("ALTER TABLE ds.t RENAME COLUMN a TO b") ==
               {:error, {:unsupported_ddl, "RENAME"}}

      assert Ddl.parse("ALTER TABLE ds.t") ==
               {:error, {:invalid_ddl, "expected ADD COLUMN or DROP COLUMN"}}
    end

    test "two statements are refused rather than half-run" do
      assert Ddl.parse("ALTER TABLE ds.t DROP COLUMN c; SELECT 1") ==
               {:error, :multiple_statements}
    end
  end

  describe "execute/2" do
    setup do
      catalog = MapCatalog.new()
      :ok = Catalog.create_dataset(catalog, "ds")

      :ok =
        Catalog.create_table(
          catalog,
          {"ds", "t"},
          Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
        )

      %{catalog: catalog}
    end

    defp run(catalog, sql) do
      {:ok, ddl} = Ddl.parse(sql)
      Ddl.execute(catalog, ddl)
    end

    test "adds and drops, reporting what it did", %{catalog: catalog} do
      assert run(catalog, "ALTER TABLE ds.t ADD COLUMN label STRING") ==
               {:ok,
                %{operation: :add_column, table: {"ds", "t"}, column: "label", performed: true}}

      assert {:ok, schema} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Schema.names(schema) == ["id", "ts", "label"]

      assert run(catalog, "ALTER TABLE ds.t DROP COLUMN ts") ==
               {:ok,
                %{operation: :drop_column, table: {"ds", "t"}, column: "ts", performed: true}}
    end

    test "IF NOT EXISTS and IF EXISTS turn the two absences into performed: false",
         %{catalog: catalog} do
      assert {:ok, %{performed: false}} =
               run(catalog, "ALTER TABLE ds.t ADD COLUMN IF NOT EXISTS ts STRING")

      assert {:ok, %{performed: false}} =
               run(catalog, "ALTER TABLE ds.t DROP COLUMN IF EXISTS nope")

      assert run(catalog, "ALTER TABLE ds.t ADD COLUMN ts STRING") ==
               {:error, {:duplicate_columns, ["ts"]}}

      assert run(catalog, "ALTER TABLE ds.t DROP COLUMN nope") ==
               {:error, {:unknown_column, "nope"}}
    end

    test "a dropped name is added again as a new column, guard or no guard (PL-62)",
         %{catalog: catalog} do
      {:ok, _dropped} = run(catalog, "ALTER TABLE ds.t DROP COLUMN ts")

      assert {:ok, %{operation: :add_column, column: "ts", performed: true}} =
               run(catalog, "ALTER TABLE ds.t ADD COLUMN IF NOT EXISTS ts STRING")

      assert {:ok, schema} = Catalog.table_schema(catalog, {"ds", "t"})
      assert {:ok, %Field{name: "ts", type: :string, id: 2}} = Schema.field(schema, "ts")
    end

    test "MATERIALIZED is parsed but not yet executable", %{catalog: catalog} do
      assert run(catalog, "ALTER TABLE ds.t ADD COLUMN x TIMESTAMP MATERIALIZED epoch_ms(id)") ==
               {:error, :materialized_unsupported}
    end
  end

  describe "error?/1" do
    test "knows every reason a DDL job can fail with, and nothing else" do
      assert Ddl.error?({:unknown_column, "ts"})
      assert Ddl.error?({:duplicate_columns, ["ts"]})
      assert Ddl.error?({:unsupported_ddl, "NOT NULL"})
      assert Ddl.error?(:last_column)
      assert Ddl.error?(:multiple_statements)
      assert Ddl.error?(:materialized_unsupported)
      refute Ddl.error?({:invalid_query, "syntax error"})
      refute Ddl.error?(:timeout)
    end
  end

  describe "message/1" do
    test "every recognised reason reads as a sentence, never as a term" do
      assert Ddl.message({:unsupported_ddl, "NOT NULL"}) ==
               "NOT NULL is not supported in ALTER TABLE"

      assert Ddl.message({:duplicate_columns, ["ts"]}) == "column ts already exists"
      assert Ddl.message({:unknown_table, {"ds", "t"}}) == "table ds.t does not exist"
      assert Ddl.message(:ddl_takes_no_params) == "ALTER TABLE takes no bind parameters"
      assert Ddl.message({:weird, 1}) == "{:weird, 1}"
    end
  end
end
