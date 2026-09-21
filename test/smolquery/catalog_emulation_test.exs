defmodule Smolquery.CatalogEmulationTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.CatalogEmulation
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Schema
  alias Smolquery.Test.ExitingCatalog
  alias Smolquery.Test.MapCatalog

  setup_all do
    engine = :"catalog_emulation_#{:erlang.unique_integer([:positive])}"
    start_supervised!({Engine, name: engine})

    %{engine: engine}
  end

  describe "serialize/2" do
    test "answers the statement's AST and its canonical text", %{engine: engine} do
      assert {:ok, %{"error" => false} = ast, canonical} =
               CatalogEmulation.serialize(engine, "select  a from s.t where b = 'it''s'")

      assert canonical =~ "SELECT a FROM s.t"
      assert [%{"schema_name" => "s", "table_name" => "t"}] = CatalogEmulation.base_tables(ast)
    end

    test "a statement the parser refuses is an invalid query", %{engine: engine} do
      assert {:error, {:invalid_query, message}} = CatalogEmulation.serialize(engine, "SELEC 1")
      assert message =~ "syntax error"
    end
  end

  describe "base_tables/1" do
    test "finds a table in a subquery, a join and a table expression", %{engine: engine} do
      sql =
        "WITH c AS (SELECT * FROM a.one) SELECT * FROM c JOIN b.two ON true WHERE x IN (SELECT y FROM d.three)"

      {:ok, ast, _canonical} = CatalogEmulation.serialize(engine, sql)

      names = ast |> CatalogEmulation.base_tables() |> Enum.map(& &1["table_name"]) |> Enum.sort()

      assert names == ["c", "one", "three", "two"]
    end

    test "a statement that reads no table has none", %{engine: engine} do
      {:ok, ast, _canonical} = CatalogEmulation.serialize(engine, "SELECT 1")

      assert CatalogEmulation.base_tables(ast) == []
    end
  end

  describe "listed_tables/1" do
    test "answers every table with its schema" do
      catalog = MapCatalog.new()
      :ok = Catalog.create_dataset(catalog, "analytics")
      schema = Schema.new!([{"id", :int64}])
      :ok = Catalog.create_table(catalog, {"analytics", "events"}, schema)

      assert {:ok, [{"analytics", "events", %Schema{fields: [%{name: "id"}]}}]} =
               CatalogEmulation.listed_tables(catalog)
    end
  end

  describe "listing/1" do
    test "counts the tables whose schema could not be read, and fails when the catalog could not be asked" do
      catalog = MapCatalog.new()
      :ok = Catalog.create_dataset(catalog, "analytics")
      :ok = Catalog.create_table(catalog, {"analytics", "events"}, Schema.new!([{"id", :int64}]))
      :ok = Catalog.create_table(catalog, {"analytics", "clicks"}, Schema.new!([{"id", :int64}]))

      clicks = fn [table] -> table == {"analytics", "clicks"} end

      assert {:ok, [{"analytics", "events", _schema}], 1} =
               CatalogEmulation.listing(
                 ExitingCatalog.new(catalog, [table_schema: clicks], :conflict)
               )

      assert {:ok, [_events, _clicks], 0} = CatalogEmulation.listing(catalog)

      assert {:error, %CallExited{}} =
               CatalogEmulation.listing(ExitingCatalog.new(catalog, table_schema: clicks))
    end
  end
end
