defmodule Smolquery.CatalogTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.Schema
  alias Smolquery.Segments.Segment
  alias Smolquery.Test.StubCatalog

  setup do
    %{catalog: StubCatalog.new(self())}
  end

  describe "dispatch" do
    test "create_dataset/2 reaches the implementation", %{catalog: catalog} do
      assert Catalog.create_dataset(catalog, "analytics") == :ok
      assert_received {:called, :create_dataset, ["analytics"]}
    end

    test "list_datasets/1 reaches the implementation", %{catalog: catalog} do
      assert Catalog.list_datasets(catalog) == {:ok, ["analytics"]}
      assert_received {:called, :list_datasets, []}
    end

    test "create_table/3 reaches the implementation", %{catalog: catalog} do
      schema = Schema.new!([{"id", :int64}])

      assert Catalog.create_table(catalog, {"ds", "t"}, schema) == :ok
      assert_received {:called, :create_table, [{"ds", "t"}, ^schema]}
    end

    test "list_tables/2 reaches the implementation", %{catalog: catalog} do
      assert Catalog.list_tables(catalog, "ds") == {:ok, ["events"]}
      assert_received {:called, :list_tables, ["ds"]}
    end

    test "table_schema/2 reaches the implementation", %{catalog: catalog} do
      assert Catalog.table_schema(catalog, {"ds", "t"}) == {:ok, StubCatalog.schema()}
      assert_received {:called, :table_schema, [{"ds", "t"}]}
    end

    test "register_segments/3 reaches the implementation", %{catalog: catalog} do
      segment = %Segment{
        id: "id",
        key: "p.parquet",
        path: "/p.parquet",
        row_count: 1,
        byte_size: 1
      }

      assert Catalog.register_segments(catalog, {"ds", "t"}, [segment]) ==
               {:ok, StubCatalog.snapshot()}

      assert_received {:called, :register_segments, [{"ds", "t"}, [^segment]]}
    end

    test "segments/3 defaults to the current snapshot", %{catalog: catalog} do
      assert Catalog.segments(catalog, {"ds", "t"}) == {:ok, ["/stub.parquet"]}
      assert_received {:called, :segments, [{"ds", "t"}, :current]}

      assert Catalog.segments(catalog, {"ds", "t"}, 7) == {:ok, ["/stub.parquet"]}
      assert_received {:called, :segments, [{"ds", "t"}, 7]}
    end

    test "drop_segments/3 reaches the implementation", %{catalog: catalog} do
      assert Catalog.drop_segments(catalog, {"ds", "t"}, ["/p.parquet"]) ==
               {:ok, StubCatalog.snapshot()}

      assert_received {:called, :drop_segments, [{"ds", "t"}, ["/p.parquet"]]}
    end

    test "current_snapshot/1 reaches the implementation", %{catalog: catalog} do
      assert Catalog.current_snapshot(catalog) == {:ok, StubCatalog.snapshot()}
      assert_received {:called, :current_snapshot, []}
    end

    test "put_clustering/3 and clustering/2 reach the implementation", %{catalog: catalog} do
      assert Catalog.put_clustering(catalog, {"ds", "t"}, ["id"]) == :ok
      assert_received {:called, :put_clustering, [{"ds", "t"}, ["id"]]}

      assert Catalog.clustering(catalog, {"ds", "t"}) == {:ok, []}
      assert_received {:called, :clustering, [{"ds", "t"}]}
    end
  end
end
