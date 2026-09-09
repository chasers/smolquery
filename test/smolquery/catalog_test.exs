defmodule Smolquery.CatalogTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Segment
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.StubCatalog

  setup do
    %{catalog: StubCatalog.new(self())}
  end

  describe "alter_table/3: the checks every implementation shares" do
    test "refuses a partition ref before touching the catalog", %{catalog: catalog} do
      assert Catalog.alter_table(catalog, {"ds", "t__p1"}, {:drop_column, "id"}) ==
               {:error, {:partition_ref, {"ds", "t__p1"}}}

      refute_received {:called, :table_schema, _args}
    end

    test "refuses to add a column that is not nullable", %{catalog: catalog} do
      field = Field.new!("label", :string, nullable: false)

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:add_column, field}) ==
               {:error, {:column_must_be_nullable, "label"}}
    end

    test "refuses to add a name the table already has", %{catalog: catalog} do
      assert Catalog.alter_table(catalog, {"ds", "t"}, {:add_column, Field.new!("id", :string)}) ==
               {:error, {:duplicate_columns, ["id"]}}
    end

    test "refuses to drop an unknown column, or the last one", %{catalog: catalog} do
      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "missing"}) ==
               {:error, {:unknown_column, "missing"}}

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "id"}) ==
               {:error, :last_column}
    end

    test "an implementation without the callback answers unsupported", %{catalog: catalog} do
      field = Field.new!("label", :string)

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:add_column, field}) ==
               {:error, :alter_table_unsupported}

      assert_received {:called, :table_schema, [{"ds", "t"}]}
      assert_received {:called, :retention, [{"ds", "t"}]}
    end
  end

  describe "alter_table/3 against an implementation" do
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

    test "adds last and drops by name, and the schema reads back changed", %{catalog: catalog} do
      assert Catalog.alter_table(
               catalog,
               {"ds", "t"},
               {:add_column, Field.new!("label", :string)}
             ) ==
               :ok

      assert {:ok, schema} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Schema.names(schema) == ["id", "ts", "label"]

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"}) == :ok

      assert {:ok, schema} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Schema.names(schema) == ["id", "label"]
    end

    test "refuses to drop the retention column until the policy is cleared", %{catalog: catalog} do
      :ok = Catalog.put_retention(catalog, {"ds", "t"}, %{column: "ts", ttl_ms: 1_000})

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"}) ==
               {:error, {:retention_column, "ts"}}

      :ok = Catalog.put_retention(catalog, {"ds", "t"}, nil)

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"}) == :ok
    end

    test "refuses to drop a clustering column until the key is cleared", %{catalog: catalog} do
      :ok = Catalog.put_clustering(catalog, {"ds", "t"}, ["ts"])

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"}) ==
               {:error, {:clustering_column, "ts"}}

      :ok = Catalog.put_clustering(catalog, {"ds", "t"}, [])

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"}) == :ok
    end

    test "a dropped name comes back as a new column with a new id (PL-62)", %{catalog: catalog} do
      :ok = Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"})

      assert Catalog.alter_table(catalog, {"ds", "t"}, {:add_column, Field.new!("ts", :int64)}) ==
               :ok

      {:ok, schema} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Enum.map(schema.fields, &{&1.name, &1.id}) == [{"id", 1}, {"ts", 3}]
    end

    test "a change that lands emits the schema_change event, a refusal does not (PL-61 L3)",
         %{catalog: catalog} do
      handler = "catalog-schema-change-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler,
          [:smolquery, :catalog, :schema_change],
          fn _event, measurements, meta, nil ->
            send(parent, {:schema_change, measurements, meta})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      probe = {"ds", "schema_change_probe"}
      :ok = Catalog.create_table(catalog, probe, Schema.new!([{"id", :int64}]))
      :ok = Catalog.alter_table(catalog, probe, {:add_column, Field.new!("label", :string)})

      assert_receive {:schema_change, %{count: 1},
                      %{table_ref: ^probe, change: :add_column, column: "label", result: :ok}}

      :ok = Catalog.alter_table(catalog, probe, {:drop_column, "label"})
      assert_receive {:schema_change, _measurements, %{table_ref: ^probe, change: :drop_column}}

      {:error, {:unknown_column, "label"}} =
        Catalog.alter_table(catalog, probe, {:drop_column, "label"})

      refute_receive {:schema_change, _measurements, %{table_ref: ^probe}}, 50
    end

    test "an unknown table is the catalog's error", %{catalog: catalog} do
      assert Catalog.alter_table(catalog, {"ds", "nope"}, {:drop_column, "id"}) ==
               {:error, {:unknown_table, {"ds", "nope"}}}
    end

    test "columns get ids at creation, and an added column a fresh one (PL-62)",
         %{catalog: catalog} do
      {:ok, created} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Enum.map(created.fields, &{&1.name, &1.id}) == [{"id", 1}, {"ts", 2}]

      :ok = Catalog.alter_table(catalog, {"ds", "t"}, {:drop_column, "ts"})
      :ok = Catalog.alter_table(catalog, {"ds", "t"}, {:add_column, Field.new!("label", :string)})

      {:ok, widened} = Catalog.table_schema(catalog, {"ds", "t"})
      assert Enum.map(widened.fields, &{&1.name, &1.id}) == [{"id", 1}, {"label", 3}]
    end
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
