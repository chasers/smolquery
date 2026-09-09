defmodule Smolquery.QueryService.AlterColumnIntegrationTest do
  @moduledoc """
  A column added to a live table, read back across both tiers as deployed
  (`Smolquery.Test.FullNode`, PL-61 layer 1).

  Two batches seal before the column exists, so the sealed tier holds files
  that never carried it; a third lands after and stays hot. One query then
  reads the union: DuckLake fills the sealed rows with `NULL`, the planner's
  view projects the catalog's columns over `union_by_name` for the hot one,
  and the row written with the column carries its value. That is the whole
  claim behind "adding a column rewrites nothing".

  The seal valve is two files, as in the variant-column test, so no seal is
  committing while the read runs.
  """
  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema, do: Schema.new!([{"id", :int64, nullable: false}])

  setup context do
    node =
      FullNode.start(context,
        schema: schema(),
        seal_max_files: 2,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000
      )

    %{node: node}
  end

  defp rows(node, sql) do
    {:ok, job, %Explorer.DataFrame{} = frame} = QueryService.Client.query(node.query, sql)

    Frame.to_rows(frame, json_columns: job.json_columns)
  end

  test "rows sealed before the column read NULL; a row written after carries it", %{node: node} do
    {:ok, _one} =
      Client.write_batch(node.buffer, @table, %{schema: schema(), rows: [%{"id" => 1}]})

    {:ok, _two} =
      Client.write_batch(node.buffer, @table, %{schema: schema(), rows: [%{"id" => 2}]})

    assert Eventually.until(fn -> FullNode.sealed_count(node) >= 1 end, 200, 25)

    assert Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("label", :string)}) ==
             :ok

    {:ok, widened} = Catalog.table_schema(node.catalog, @table)
    assert Schema.names(widened) == ["id", "label"]

    {:ok, _three} =
      Client.write_batch(node.buffer, @table, %{
        schema: widened,
        rows: [%{"id" => 3, "label" => "three"}]
      })

    assert rows(node, "SELECT id, label FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "label" => nil},
             %{"id" => 2, "label" => nil},
             %{"id" => 3, "label" => "three"}
           ]
  end

  test "a dropped column disappears from every tier the view serves", %{node: node} do
    :ok = Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("label", :string)})
    {:ok, widened} = Catalog.table_schema(node.catalog, @table)

    {:ok, _one} =
      Client.write_batch(node.buffer, @table, %{
        schema: widened,
        rows: [%{"id" => 1, "label" => "one"}]
      })

    assert rows(node, "SELECT * FROM analytics.events") == [%{"id" => 1, "label" => "one"}]

    :ok = Catalog.alter_table(node.catalog, @table, {:drop_column, "label"})

    assert rows(node, "SELECT * FROM analytics.events") == [%{"id" => 1}]
  end
end
