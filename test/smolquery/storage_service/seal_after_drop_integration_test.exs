defmodule Smolquery.StorageService.SealAfterDropIntegrationTest do
  @moduledoc """
  A column dropped while micro-segments that carry it are still unsealed
  (T-430, as deployed on `Smolquery.Test.FullNode`).

  Every one of those micro-segments still holds the dropped column, and the
  claim that seals them is frozen, so a merge that refused an undeclared
  column would fail every retry and strand the table's tail in the hot tier
  for good. The merge projects the column away instead; this is the test
  that the seal completes, and that the sealed segment carries the catalog's
  columns and nothing else.

  The seal valve is age: the batch is written, the column dropped well inside
  `seal_max_age_ms`, and the maintenance tick then seals it.
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

  setup context do
    node =
      FullNode.start(context,
        schema: Schema.new!([{"id", :int64, nullable: false}]),
        seal_max_files: 1_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 500,
        maintenance_interval_ms: 50
      )

    %{node: node}
  end

  defp rows(node, sql) do
    {:ok, job, %Explorer.DataFrame{} = frame} = QueryService.Client.query(node.query, sql)

    Frame.to_rows(frame, json_columns: job.json_columns)
  end

  test "an unsealed micro-segment carrying a dropped column still seals, without it",
       %{node: node} do
    :ok = Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("label", :string)})
    {:ok, widened} = Catalog.table_schema(node.catalog, @table)

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, %{
        schema: widened,
        rows: [%{"id" => 1, "label" => "gone"}]
      })

    :ok = Catalog.alter_table(node.catalog, @table, {:drop_column, "label"})

    assert Eventually.until(fn -> FullNode.sealed_count(node) == 1 end, 400, 25)
    assert rows(node, "SELECT * FROM analytics.events") == [%{"id" => 1}]
  end
end
